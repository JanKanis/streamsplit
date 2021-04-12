use std::convert::{TryFrom, TryInto};
use std::fmt::{Display, Formatter};
use std::fs::File;
use std::io::{self, Error, ErrorKind, Read, Write};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};

use hex;
use once_cell::sync::Lazy;
use structopt::StructOpt;
use std::time::{Instant, Duration};
use std::cmp::{min, max};
use std::mem::size_of;
use std::net::Shutdown;

// The format! macro doesn't understand regular constants :(
macro_rules! MERGE_SOCKET_NAME {
    () => {
        "streamsplit_merge_sock_{}.sock"
    };
}

#[derive(Debug, Default, StructOpt)]
#[structopt(name = "streamsplit", about = "Split a byte stream into multiple streams and merge them back")]
struct Opt {
    #[structopt(long, conflicts_with = "merge")]
    split: Option<u16>,
    #[structopt(skip)]
    splitcount: usize,

    #[structopt(long)]
    merge: bool,

    /// By default streamsplit will add a header to the resulting streams that will allow another
    /// instance of streamsplit to merge the streams back together. This flag disables the header,
    ///
    #[structopt(long)]
    no_header: bool,

    /// The block size in which to split the datastream. A linux pipe by default has a buffer of
    /// 16 KB, so a larger block size is probably not useful.
    #[structopt(short, long, default_value = "16384")]
    blocksize: u32,

    /// The shell used to interpret commands. Defaults to the user's default shell in $SHELL.
    #[structopt(long, env = "SHELL", default_value = "/bin/sh")]
    sh: String,

    /// Do not interpret the command as a shell command, but run it directly
    #[structopt(long, short, conflicts_with = "sh")]
    no_shell: bool,

    /// timeout when waiting for the master merge socket to appear
    #[structopt(short, long, default_value = "5000")]
    timeout: u32,

    /// The time to wait after closing output streams of the non-leading merge processes
    #[structopt(short, long, default_value = "1000")]
    pause_after_close: u32,

    /// Output file for the merged stream. If not used, the merged stream will be written to the
    /// leading process's stdout.
    #[structopt(short, long)]
    output: Option<String>,

    /// The directory to create the merging socket. Defaults to $XDG_RUNTIME_DIR or /tmp if that is not set.
    #[structopt(long, env = "XDG_RUNTIME_DIR", default_value = "/tmp")]
    socket_dir: String,

    //#[structopt(last)]
    #[structopt(name = "command", subcommand)]
    _command: Subcommand,
    #[structopt(skip)]
    command: Vec<String>,

    #[structopt(long, short)]
    verbose: bool,
}

#[derive(Debug, PartialEq, StructOpt)]
enum Subcommand {
    #[structopt(external_subcommand)]
    Command(Vec<String>),
}

impl Default for Subcommand {
    fn default() -> Self {
        Subcommand::Command(Vec::new())
    }
}

impl Opt {
    fn init(mut self) -> Self {
        match &mut self._command {
            Subcommand::Command(cmd) => {
                std::mem::swap(&mut self.command, cmd);
            }
        }

        match self.split {
            None => {}
            Some(num) => self.splitcount = num.usize(),
        }

        return self;
    }
}

#[derive(Debug)]
pub enum CatError {
    ReadError(Error),
    WriteError(Error),
    BytesLost(usize),
}

impl Display for CatError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl std::error::Error for CatError {}

#[derive(PartialEq)]
enum InputState {
    More,
    Done,
}

// Ostensibly a #[repr(C, packed)], but I'm sticking to safe reading/writing so the actual layout isn't relevant
#[derive(Copy, Clone, Debug)]
struct Header {
    /// "streamsplit;" in ascii as magic number
    magic: [u8; 12],
    /// The protocol/header version, currently 1
    version: u32,
    /// The block size at which the original stream is split up
    blocksize: u32,
    /// The number of streams the original stream was split into
    splits: u16,
    /// The (zero-based) index of this stream
    stream_id: u16,
    /// zeros
    _padding: [u8; 8],
    /// A random GUID to match different streams together
    streamsplit_id: [u8; 16],
}

struct WriteChainer<'a> {
    slice: &'a mut Vec<u8>,
}
impl WriteChainer<'_> {
    fn write(self, data: &[u8]) -> Self {
        let wrote = self.slice.write(&data).unwrap();
        if wrote != data.len() {
            panic!("Write to buffer failed, wrote {} out of {} bytes", wrote, data.len())
        }
        self
    }
}

/// An extension trait to ease conversion to usize. The conversion panics on failure, so we assume
/// that usize is at least as wide as u32.
trait USize {
    fn usize(self) -> usize;
}
impl USize for u16 {
    fn usize(self) -> usize {
        usize::try_from(self).unwrap()
    }
}
impl USize for u32 {
    fn usize(self) -> usize {
        usize::try_from(self).unwrap()
    }
}

impl Header {
    fn bytes(&self) -> Vec<u8> {
        let mut res = Vec::with_capacity(size_of::<Header>());
        WriteChainer { slice: &mut res }
            .write(&self.magic)
            .write(&self.version.to_be_bytes())
            .write(&self.blocksize.to_be_bytes())
            .write(&self.splits.to_be_bytes())
            .write(&self.stream_id.to_be_bytes())
            .write(&self._padding)
            .write(&self.streamsplit_id);

        res
    }

    fn read_from_stream(r: &mut dyn Read, errmsg: &str) -> (Header, [u8; size_of::<Header>()]) {
        let mut bytes = [0u8; size_of::<Header>()];
        r.read(&mut bytes).expect("Unable to read header from stream");
        let mut hdr = Header::default();

        hdr.magic = bytes[0..12].try_into().unwrap();
        hdr.version = u32::from_be_bytes(bytes[12..16].try_into().unwrap());
        hdr.blocksize = u32::from_be_bytes(bytes[16..20].try_into().unwrap());
        hdr.splits = u16::from_be_bytes(bytes[20..22].try_into().unwrap());
        hdr.stream_id = u16::from_be_bytes(bytes[22..24].try_into().unwrap());
        hdr._padding = bytes[24..32].try_into().unwrap();
        hdr.streamsplit_id = bytes[32..48].try_into().unwrap();

        if hdr.magic != *b"streamsplit;" {
            panic!("{}: this stream is not a valid streamsplit stream. Stream marker not found", errmsg);
        }
        if hdr._padding != [0u8; 8] {
            panic!("{}: stream header is invalid: invalid padding found", errmsg)
        }
        if hdr.version != 1 {
            panic!(
                "{}: The stream is for version {}, this program only supports version 1",
                errmsg, hdr.version
            )
        }
        if hdr.stream_id >= hdr.splits {
            panic!("{}: The stream has a stream_id >= the number of splits, which is invalid", errmsg)
        }

        (hdr, bytes)
    }
}

impl Default for Header {
    fn default() -> Self {
        Header {
            magic: *b"streamsplit;",
            version: 1,
            blocksize: 0,
            splits: 0,
            stream_id: 0,
            _padding: [0u8; 8],
            streamsplit_id: [0u8; 16],
        }
    }
}

static OPTS: Lazy<Opt> = Lazy::new(|| Opt::from_args().init());
//static OPTS: OnceCell<Opt> = OnceCell::new();

fn main() {
    if let Some(splits) = OPTS.split {
        if let Err(e) = split(splits, &OPTS.command) {
            std::process::exit(1);
        }
    }

    if OPTS.merge {
        merge(Box::new(io::stdin()));
    }
    return;

    match transfer_block(&mut io::stdin(), &mut io::stdout(), OPTS.blocksize.usize()) {
        Ok(_) => {}
        // Err(CatError::ReadError(e)) => {
        //     eprintln!("Error while reading: {}", e)
        // }
        // Err(CatError::WriteError(e)) => {
        //     eprintln!("Error while writing: {}", e)
        // }
        // Err(CatError::BytesLost(lost)) => {
        //     eprintln!("not all input was written, {} bytes lost", lost)
        // }
        Err(e) => {
            eprintln!("Error: {}", e);
        }
    }
}

fn merge(mut inp: Box<dyn Read>) {
    let (hdr, hdrbytes) = Header::read_from_stream(&mut inp, "Invalid input stream");

    if hdr.stream_id == 0 {
        merge_master(hdr, inp, Box::new(io::stdout()));
    } else {
        merge_slave(hdr, hdrbytes, inp);
    }
}

struct FileDeleter<'a> {
    path: &'a Path,
}
impl Drop for FileDeleter<'_> {
    fn drop(&mut self) {
        if let Err(e) = std::fs::remove_file(self.path) {
            eprintln!("Deleting listening socket at {} failed", self.path.to_string_lossy());
        };
    }
}

fn merge_master(hdr: Header, mut inp: Box<dyn Read>, mut outp: Box<dyn Write>) {
    // Set umask so only this user has access to the socket, just to be safe
    let old_umask = unsafe { libc::umask(0o177) };
    let socketpath = socket_path(&OPTS.socket_dir, &hdr.streamsplit_id);
    let listener = UnixListener::bind(socketpath.as_path()).unwrap_or_else(|e| {
        panic!("Unable to create socket at {}: {:?}", socketpath.as_path().to_string_lossy(), e)
    });
    unsafe { libc::umask(old_umask) };
    let deleter = FileDeleter { path: &socketpath };

    let mut stream_opts = Vec::with_capacity(hdr.splits.usize());
    for i in 0..stream_opts.capacity() {
        stream_opts.push(None);
    }
    stream_opts[0] = Some(inp);

    if OPTS.verbose {
        eprintln!("Merge socket created. 1 of {} merge streams connected", hdr.splits);
    }

    // This process also counts for 1
    let mut connected_slaves = 1;
    while connected_slaves < hdr.splits {
        match listener.accept() {
            Err(e) => {
                if OPTS.verbose {
                    eprintln!("Error listening on merge socket: {}", e)
                }
                continue;
            }
            Ok((mut stream, _addr)) => {
                let (streamhdr, _) = Header::read_from_stream(&mut stream, "Invalid merge stream");
                if streamhdr.streamsplit_id != hdr.streamsplit_id {
                    panic!("Invalid merge stream: stream set id does not match, a stream that is not part of this merge set appears to have connected");
                }
                if streamhdr.blocksize != hdr.blocksize {
                    panic!("Invalid merge stream: blocksize of merge stream does not match merge master's blocksize")
                }
                if streamhdr.splits != hdr.splits {
                    panic!("Invalid merge stream: number of splits does not match, a stream that is not part of this merge set appears to have connected");
                }
                if streamhdr.stream_id == 0 {
                    panic!("Invalid merge stream: connecting stream has stream id 0, which is invalid")
                }

                stream_opts[streamhdr.stream_id.usize()] = Some(Box::new(stream));
                connected_slaves += 1;
                if OPTS.verbose {
                    eprintln!("{} of {} merge streams connected", connected_slaves, hdr.splits);
                }
            }
        }
    }

    let mut streams: Vec<Box<dyn Read>> =
        stream_opts.into_iter().map(|s| s.unwrap()).collect();

    'outer: loop {
        for i in 0..streams.len() {
            let state = transfer_block(&mut *streams[i], &mut *outp, OPTS.blocksize.usize()).unwrap();
            if state == InputState::Done {
                break 'outer;
            }
        }
    }

    check_all_eof(streams.as_mut_slice());
}

fn merge_slave(hdr: Header, hdrbytes: [u8; size_of::<Header>()], mut inp: Box<dyn Read>) {
    if OPTS.verbose {
        eprintln!("slave merger #{} trying to connect...", hdr.stream_id);
    }

    let socketpath = socket_path(&OPTS.socket_dir, &hdr.streamsplit_id);

    let deadline = Instant::now() + Duration::from_millis(u64::from(OPTS.timeout));

    let mut interval = Duration::from_millis(1);

    // Connect to the merge master's socket, give the master some time to create it but don't wait forever
    let mut socket = loop {
        let conn = UnixStream::connect(&socketpath);
        match conn {
            Ok(socket) => { break socket }
            Err(e) => {
                if e.kind() != ErrorKind::NotFound {
                    panic!("Error connecting to merge master process: {:?}", e);
                } else if Instant::now() > deadline {
                    panic!("Timeout exceeded while trying to connect to merge master");
                } else {
                    std::thread::sleep(interval);
                    interval = min(interval * 2, Duration::from_millis(300)) ;
                    continue
                }
            }
        }
    };

    socket.write_all(&hdrbytes).expect("Error writing to merge socket");

    if OPTS.verbose {
        eprintln!("slave merger #{} connected", hdr.stream_id);
    }

    let mut buf = vec![0u8; 64*1024];
    loop {
        let count = inp.read(&mut buf).expect("Error reading from stdin");
        if count == 0 {
            continue  // EOF
        }
        socket.write_all(&buf[..count]).expect("Error writing to merge socket");
    }

    socket.shutdown(Shutdown::Both);
}

fn check_all_eof(readers: &mut [Box<dyn Read>]) {
    let mut buf = [0u8; 1];
    for mut r in readers.iter_mut() {
        let count = r.read(&mut buf).unwrap();
        if count != 0 {
            panic!(
                "Not all merge streams were finished at the same time. This could indicate data corruption"
            );
        }
    }
}

fn socket_path(socket_dir: &str, guid: &[u8; 16]) -> PathBuf {
    let mut sock = PathBuf::from(socket_dir);
    let filename = format!(MERGE_SOCKET_NAME!(), hex::encode(guid));
    sock.push(filename);
    sock
}

fn random() -> [u8; 16] {
    let mut r = File::open("/dev/urandom").expect("Failed to open /dev/urandom");
    let mut buf = [0u8; 16];
    r.read(&mut buf).expect("Error reading random number from /dev/urandom");
    buf
}

fn split(num: u16, command: &Vec<String>) -> Result<(), Error> {
    let mut children = Vec::<Child>::with_capacity(num.usize());

    let mut header = Header::default();
    header.blocksize = OPTS.blocksize;
    header.splits = num;
    header.streamsplit_id = random();
    if OPTS.verbose {
        eprintln!("Instance id: {}", hex::encode(header.streamsplit_id));
    }

    for i in 0..num {
        let child = Command::new(&command[0])
            .stdin(Stdio::piped())
            .env("STREAMSPLIT_N", (i + 1).to_string())
            .args(&command[1..])
            .spawn()?;

        header.stream_id = i;
        if !OPTS.no_header {
            child
                .stdin
                .as_ref()
                .unwrap()
                .write(header.bytes().as_slice())
                .expect("Error writing header to output");
        }
        children.push(child);
    }

    let mut current_child = 0u16;

    loop {
        let inputstate = transfer_block(
            &mut io::stdin(),
            &mut children[current_child.usize()].stdin.as_ref().unwrap(),
            OPTS.blocksize.usize(),
        )
        .unwrap();
        if inputstate == InputState::Done {
            break;
        }

        current_child = (current_child + 1) % num;
    }

    let mut failure = false;
    for (i, mut child) in children.iter_mut().enumerate() {
        let status = child.wait().unwrap_or_else(|e| {
            panic!("Error waiting on child {}: {:?}", i, e);
        });
        if !status.success() {
            eprintln!("Error: Child {} exited with status {}", i, status.code().unwrap());
            failure = true;
        }
    }
    if failure {
        return Err(Error::from(ErrorKind::Other));
    }

    children.clear();

    return Ok(());
}

fn transfer_block(inp: &mut dyn Read, out: &mut dyn Write, blocksize: usize) -> Result<InputState, Error> {
    let mut buffer = vec![0u8; blocksize];

    let mut readcount = 0usize;
    while readcount < blocksize {
        let read = inp.read(&mut buffer[readcount..])?;
        if read == 0 {
            return Ok(InputState::Done);
        }
        out.write_all(&buffer[readcount..readcount+read])?;
        readcount += read;
    }
    return Ok(InputState::More);
}
