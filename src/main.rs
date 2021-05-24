use std::cmp::min;
use std::convert::{TryFrom, TryInto};
use std::fs::File;
use std::io::{self, ErrorKind, Read, Write};
use std::mem::replace;
use std::mem::size_of;
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::thread::sleep;
use std::time::{Duration, Instant};

use hex;
use once_cell::sync::Lazy;
use structopt::StructOpt;

use streamsplit::syscall_wrappers::AsSSRawFd;
use streamsplit::syscall_wrappers::*;
use streamsplit::transfer::{InputState, Transfer};

// The format! macro doesn't understand regular constants :(
macro_rules! MERGE_SOCKET_NAME {
    () => {
        "streamsplit_merge_sock_{}.sock"
    };
}

macro_rules! debug {
    ($($arg:tt)*) => {
        if OPTS.verbose {
            // ensure output is written in a single write, to avoid mixing up writes from different processes
            eprint!("{}", format!("{}\n", format_args!($($arg)*)));
        }
    };
}

#[derive(Debug, Default, StructOpt)]
#[structopt(name = "streamsplit", about = "Split a byte stream into multiple streams and merge them back")]
struct Opt {
    #[structopt(long, conflicts_with = "merge")]
    split: Option<u16>,

    #[structopt(long)]
    merge: bool,

    /// By default streamsplit will add a header to the resulting streams that will allow another
    /// instance of streamsplit to merge the streams back together. This flag disables the header,
    ///
    #[structopt(long)]
    no_header: bool,

    /// The block size in which to split the datastream. A linux pipe by default has a buffer of
    /// 64 KB, and using that as block size appears to be optimal if the data goes through a pipe.
    #[structopt(short, long, default_value = "65536")]
    blocksize: u32,

    /// The shell used to interpret commands. Defaults to the user's default shell in $SHELL.
    #[structopt(long, env = "SHELL", default_value = "/bin/sh")]
    sh: String,

    /// Do not interpret the command as a shell command, but run it directly
    #[structopt(long, short, conflicts_with = "sh")]
    no_sh: bool,

    /// timeout when waiting for the master merge socket to appear
    #[structopt(short, long, default_value = "5000")]
    timeout: u32,

    /// The time in ms to wait after closing output streams of the non-leading merge processes,
    /// to prevent a pipeline on a merge slave's stdout from messing up the pipeline on the merge master.
    #[structopt(short, long, default_value = "5")]
    pause_after_close: u32,

    /// Output file for the merged stream. If not used, the merged stream will be written to the
    /// leading process's stdout.
    #[structopt(short, long)]
    output: Option<String>,

    /// input file. If not specified, stdin is used
    #[structopt(short, long)]
    input: Option<String>,

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

        if let Some(0) = self.split {
            panic!("The number of splits must be > 0");
        }

        return self;
    }
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

struct FileDeleter<'a> {
    path: &'a Path,
}
impl Drop for FileDeleter<'_> {
    fn drop(&mut self) {
        if let Err(e) = std::fs::remove_file(self.path) {
            eprintln!("Error: Deleting listening socket at {} failed: {:?}", self.path.to_string_lossy(), e);
        };
    }
}

/// An extension trait to ease conversion to usize. The conversion panics on failure, so we assume
/// that usize is at least as wide as u32.
trait AsUSize {
    fn usize(self) -> usize;
}
impl AsUSize for u16 {
    fn usize(self) -> usize {
        usize::try_from(self).unwrap()
    }
}
impl AsUSize for u32 {
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

    fn read_from_stream(mut r: SSRawFd, errmsg: &str) -> (Header, [u8; size_of::<Header>()]) {
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

/// Verifies that all readers are at EOF
fn check_all_eof<'a>(readers: &mut [SSRawFd]) {
    let mut buf = [0u8; 1];
    for r in readers {
        let count = r.read(&mut buf).unwrap();
        if count != 0 {
            panic!(
                "Not all merge streams were finished at the same time. This could indicate data corruption"
            );
        }
    }
}

/// Build the unix socket path to listen on
fn socket_path(socket_dir: &str, guid: &[u8; 16]) -> PathBuf {
    let mut sock = PathBuf::from(socket_dir);
    let filename = format!(MERGE_SOCKET_NAME!(), hex::encode(guid));
    sock.push(filename);
    sock
}

/// Generate a random byte array from /dev/urandom
fn random() -> [u8; 16] {
    let mut r = File::open("/dev/urandom").expect("Failed to open /dev/urandom");
    let mut buf = [0u8; 16];
    r.read(&mut buf).expect("Error reading random number from /dev/urandom");
    buf
}

/// Build the right Command given the input and options
fn command(cmd: &[String]) -> Command {
    if OPTS.no_sh {
        let mut com = Command::new(&cmd[0]);
        com.args(&cmd[1..]);
        return com;
    } else {
        let shellcmd = cmd.join(" ");
        let mut com = Command::new(&OPTS.sh);
        com.arg("-c");
        com.arg(shellcmd);
        return com;
    }
}

static OPTS: Lazy<Opt> = Lazy::new(|| Opt::from_args().init());

fn main() {
    if let Some(splits) = OPTS.split {
        split(splits);
    }

    if OPTS.merge {
        merge();
    }
}

fn split(num: u16) {
    let mut inp = io::stdin().as_ssrawfd();
    let infile;
    match &OPTS.input.as_ref().map(|s| s.as_str()) {
        None | Some("-") => {}
        Some(path) => {
            infile = File::open(path).expect("unable to open input file");
            inp = infile.as_ssrawfd();
        }
    }

    let mut children = Vec::<Child>::with_capacity(num.usize());
    let mut children_fds = Vec::with_capacity(num.usize());

    let mut header = Header::default();
    header.blocksize = OPTS.blocksize;
    header.splits = num;
    header.streamsplit_id = random();
    debug!("Instance id: {}", hex::encode(header.streamsplit_id));

    for i in 0..num {
        let child = command(&OPTS.command)
            .stdin(Stdio::piped())
            .env("STREAMSPLIT_N", i.to_string())
            .spawn()
            .expect("Unable to start child process");

        header.stream_id = i;

        let mut fd = child.stdin.as_ref().unwrap().as_ssrawfd();
        if !OPTS.no_header {
            fd.write_all(header.bytes().as_slice()).expect("Error writing header to output");
        }
        children.push(child);
        children_fds.push(fd);
        debug!("Splitter child {} started", i);
    }

    let blocksize = OPTS.blocksize.usize();
    let mut transfer = Transfer::new("Splitter", OPTS.verbose);
    'outer: loop {
        for i in 0..children.len() {
            if transfer.block(inp, children_fds[i], blocksize) == InputState::Done {
                break 'outer;
            }
        }
    }

    debug!("Splitter input finished, closing children");

    for child in children.iter_mut() {
        drop(child.stdin.take());
    }

    let mut failure = false;
    for (i, child) in children.iter_mut().enumerate() {
        let status = child.wait().unwrap_or_else(|e| {
            panic!("Error waiting on child {}: {:?}", i, e);
        });
        if !status.success() {
            eprintln!("Error: Child {} exited with status {}", i, status.code().unwrap());
            failure = true;
        }
        debug!("Splitter child {} exited", i);
    }
    if failure {
        panic!("Child process exited with error");
    }

    children.clear();
    debug!("splitter exiting");
}

fn merge() {
    // The file descriptor for stdin will stay open, despite dropping the stdin object
    let inp = io::stdin().as_ssrawfd();
    let (hdr, hdrbytes) = Header::read_from_stream(inp, "Invalid input stream");

    let file: File;
    if hdr.stream_id == 0 {
        let mut cmd;
        let out = match OPTS.output.as_ref().map(String::as_str) {
            None if !OPTS.command.is_empty() => {
                cmd = command(&OPTS.command)
                    .stdin(Stdio::piped())
                    .spawn()
                    .expect("Unable to start child process");
                cmd.stdin.as_mut().unwrap().as_ssrawfd()
            }
            None | Some("-") => io::stdout().as_ssrawfd(),
            Some(f) => {
                file = File::create(f).expect(&format!("Error opening {}", f));
                file.as_ssrawfd()
            }
        };

        merge_master(hdr, inp, out);
        debug!("merge master exiting");
    } else {
        merge_slave(hdr, hdrbytes, inp);
        debug!("merge slave #{} exiting", hdr.stream_id);
    }
}

fn merge_master(hdr: Header, inp: SSRawFd, outp: SSRawFd) {
    // Set umask so only this user has access to the socket, just to be safe. It doesn't look like
    // it is possible tp pass a umask when binding the socket.
    let old_umask = umask(0o177);
    let socketpath = socket_path(&OPTS.socket_dir, &hdr.streamsplit_id);
    let listener = UnixListener::bind(socketpath.as_path()).unwrap_or_else(|e| {
        panic!("Unable to create socket at {}: {:?}", socketpath.as_path().to_string_lossy(), e)
    });
    umask(old_umask);
    let deleter = FileDeleter { path: &socketpath };

    // The socket positions are at stream_id - 1
    let mut sockets = Vec::with_capacity(hdr.splits.usize() - 1);
    sockets.resize_with(sockets.capacity(), || None);

    debug!("Merge master: socket created. 1 of {} merge streams connected", hdr.splits);

     // The current process also counts for 1
    let mut connected_slaves = 1;
    while connected_slaves < hdr.splits {
        match listener.accept() {
            Err(e) => {
                eprintln!("Error listening on merge socket: {}", e);
                continue;
            }
            Ok((stream, _addr)) => {
                let (streamhdr, _) = Header::read_from_stream(stream.as_ssrawfd(), "Invalid merge stream");
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

                if let Some(_) = replace(&mut sockets[streamhdr.stream_id.usize() - 1], Some(stream)) {
                    panic!(
                        "Invalid merge stream: Multiple streams with id {} tried to connect",
                        streamhdr.stream_id
                    );
                }
                connected_slaves += 1;
                debug!("Merge master: {} of {} merge streams connected", connected_slaves, hdr.splits);
            }
        }
    }

    // remove socket file
    drop(deleter);

    let mut streams: Vec<SSRawFd> = Vec::with_capacity(hdr.splits.usize());
    streams.push(inp);
    streams.extend(sockets.iter().map(|s| s.as_ref().unwrap().as_ssrawfd()));

    sleep(Duration::from_millis(u64::from(OPTS.pause_after_close)));

    let blocksize = OPTS.blocksize.usize();
    let mut transfer = Transfer::new("Merge master", OPTS.verbose);
    'outer: loop {
        for i in 0..streams.len() {
            if transfer.block(streams[i], outp, blocksize) == InputState::Done {
                break 'outer;
            }
        }
    }

    check_all_eof(streams.as_mut_slice());
}

fn merge_slave(hdr: Header, hdrbytes: [u8; size_of::<Header>()], inp: SSRawFd) {
    debug!("slave merger #{} trying to connect...", hdr.stream_id);

    let socketpath = socket_path(&OPTS.socket_dir, &hdr.streamsplit_id);

    let deadline = Instant::now() + Duration::from_millis(u64::from(OPTS.timeout));

    let mut interval = Duration::from_millis(1);

    // Connect to the merge master's socket, give the master some time to create it but don't wait forever
    let socketobj = loop {
        let conn = UnixStream::connect(&socketpath);
        match conn {
            Ok(socket) => break socket,
            Err(e) => {
                if e.kind() != ErrorKind::NotFound {
                    panic!("Error connecting to merge master process: {:?}", e);
                } else if Instant::now() > deadline {
                    panic!("Timeout exceeded while trying to connect to merge master");
                } else {
                    std::thread::sleep(interval);
                    interval = min(interval * 2, Duration::from_millis(300));
                    continue;
                }
            }
        }
    };
    let mut socket = socketobj.as_ssrawfd();

    // Close stdout to let any pipelined processes exit, they should only keep running from the merge master
    io::stdout().as_ssrawfd().close().expect("Closing stdout failed");

    socket.write_all(&hdrbytes).expect("Error writing to merge socket");

    debug!("slave merger #{} connected", hdr.stream_id);

    Transfer::new(&format!("slave merger #{}", hdr.stream_id), OPTS.verbose).to_end(inp, socket, 64*1024);
}
