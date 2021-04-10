use std::convert::{TryFrom, TryInto};
use std::fmt::{Display, Formatter};
use std::io::{self, Error, Read, Write, ErrorKind, Cursor};
use std::process::{Child, ChildStdin, Command, Stdio};

use once_cell::sync::Lazy;
use structopt::StructOpt;
use std::fs::File;
use hex;

#[derive(Debug, Default, StructOpt)]
#[structopt(
    name = "streamsplit",
    about = "Split a byte stream into multiple streams and merge them back"
)]
struct Opt {
    #[structopt(long, conflicts_with="merge")]
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
    #[structopt(long, env="SHELL", default_value="/bin/sh")]
    sh: String,

    #[structopt(short, long, default_value="5000")]
    timeout: u32,

    /// The time to wait after closing output streams of the non-leading merge processes
    #[structopt(short, long, default_value="1000")]
    pause_after_close: u32,

    /// Output file for the merged stream. If not used, the merged stream will be written to the
    /// leading process's stdout.
    #[structopt(short, long)]
    output: Option<String>,

    /// The directory to create the merging socket. Defaults to $XDG_RUNTIME_DIR or /tmp if that is not set.
    #[structopt(long, env="XDG_RUNTIME_DIR", default_value="/tmp")]
    socket_dir: String,

    //#[structopt(last)]
    #[structopt(name="command", subcommand)]
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
            Some(num) => {
                self.splitcount = num.usize()
            }
        }

        return self
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

#[repr(packed)]
#[derive(Copy, Clone, Debug)]
struct Header {
    /// "streamsplit;" in ascii as magic number
    magic: [u8; 12],
    /// The protocol/header version, currently 1
    version: u32,
    /// The number of streams the original stream was split into
    splits: u16,
    /// The (zero-based) index of this stream
    stream_id: u16,
    /// zeros
    _padding: [u8; 12],
    /// A random GUID to match different streams together
    streamsplit_id: [u8; 16],
}

struct WriteChainer<'a> { slice: &'a mut Vec<u8> }
impl WriteChainer<'_> {
    fn write(mut self, data: &[u8]) -> Self {
        let wrote = self.slice.write(&data).unwrap();
        if wrote != data.len() {
            panic!("Write to buffer failed, wrote {} out of {} bytes", wrote, data.len() )
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
    fn usize(self) -> usize { usize::try_from(self).unwrap() }
}
impl USize for u32 {
    fn usize(self) -> usize { usize::try_from(self).unwrap() }
}


impl Header {
    fn bytes(&self) -> Vec<u8> {
        let mut res = Vec::with_capacity(std::mem::size_of::<Header>());
        WriteChainer{slice: &mut res}
            .write(&self.magic)
            .write(&self.version.to_be_bytes())
            .write(&self.splits.to_be_bytes())
            .write(&self.stream_id.to_be_bytes())
            .write(&self._padding)
            .write(&self.streamsplit_id);

        res
    }

    fn from_bytes(bytes: &[u8]) -> Header {
        let mut hdr = Header::default();
        hdr.magic = bytes[0..12].try_into().unwrap();
        hdr.version = u32::from_be_bytes(bytes[12..16].try_into().unwrap());
        hdr.splits = u16::from_be_bytes(bytes[16..18].try_into().unwrap());
        hdr.stream_id = u16::from_be_bytes(bytes[18..20].try_into().unwrap());
        hdr._padding = bytes[20..32].try_into().unwrap();
        hdr.streamsplit_id = bytes[32..48].try_into().unwrap();

        if hdr.magic != *b"streamsplit;" {
            panic!("Invalid stream: this input stream is not a valid streamsplit stream. Stream marker not found");
        }
        if hdr._padding != [0u8; 12] {
            panic!("Invalid stream: input stream header is invalid: invalid padding found")
        }
        if hdr.version != 1 {
            panic!("Invalid stream: The input stream is for version {}, this program only supports version 1", hdr.version)
        }
        if hdr.stream_id >= hdr.splits {
            panic!("Invalid stream: The input stream has a stream_id >= the number of splits, which is invalid")
        }

        hdr
    }
}

impl Default for Header {
    fn default() -> Self {
        Header {
            magic: *b"streamsplit;",
            version: 1,
            splits: 0,
            stream_id: 0,
            _padding: [0u8; 12],
            streamsplit_id: [0u8; 16],
        }
    }
}


static OPTS: Lazy<Opt> = Lazy::new(|| {Opt::from_args().init()});
//static OPTS: OnceCell<Opt> = OnceCell::new();

fn main() {

    if let Some(splits) = OPTS.split {
        split(splits, &OPTS.command);
    }

    if OPTS.merge {
        merge(&mut io::stdin());
    }
    return;

    match transfer_block(
        &mut io::stdin(),
        &mut io::stdout(),
        OPTS.blocksize.usize(),
    ) {
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

fn merge(inp: &mut io::Stdin) {
    let mut hdrbuffer = [0u8; std::mem::size_of::<Header>()];
    inp.read(&mut hdrbuffer);
    let hdr = Header::from_bytes(&hdrbuffer);

    println!("parsed: {:?}", hdr);
}

fn random() -> [u8; 16] {
    let mut r = File::open("/dev/urandom").expect("Failed to open /dev/urandom");
    let mut buf = [0u8; 16];
    r.read(&mut buf);
    buf
}

fn split(num: u16, command: &Vec<String>) -> Result<(), Error> {
    let mut pipes = Vec::<Child>::with_capacity(num.usize());

    let mut header = Header::default();
    header.splits = num;
    header.streamsplit_id = random();
    if OPTS.verbose {
        eprintln!("Instance id: {}", hex::encode(header.streamsplit_id));
    }

    for i in 0..num {
        let child = Command::new(&command[0])
            .stdin(Stdio::piped())
            .env("STREAMSPLIT_N", (i+1).to_string())
            .args(&command[1..])
            .spawn()?;

        header.stream_id = i;
        child.stdin.as_ref().unwrap().write(header.bytes().as_slice());
        pipes.push(child);
    }

    let mut current_child = 0u16;

    loop {
        let inputstate = transfer_block(
            &mut io::stdin(),
            &mut pipes[current_child.usize()].stdin.as_ref().unwrap(),
            OPTS.blocksize.usize(),
        )
        .unwrap();
        if inputstate == InputState::Done {
            break;
        }

        current_child = (current_child + 1) % num;
    }

    pipes.clear();

    return Ok(());
}

fn transfer_block(
    inp: &mut io::Stdin,
    out: &mut dyn Write,
    blocksize: usize,
) -> Result<InputState, Error> {
    let mut buffer = vec![0u8; blocksize];

    let mut readcount = 0usize;
    let mut writecount = 0usize;
    while readcount < blocksize {
        let read = inp.read(&mut buffer[readcount..])?;
        if read == 0 {
            return Ok(InputState::Done);
        }
        readcount += read;

        while readcount > writecount {
            writecount += out.write(&buffer[writecount..readcount])?;
        }
    }
    return Ok(InputState::More);
}
