
use std::io::{self, Read, Write, Error};
use std::fmt::{Display, Formatter};
use std::convert::TryFrom;
use std::process::{Command, Stdio, ChildStdin, Child};

use structopt::{StructOpt};
use once_cell::sync::Lazy;


#[derive(Debug, Default, StructOpt)]
#[structopt(name = "streamsplit", about = "Split a byte stream into multiple streams and merge them back")]
struct Opt {
    #[structopt(short, long)]
    debug: bool,

    #[structopt(short, long)]
    verbose: bool,

    #[structopt(long)]
    split: Option<u16>,

    #[structopt(long)]
    merge: bool,

    /// The block size in which to split the datastream. A linux pipe by default has a buffer of
    /// 16 KB, so a larger block size is probably not useful.
    #[structopt(short, long,
        default_value="16384")]
    blocksize: u32,

    /// Use the user's shell to interpret the command. If an argument is given, use that as the path
    /// to the shell.
    #[structopt(long)]
    sh: Option<Option<String>>,

    command: Vec<String>,
}

// #[derive(Debug, PartialEq, StructOpt)]
// enum Subcommand {
//     #[structopt(external_subcommand)]
//     Command(Vec<String>),
// }

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


static OPTS: Lazy<Opt> = Lazy::new(Opt::from_args);
//static OPTS: OnceCell<Opt> = OnceCell::new();


fn main() {
    println!("{:?}", *OPTS);
    println!("{:?}", OPTS.blocksize);

    match OPTS.split {
        Some(splits) => {
            if OPTS.command.len() == 0 {
                panic!("no command given");
            }

            split(usize::try_from(splits).unwrap(), &OPTS.command);
        }
        None => {}
    }

    return;

    match transfer_block(&mut io::stdin(), &mut io::stdout(),
                         usize::try_from(OPTS.blocksize).unwrap()) {
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


fn split(num: usize, command: &Vec<String>) -> Result<(), Error> {

    let mut pipes = Vec::<Child>::with_capacity(num);

    for i in 1..=num {
        let child = Command::new(&command[0])
            .stdin(Stdio::piped())
            .env("STREAMSPLIT_N", i.to_string())
            .args(&command[1..])
            .spawn()?;
        //let stdin = child.stdin.unwrap();
        pipes.push(child);
    }

    let mut current_child = 0;

    loop {
        let inputstate = transfer_block(&mut io::stdin(), &mut pipes[current_child].stdin.as_ref().unwrap(),
                                        usize::try_from(OPTS.blocksize).unwrap())
            .unwrap();
        if inputstate == InputState::Done {
            break
        }

        current_child = (current_child + 1) % num;
    }

    pipes.clear();

    return Ok(());
}


fn transfer_block(inp: &mut io::Stdin, out: &mut dyn Write, blocksize: usize) -> Result<InputState,Error> {
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

