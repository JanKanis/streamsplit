use std::fs::File;
use std::io;

use once_cell::sync::Lazy;
use structopt::StructOpt;

use streamsplit::syscall_wrappers::AsSSRawFd;
use streamsplit::transfer::Transfer;

#[derive(Debug, Default, StructOpt)]
#[structopt(
    name = "fastcat",
    about = "A cat command that uses splice internally when possible for higher performance"
)]
struct Opt {
    /// input file. If not specified, stdin is used
    input: Option<String>,

    /// Output file. If not specified stdout is used
    #[structopt(short, long)]
    output: Option<String>,

    #[structopt(long, short)]
    verbose: bool,
}

static OPTS: Lazy<Opt> = Lazy::new(|| Opt::from_args());

fn main() {
    let mut inp = io::stdin().as_ssrawfd();
    let infile;
    let mut outp = io::stdout().as_ssrawfd();
    let outfile;

    match OPTS.input.as_ref().map(|s| s.as_str()) {
        None | Some("-") => {}
        Some(path) => {
            infile = File::open(path).expect("unable to open input file");
            inp = infile.as_ssrawfd();
        }
    }

    match OPTS.output.as_ref().map(|s| s.as_str()) {
        None | Some("-") => {}
        Some(path) => {
            outfile = File::create(path).expect("unable to open output file");
            outp = outfile.as_ssrawfd();
        }
    }

    Transfer::new("fastcat", OPTS.verbose).to_end(inp, outp, 64*1024);
}
