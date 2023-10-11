use std::io::{ErrorKind, Read, Write};

use crate::syscall_wrappers::SSRawFd;


#[derive(PartialEq)]
pub enum InputState {
    More,
    Done,
}

#[derive(Default)]
pub struct Transfer<'a> { try_splice: bool, err_context: &'a str, buffer: Vec<u8>, verbose: bool }

impl Transfer<'_> {
    #[inline(always)]
    pub fn new(err_context: &str, verbose: bool) -> Transfer {
        Transfer {try_splice: true, err_context, buffer: Vec::new(), verbose: verbose}
    }

    #[inline(always)]
    pub fn to_end(&mut self, inp: SSRawFd, outp: SSRawFd, blocksize: usize) {
        while self.single_read(inp, outp, blocksize) != 0 {}
    }

    #[inline(always)]
    pub fn single_read(&mut self, mut inp: SSRawFd, mut outp: SSRawFd, blocksize: usize) -> usize {
        debug_assert!(blocksize > 0);
        if self.try_splice {
            match inp.splice_to(outp, blocksize) {
                Ok(count) => { return count }
                Err(e) => {
                    if e.raw_os_error().unwrap() == libc::ENOSYS
                        || e.kind() == ErrorKind::InvalidInput {
                        self.try_splice = false;
                        if self.verbose {
                            // ensure output is written in a single write, to avoid mixing up writes from different processes
                            eprint!("{}", format!("{}: splice() not possible, falling back to read/write\n", self.err_context));
                        }
                    } else {
                        panic!("{}: Error splicing data: {:?}", self.err_context, e);
                    }
                }
            }
        }

        self.reserve(blocksize);
        let count = inp.read(&mut self.buffer).expect("{}: Error reading from input");
        outp.write_all(&self.buffer[..count]).expect("{}: Error writing to output");
        return count;
    }

    #[inline(always)]
    fn reserve(&mut self, size: usize) {
        if self.buffer.len() < size {
            self.buffer.resize(size, 0);
        }
    }

    #[inline(always)]
    pub fn block(&mut self, inp: SSRawFd, outp: SSRawFd, blocksize: usize) -> InputState {
        debug_assert!(blocksize > 0);
        let mut read = 0usize;
        while read < blocksize {
            let cnt = self.single_read(inp, outp, blocksize - read);
            if cnt == 0 {
                return InputState::Done;
            }
            read += cnt;
        }
        InputState::More
    }
}
