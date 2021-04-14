use libc;
use std::io::{Error, Result};
use std::os::raw::c_int;
use std::os::unix::io::AsRawFd;

fn cvt(t: c_int) -> Result<c_int> {
    if t == -1 {
        Err(Error::last_os_error())
    } else {
        Ok(t)
    }
}

pub fn umask(mask: libc::mode_t) -> libc::mode_t {
    return unsafe { libc::umask(mask) };
}

pub trait UnixCalls: AsRawFd {
    fn close(&self) -> Result<c_int>;
}

impl<T: AsRawFd> UnixCalls for T {
    fn close(&self) -> Result<c_int> {
        cvt(unsafe { libc::close(self.as_raw_fd()) })
    }
}
