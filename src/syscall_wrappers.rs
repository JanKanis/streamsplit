use libc;
use libc::{c_void, ssize_t, size_t};
use std::convert::TryFrom;
use std::io::{Error, Result, Read, Write};
use std::os::raw::{c_int};
use std::os::unix::io::{AsRawFd, RawFd};
use num_traits::{Num, One};
use std::ops::Neg;

fn cvt<N: Eq + Neg<Output = N> + One>(t: N) -> Result<N> {
    if t == -N::one() {
        Err(Error::last_os_error())
    } else {
        Ok(t)
    }
}

fn cvtsize(t: ssize_t) -> Result<usize> {
    if t == -1 {
        Err(Error::last_os_error())
    } else {
        Ok(usize::try_from(t).unwrap())
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

pub trait SSRawFd {
    fn to_c_int(&self) -> c_int;
    fn read(&mut self, buf: &mut [u8]) -> Result<usize>;
    fn write(&mut self, buf: &[u8]) -> Result<usize>;
    fn flush(&mut self) -> Result<()>;
}

impl SSRawFd for RawFd {
    fn to_c_int(&self) -> c_int {
        *self
    }

    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        cvtsize(unsafe { libc::read(self.to_c_int(), buf.as_mut_ptr() as *mut c_void, buf.len() as size_t) })
    }

    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        cvtsize(unsafe { libc::write(self.to_c_int(), buf.as_ptr() as *const c_void, buf.len() as size_t) })
    }

    /// Always return succes, there is no userspace buffer here
    fn flush(&mut self) -> Result<()> {
        Ok(())
    }
}


// impl Write for SSRawFd {
//     fn write(&mut self, buf: &[u8]) -> Result<usize> {
//         cvtsize(unsafe { libc::write(self.to_c_int(), buf.as_ptr() as *const c_void, buf.len() as size_t) })
//     }
//
//     /// Always return succes, there is no userspace buffer here
//     fn flush(&mut self) -> Result<()> {
//         Ok(())
//     }
// }