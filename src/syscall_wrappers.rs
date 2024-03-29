use libc;
use libc::{c_void, size_t, ssize_t};
use num_traits::One;
use std::convert::TryFrom;
use std::io::{Error, Read, Result, Write};
use std::ops::Neg;
use std::os::raw::c_int;
use std::os::unix::io::{AsRawFd, RawFd};
use std::ptr;

#[inline(always)]
fn cvt<N: Eq + Neg<Output = N> + One>(t: N) -> Result<N> {
    if t == -N::one() {
        Err(Error::last_os_error())
    } else {
        Ok(t)
    }
}

#[inline(always)]
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

#[repr(transparent)]
#[derive(Copy, Clone, Debug)]
pub struct SSRawFd(RawFd);

impl SSRawFd {
    pub fn close(&self) -> Result<c_int> {
        cvt(unsafe { libc::close(self.0) })
    }

    #[inline(always)]
    pub fn splice_to(&self, dest: SSRawFd, len: usize) -> Result<size_t> {
        cvtsize(unsafe {
            libc::splice(self.0, ptr::null_mut(), dest.0, ptr::null_mut(), len, libc::SPLICE_F_MOVE)
        })
    }
}

pub trait AsSSRawFd {
    fn as_ssrawfd(&self) -> SSRawFd;
}

impl<T: AsRawFd> AsSSRawFd for T {
    fn as_ssrawfd(&self) -> SSRawFd {
        SSRawFd(self.as_raw_fd())
    }
}

impl Read for SSRawFd {
    #[inline(always)]
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        cvtsize(unsafe { libc::read(self.0, buf.as_mut_ptr() as *mut c_void, buf.len() as size_t) })
    }
}

impl Write for SSRawFd {
    #[inline(always)]
    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        cvtsize(unsafe { libc::write(self.0, buf.as_ptr() as *const c_void, buf.len() as size_t) })
    }

    /// Always return success, there is no userspace buffer here
    #[inline(always)]
    fn flush(&mut self) -> Result<()> {
        Ok(())
    }
}
