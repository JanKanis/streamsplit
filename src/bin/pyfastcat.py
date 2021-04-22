#!/usr/bin/env python3

# Somehow this python fastcat is actually faster than the native fastcat on my laptop. The python version uses more cpu,
# which causes the frequency scaling governor to scale to a higher frequency, which makes this script run faster than
# the native implementation which does not use as much cpu

import sys
import errno
from ctypes import *

SPLICE_F_MOVE = c_uint(1)
blocksize = c_size_t(64*1024)

libc = CDLL('libc.so.6', use_errno=True)

while True:
    ret = libc.splice(0, None, 1, None, blocksize, SPLICE_F_MOVE)
    if ret == -1:
        err = get_errno()
        if err == errno.EINVAL:
            print("splice not supported on this input/output", file=sys.stderr)
            sys.exit(1)
        print(f"Error: {errno.errorcode[err]}", file=sys.stderr)
        sys.exit(2)
    elif ret == 0:
        # EOF
        break

