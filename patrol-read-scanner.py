#!/usr/bin/env python3
import argparse
import json
import multiprocessing as mp
import os
import sys
from itertools import chain
from pathlib import Path
from syslog import syslog
from time import sleep

def worker(path, read_size, delay):
    dev = open(path, 'rb')
    # Seek to the middle of the disk. The rationale is that, since rotational
    # disks tend to get filled up starting from the beginning, there is a decent
    # chance that data will be read at some point, and, therefore, bad sectors
    # in the beginning of the disk are more likely to be discovered "naturally".
    # The tail end of a disk is less likely to contain application data, and bad
    # sectors there are less likely to be discovered. This situation is made worse
    # by the fact that this script is likely to be reading data at a VERY slow pace,
    # and restarts and reboots may interrupt the progress before the tail regions
    # of disks are scanned.
    size = dev.seek(0, os.SEEK_END)
    # ensure disks are aligned
    dev.seek((int(size / 2) // read_size) * read_size)
    while True:
        position = dev.tell()
        try:
            rsize = len(dev.read(read_size))
        except OSError as e:
            if e.errno == 5:
                syslog(f'I/O error: position={position} tell={dev.tell()}')
                dev.seek(position + read_size)
                continue
            else:
                raise
        if rsize == 0:
            dev.seek(0)
        sleep(delay)


def main():
    default_delay = 0.025
    default_readsize = 1024*128
    default_devpaths = list(chain(Path('/dev').glob('sd[a-z]'), Path('/dev').glob('sd[a-z][a-z]')))

    parser = argparse.ArgumentParser(
        description="This script implements a form of a disk patrol read/scan "
                    "by sequentially reading disk device file(s) in an infinite "
                    "loop. The reads are performed with pauses to reduce impact "
                    "on disk performance. To further reduce impact on performance "
                    "consider running under `ionice -c idle`. The script is meant"
                    "to run as a daemon and communicates via syslog.")
    parser.add_argument('devpaths', nargs='*', metavar='PATH',
                        help='device files to read from (default: /dev/sd[a-z][a-z]?)')
    parser.add_argument('--delay', metavar='SECONDS', type=float,
                        help=f'delay between reads (default: {default_delay})')
    parser.add_argument('--readsize', metavar='BYTES', type=int,
                        help=f'read() size (default: {default_readsize})')
    parser.add_argument('--conf-file', metavar='PATH',
                        help='load settings from YAML config file (command line '
                             'arguments override config file values)')
    args = parser.parse_args()

    conf = {}
    if args.conf_file:
        with open(args.conf_file, 'rb') as f:
            jsons = f.read().strip()
            if jsons:
                conf = json.loads(jsons)
    devpaths = args.devpaths if args.devpaths else conf.get('devpaths', default_devpaths)
    delay = args.delay if args.delay is not None else conf.get('delay', default_delay)
    readsize = args.readsize if args.readsize else conf.get('readsize', default_readsize)

    syslog(f'Starting patrol read scanner devpaths={devpaths}, delay={delay}, readsize={readsize}')
    mp.set_start_method('fork')
    procs = [mp.Process(target=worker, args=(devpath, readsize, delay)) for devpath in devpaths]
    [p.start() for p in procs]
    [p.join() for p in procs]


if __name__ == '__main__':
    sys.exit(main())
