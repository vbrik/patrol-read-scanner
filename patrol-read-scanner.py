#!/usr/bin/env python3
import argparse
import json
import multiprocessing as mp
import os
import sys
import errno
from pathlib import Path
from syslog import syslog
from time import sleep, perf_counter


def scan_device(path, read_size, delay, slow_read_threshold, problem_backoff, start_from_middle=False):
    syslog(f"patrol-read-scanner: starting new scan path={path} read_size={read_size} delay={delay} "
           f"slow_read_threshold={slow_read_threshold} problem_backoff={problem_backoff} "
           f"start_from_middle={start_from_middle}")
    dev = open(path, "rb", buffering=0)
    if start_from_middle:
        size = dev.seek(0, os.SEEK_END)
        # stay aligned
        dev.seek((int(size / 2) // read_size) * read_size)
    while True:
        start_pos = dev.tell()
        try:
            start_time = perf_counter()
            bytes_read = dev.read(read_size)
            latency = perf_counter() - start_time
        except OSError as e:
            if e.errno == errno.EIO:
                syslog(f"patrol-read-scanner ERROR: I/O error dev={dev.name} start_pos={start_pos}, read_size={read_size}")
                # move forward but stay aligned
                dev.seek(start_pos + read_size)
                sleep(problem_backoff)
            else:
                raise
        else:
            if latency > slow_read_threshold:
                syslog(f"patrol-read-scanner WARNING: slow I/O dev={dev.name}, latency={latency}s, start_pos={start_pos}, "
                       f"read_size={read_size}")
                sleep(problem_backoff)
            if len(bytes_read) == 0:
                syslog(f"patrol-read-scanner: completed scan dev={dev.name}")
                return
        sleep(delay)


def get_rotational_devices():
    rotational_file_paths = Path('/sys/devices').glob('pci*/**/rotational')
    rotational_device_names = [path.parts[-3] for path in rotational_file_paths if path.read_bytes() == b'1\n']
    return [Path(f"/dev/{dev_name}") for dev_name in rotational_device_names]


def main():
    default_delay = 0.025
    default_readsize = 1024*128
    default_slowthreshold = 0.2
    default_problembackoff = 10

    parser = argparse.ArgumentParser(
        description="This script implements a form of a disk patrol read/scan "
                    "by sequentially reading disk device file(s) in an infinite "
                    "loop. Reads are performed with pauses to reduce impact on "
                    "disk performance. To further reduce impact on performance "
                    "consider running under `ionice -c idle`. The script is meant "
                    "to run as a daemon and communicates via syslog. It reports "
                    "read failures and abnormal read latencies.",
        epilog=("Notes: [1] If device paths are not specified as arguments or in the "
                "config file, the script will use all rotational devices. Removal of "
                "a device will not result in an error, and if a device is added, the"
                "script will start scanning it. If device paths are specified, the "
                "script will error out if a device is removed, and added devices will "
                "be ignored. "
                "[2] Attempts to read from a problematic area are likely to cause very "
                "high latencies (e.g. 10 seconds) for other I/O operations, and problematic "
                "areas may be clustered together, and therefore scanned in close succession. "
                "The --problembackoff parameter is intended to soften the impact of the "
                "disruption. "
                "[3] The first scan of a device will start from the middle, rather than "
                "the beginning. The rationale is that, since rotational devices tend to "
                "fill up from the beginning, problems there are likely to get discovered "
                "'naturally' by the underlying application. Whereas, problems in the tail "
                "regions may remain undetected for longer, since a complete scan of a "
                "device is likely to take a *VERY* long time, and events like script "
                "restarts and reboots will cause tail ends of devices to be scanned less "
                "often. "))
    parser.add_argument("devpaths", nargs="*", metavar="PATH",
                        help="device files to read from [1] (default: all rotational devices)")
    parser.add_argument("--delay", metavar="SECONDS", type=float,
                        help=f"delay between reads (default: {default_delay})")
    parser.add_argument("--readsize", metavar="BYTES", type=int,
                        help=f"read() size (default: {default_readsize})")
    parser.add_argument("--slowthreshold", metavar="SECONDS", type=float,
                        help=f"slow read threshold (default: {default_slowthreshold})")
    parser.add_argument("--problembackoff", metavar="SECONDS", type=float,
                        help=f"amount of time to sleep if an IO problem is encountered [2] "
                             f"(default={default_problembackoff})")
    parser.add_argument("--conf-file", metavar="PATH",
                        help="load settings from YAML config file (command line "
                             "arguments override config file values)")
    args = parser.parse_args()

    conf = {}
    # Attempt to load config from file.
    if args.conf_file:
        with open(args.conf_file, "rb") as f:
            jsons = f.read().strip()
            if jsons:
                conf = json.loads(jsons)

    # Set parameters. Precedence: command line, then config file, then defaults.
    devpaths = args.devpaths if args.devpaths else conf.get("devpaths", None)  # None means use all rotational devs
    delay = args.delay if args.delay is not None else conf.get("delay", default_delay)
    readsize = args.readsize if args.readsize else conf.get("readsize", default_readsize)
    slowthreshold = args.slowthreshold if args.slowthreshold else conf.get("slowthreshold", default_slowthreshold)
    problembackoff = args.problembackoff if args.problembackoff else conf.get("problembackoff", default_problembackoff)

    syslog(f"patrol-read-scanner: main thread starting devpaths={devpaths or 'ALL_ROTATIONAL'}, delay={delay}, "
           f"readsize={readsize}, slowthreshold={slowthreshold}, problembackoff={problembackoff}")

    if not devpaths and not get_rotational_devices():
        syslog(f"patrol-read-scanner ERROR: no device paths specified and no rotational devices discovered")
        parser.error("Error: no device paths specified and no rotational devices discovered.")

    # Use fork to start children in case we are run under ionice
    # (not sure if other start methods preserve ionice settings).
    mp.set_start_method("fork")

    children = {}
    while True:
        for proc in children.values():
            if proc.exitcode == 1:
                syslog(f"patrol-read-scanner ERROR: terminating because child encountered an error")
                [p.kill() for p in children.values() if p.is_alive()]
                return
        for devpath in devpaths or get_rotational_devices():
            worker_args = (devpath, readsize, delay, slowthreshold, problembackoff)
            # If we have started a scan on this device before,
            # start a new scan if the old one finished.
            if devpath in children:
                if not children[devpath].is_alive():
                    children[devpath] = mp.Process(target=scan_device, args=worker_args)
                    children[devpath].start()
            # If we haven't started any scans of this device before,
            # start new scan from the middle of the device.
            else:
                children[devpath] = mp.Process(target=scan_device, args=worker_args, kwargs={'start_from_middle': True})
                children[devpath].start()
        sleep(1)


if __name__ == "__main__":
    sys.exit(main())
