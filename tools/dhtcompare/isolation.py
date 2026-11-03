"""Linux offline-only worker ownership. No external network interface is attached."""
from dataclasses import dataclass
import math
import os
from pathlib import Path
import selectors
import select
import signal
import subprocess
import sys
import time

_BOOTSTRAP = '''import os,subprocess,sys
if os.getpid()!=1:raise SystemExit(125)
subprocess.run(['/usr/bin/ip','link','set','lo','up'],check=True,timeout=2)
os.execv(sys.argv[1],sys.argv[1:])
'''

@dataclass(frozen=True)
class Result:
    exit_code: int | None
    stdout: bytes
    stderr: bytes
    stdout_bytes: int
    stderr_bytes: int
    timed_out: bool
    cleanup_complete: bool
    truncated: bool

def run_offline(argv, *, timeout, prefix_cap=65536, env=None):
    """Run in fresh user/network/PID namespaces; retain prefixes and drain both pipes.

    Timeout must leave the caller's outer deadline at least 2.1 seconds for cleanup.
    Missing namespace support fails through the original nonzero launcher exit.
    This does not prove an adapter's network behavior or join its internal actors.
    """
    if (isinstance(timeout, bool) or not isinstance(timeout, (int,float))
            or not math.isfinite(timeout) or not 0 < timeout <= 28
            or type(prefix_cap) is not int or not 0 < prefix_cap <= 65536
            or not argv or len(argv)>32
            or any(not isinstance(a,str) or not a or len(a)>16384 for a in argv)
            or not Path(argv[0]).is_absolute()):
        raise ValueError('invalid_offline_worker')
    command = ['/usr/bin/unshare','--user','--map-root-user','--net','--pid',
               '--fork','--kill-child=KILL','--mount-proc',sys.executable,
               '-I','-c',_BOOTSTRAP,*argv]
    start=time.monotonic(); deadline=start+timeout; cleanup_deadline=deadline+2
    selector=selectors.DefaultSelector(); proc=None
    retained=[bytearray(),bytearray()]; counts=[0,0]
    timed_out=False; uncertain=False; init_fd=None; init_terminal=False
    def find_init():
        nonlocal init_fd
        if init_fd is not None or proc is None: return
        fd=None
        try:
            children=Path(f'/proc/{proc.pid}/task/{proc.pid}/children').read_text().split()
            if len(children)!=1: return
            child=int(children[0]); fd=os.pidfd_open(child)
            status=Path(f'/proc/{child}/status').read_text().splitlines()
            fields={line.split(':',1)[0]:line.split(':',1)[1].split() for line in status if ':' in line}
            if fields.get('PPid')!=[str(proc.pid)] or fields.get('NSpid',[])[-1:]!=['1']: return
            if Path(f'/proc/{child}/ns/pid').stat().st_ino==Path('/proc/self/ns/pid').stat().st_ino: return
            info=Path(f'/proc/self/fdinfo/{fd}').read_text().splitlines()
            if not any(line.split()==['Pid:',str(child)] for line in info): return
            init_fd=fd; fd=None
        except (OSError, ValueError): pass
        finally:
            if fd is not None: os.close(fd)
    def kill():
        nonlocal uncertain
        if proc is not None and proc.poll() is None:
            find_init()
            if init_fd is not None:
                try:
                    # Leave unshare alive to wait for PID 1/kernel descendant teardown.
                    signal.pidfd_send_signal(init_fd,signal.SIGKILL)
                    return
                except ProcessLookupError: return
                except OSError: pass
            # No verified init ownership: contain best-effort but never claim cleanup.
            uncertain=True
            try: os.killpg(proc.pid,signal.SIGKILL)
            except ProcessLookupError: pass
            except OSError: pass
    try:
        proc=subprocess.Popen(command,stdout=subprocess.PIPE,stderr=subprocess.PIPE,
                              start_new_session=True,env=env,close_fds=True)
        for index,stream in enumerate((proc.stdout,proc.stderr)):
            os.set_blocking(stream.fileno(),False)
            selector.register(stream,selectors.EVENT_READ,index)
        while True:
            find_init()
            now=time.monotonic()
            if now>=deadline and proc.poll() is None:
                timed_out=True; kill()
            if proc.poll() is not None and not selector.get_map(): break
            if now>=cleanup_deadline:
                uncertain=True; break
            for key,_ in selector.select(min(.05,max(0,cleanup_deadline-now))):
                try: chunk=os.read(key.fd,65536)
                except BlockingIOError: continue
                except OSError:
                    uncertain=True; chunk=b''
                if not chunk:
                    selector.unregister(key.fileobj)
                    continue
                index=key.data; counts[index]+=len(chunk)
                room=prefix_cap-len(retained[index])
                retained[index].extend(chunk[:room])
    finally:
        kill()
        if proc is not None:
            try: proc.wait(timeout=min(2,max(.1,cleanup_deadline-time.monotonic())))
            except subprocess.TimeoutExpired: uncertain=True
            for stream in (proc.stdout,proc.stderr):
                if stream is not None: stream.close()
        if init_fd is not None:
            poll=select.poll(); poll.register(init_fd,select.POLLIN)
            init_terminal=any(event & select.POLLIN for _,event in poll.poll(0))
            os.close(init_fd)
            if not init_terminal: uncertain=True
        selector.close()
    reaped=proc is not None and proc.returncode is not None
    if reaped and proc.returncode < 0 and not init_terminal: uncertain=True
    return Result(proc.returncode,bytes(retained[0]),bytes(retained[1]),*counts,
                  timed_out,reaped and not uncertain,
                  any(count>prefix_cap for count in counts))
