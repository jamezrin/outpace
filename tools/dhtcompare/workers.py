"""Fixed offline-only workers. No public-network mode or registry access."""
from dataclasses import dataclass
import ipaddress
import math
import os
import re
import time
from isolation import run_offline
from worker_records import extract_record

CUSTOM_TEST='dht::comparison::worker::custom_offline_worker'
PREFIX='OUTPACE_DHT_COMPARE_'

@dataclass(frozen=True)
class Observation:
    record: dict
    capture: object
    elapsed: float

def run_worker(role,artifact,*,target,seeds,deadline):
    """Execute one pinned trusted artifact inside owned offline namespaces.

    A 27.8-second helper limit plus 2.1 seconds of cleanup stays inside the
    caller's absolute 30-second envelope. Identity-bearing input is environment
    only; caller persists capture privately. Pinning assumes no hostile concurrent
    replacement of operator-owned binaries, as the qualification harness does.
    """
    try:
        if (role not in ('mainline','n0','custom') or type(target) is not str
                or re.fullmatch(r'[0-9a-fA-F]{40}',target) is None
                or type(seeds) is not str or not 0<len(seeds)<=256
                or type(deadline) not in (int,float) or not math.isfinite(deadline)):
            raise ValueError
        endpoints=seeds.split(',')
        if not 1<=len(endpoints)<=4 or len(set(endpoints))!=len(endpoints):raise ValueError
        normalized=set()
        for endpoint in endpoints:
            host,port=endpoint.split(':')
            if (not ipaddress.IPv4Address(host).is_loopback or not port.isascii()
                    or not port.isdigit() or not 0<int(port)<=65535):raise ValueError
            normalized.add((host,int(port)))
        if len(normalized)!=len(endpoints):raise ValueError
        if deadline-time.monotonic()<29.9:raise ValueError
    except (ValueError,TypeError,OverflowError):
        raise ValueError('invalid_offline_worker') from None
    artifact.verify()
    if deadline-time.monotonic()<29.9:raise ValueError('invalid_offline_worker')
    environment={k:v for k,v in os.environ.items() if not k.startswith(PREFIX)}
    environment.update({PREFIX+'MODE':'offline-v1',PREFIX+'ROLE':role,
                        PREFIX+'TARGET':target,PREFIX+'SEEDS':seeds})
    args=([str(artifact.path),'--exact',CUSTOM_TEST,'--ignored','--nocapture','--test-threads=1']
          if role=='custom' else [str(artifact.path),'--offline-worker',role])
    start=time.monotonic()
    result=run_offline(args,timeout=27.8,env=environment)
    elapsed=time.monotonic()-start
    if time.monotonic()>deadline:raise ValueError('offline_worker_deadline')
    record=extract_record(role,result,exact_custom_test=CUSTOM_TEST if role=='custom' else None)
    return Observation(record,result,elapsed)
