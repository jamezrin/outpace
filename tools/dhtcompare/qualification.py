"""Fixed offline UDP qualification. This is not a live discovery comparison."""
import argparse
from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path
import stat
import time
from isolation import run_offline

@dataclass(frozen=True)
class Artifact:
    path: Path
    sha256: str

    def verify(self):
        if (not self.path.is_absolute() or len(self.sha256)!=64
                or any(c not in '0123456789abcdef' for c in self.sha256)):
            raise ValueError('artifact_pin')
        with self.path.open('rb') as source:
            metadata=os.fstat(source.fileno())
            if (not stat.S_ISREG(metadata.st_mode) or not metadata.st_mode&0o111
                    or metadata.st_size>512*1024*1024):
                raise ValueError('artifact_pin')
            digest=hashlib.file_digest(source,'sha256').hexdigest()
        if digest!=self.sha256: raise ValueError('artifact_pin')


def _write(path,data):
    with path.open('xb') as out:
        os.chmod(path,0o600)
        out.write(data)


def qualify(custom,upstream,*,output,mode="qualification"):
    """Run fixed qualification or witnessed cancellation in owned offline namespaces.

    The 180-second qualification deadline covers all cases and cleanup; it is
    separate from the proposed 120-second live block, which this module cannot run.
    The optional three-case lookup-cancellation block has a 40-second deadline.
    Artifacts are trusted operator-supplied test binaries; pins detect stale builds,
    not concurrent hostile modification of the operator's files.
    """
    if mode not in ('qualification','lookup-cancellation'):
        raise ValueError('qualification_mode')
    cancellation=mode=='lookup-cancellation'
    start=time.monotonic();deadline=start+(40 if cancellation else 180)
    custom.verify();upstream.verify()
    output=Path(output)
    output.mkdir(mode=0o700,parents=False,exist_ok=False)
    schedule=[]
    for suffix in ('target','silent','malformed_uncorrelated_duplicates'):
        for role,artifact,prefix in (
                ('custom',custom,'dht::comparison::'),
                ('mainline',upstream,''),('n0',upstream,'')):
            name=('n0' if role=='n0' else role)+'_real_udp_'+suffix
            schedule.append((artifact,prefix+name,8 if suffix=='target' else 20))
    if cancellation:
        schedule=[(custom,'dht::comparison::custom_external_lookup_cancel',8),
                  (upstream,'mainline_external_lookup_cancel',8),
                  (upstream,'n0_external_lookup_cancel',8)]
    cases=[];reason='complete'
    for artifact,name,timeout in schedule:
        if deadline-time.monotonic()<timeout+2.1:
            reason='deadline';break
        # Verify immediately before launch as well as before admitting the block.
        artifact.verify()
        if deadline-time.monotonic()<timeout+2.1:
            reason="deadline";break
        begin=time.monotonic()
        options={'cancel_on_stdout':b'dht_lookup_query_witness\n'} if cancellation else {}
        result=run_offline([str(artifact.path),'--exact',name,'--ignored','--nocapture'],timeout=timeout,**options)
        elapsed=time.monotonic()-begin
        index=len(cases)
        _write(output/f'{index}.stdout.private',result.stdout)
        _write(output/f'{index}.stderr.private',result.stderr)
        executed=(f"test {name} ... ok\n".encode() in result.stdout
                  and b"test result: ok. 1 passed; 0 failed; 0 ignored;" in result.stdout)
        passed=(executed and result.exit_code==0 and result.cleanup_complete
                and not result.timed_out and not result.truncated)
        witness=b'dht_lookup_query_witness\n' in result.stdout
        if cancellation:
            passed=(witness and result.cancelled_on_witness and result.exit_code in (-9,137)
                    and b'dht_lookup_returned_before_cancel' not in result.stdout
                    and result.cleanup_complete and not result.timed_out and not result.truncated)
        cases.append(dict(test=name,exit_code=result.exit_code,query_witness=witness,
                          cancelled_on_witness=result.cancelled_on_witness,
                          elapsed_seconds=elapsed,cleanup_complete=result.cleanup_complete,
                          timed_out=result.timed_out,truncated=result.truncated,
                          executed_exact_case=executed,stdout_bytes=result.stdout_bytes,stderr_bytes=result.stderr_bytes,
                          passed=passed))
        if not passed:
            reason='case_failed';break
    if time.monotonic()>deadline: reason='deadline'
    report=dict(mode='offline_'+mode,passed=reason=='complete' and len(cases)==len(schedule),
                stop_reason=reason,elapsed_seconds=time.monotonic()-start,cases=cases,
                custom_sha256=custom.sha256,upstream_sha256=upstream.sha256)
    _write(output/'report.private.json',(json.dumps(report,indent=2)+'\n').encode())
    return report


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--offline',action='store_true',required=True)
    parser.add_argument('--mode',choices=('qualification','lookup-cancellation'),default='qualification')
    parser.add_argument('--custom',type=Path,required=True)
    parser.add_argument('--custom-sha256',required=True)
    parser.add_argument('--upstream',type=Path,required=True)
    parser.add_argument('--upstream-sha256',required=True)
    parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args()
    report=qualify(Artifact(args.custom,args.custom_sha256),
                   Artifact(args.upstream,args.upstream_sha256),output=args.output,mode=args.mode)
    print(json.dumps({'passed':report['passed'],'stop_reason':report['stop_reason'],
                      'completed_cases':len(report['cases'])}))
    return 0 if report['passed'] else 1

if __name__=='__main__':raise SystemExit(main())
