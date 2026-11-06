"""Version-one worker record validation. Parsing is not execution authorization."""
import json
import math
import re

U64=(1<<64)-1
FIELDS={'schema_version','candidate','terminal','peer_count','first_peer_seconds',
        'elapsed_seconds','batch_sizes','batches_observed','batch_history_truncated',
        'target_overshoot','nodes_queried','valid_responses','diagnostics_unavailable_reason'}
PREFIX=b'DHT_COMPARE_RECORD '

def _deny():
    raise ValueError('invalid_worker_record')

def _object(pairs):
    result={}
    for key,value in pairs:
        if key in result:_deny()
        result[key]=value
    return result

def _uint(value):
    return type(value) is int and 0<=value<=U64

def _number(value):
    return type(value) in (int,float) and 0<=value<=30 and math.isfinite(value)

def validate_record(payload,role):
    """Decode exactly one bounded flat record, rejecting identity-bearing extras."""
    try:
        if (role not in ('custom','mainline','n0') or type(payload) is not bytes
                or not 2<=len(payload)<=8193 or not payload.endswith(b'\n')
                or b'\n' in payload[:-1] or b'\r' in payload
                or not payload.startswith(b'{') or not payload.endswith(b'}\n')):_deny()
        value=json.loads(payload.decode('utf-8'),object_pairs_hook=_object,
                         parse_constant=lambda _: _deny())
        if type(value) is not dict or set(value)!=FIELDS:_deny()
        if type(value['schema_version']) is not int or value['schema_version']!=1:_deny()
        if value['candidate']!=role or value['terminal'] not in ('target','exhausted','deadline','adapter_error'):_deny()
        count=value['peer_count'];elapsed=value['elapsed_seconds'];first=value['first_peer_seconds']
        if not _uint(count) or count>1024 or not _number(elapsed):_deny()
        if (first is None)!=(count==0) or (first is not None and (not _number(first) or first>elapsed)):_deny()
        history=value['batch_sizes'];observed=value['batches_observed'];truncated=value['batch_history_truncated']
        if (not _uint(observed) or type(history) is not list or len(history)!=min(observed,64)
                or any(not _uint(n) for n in history) or type(truncated) is not bool
                or truncated!=(observed>64)):_deny()
        if count>0 and observed==0:_deny()
        if not truncated and sum(history)<count:_deny()
        if not _uint(value['target_overshoot']) or value['target_overshoot']!=max(count-8,0):_deny()
        if value['terminal']=='target' and count<8:_deny()
        if value['terminal'] in ('deadline','exhausted') and count>=8:_deny()
        nodes=value['nodes_queried'];responses=value['valid_responses'];reason=value['diagnostics_unavailable_reason']
        if role!='custom':
            if nodes is not None or responses is not None or reason!='library_not_exposed':_deny()
        elif nodes is None or responses is None:
            if nodes is not None or responses is not None or reason!='not_collected':_deny()
        elif not _uint(nodes) or not _uint(responses) or reason is not None:_deny()
        return value
    except (ValueError,TypeError,UnicodeError,RecursionError,OverflowError):
        raise ValueError('invalid_worker_record') from None

def extract_record(role,result,*,exact_custom_test=None):
    """Require complete successful owned process evidence before parsing stdout."""
    try:
        if (type(result.exit_code) is not int or result.exit_code!=0 or result.timed_out is not False
                or result.cleanup_complete is not True or result.truncated is not False
                or type(result.stdout) is not bytes or type(result.stderr) is not bytes
                or len(result.stdout)>65536 or len(result.stderr)>65536
                or result.cancelled_on_witness is not False
                or type(result.stdout_bytes) is not int or type(result.stderr_bytes) is not int
                or result.stdout_bytes!=len(result.stdout) or result.stderr_bytes!=len(result.stderr)):_deny()
        if role!='custom':
            if exact_custom_test is not None:_deny()
            return validate_record(result.stdout,role)
        if (type(exact_custom_test) is not str or not 0<len(exact_custom_test)<=256
                or re.fullmatch(r'[A-Za-z0-9_:]+',exact_custom_test) is None):_deny()
        witness=f'test {exact_custom_test} ... '.encode()
        summary=b'test result: ok. 1 passed; 0 failed; 0 ignored;'
        if result.stdout.count(witness)!=1 or result.stdout.count(summary)!=1:_deny()
        # libtest emits a deferred name line with parallel execution, or a split
        # name/ok line with --test-threads=1 and nocapture. Both require exact name.
        if (witness+b'ok\n' not in result.stdout
                and not (witness+b'\n' in result.stdout and b'\nok\n' in result.stdout)):_deny()
        lines=[line for line in result.stdout.split(b'\n') if line.startswith(PREFIX)]
        if len(lines)!=1:_deny()
        return validate_record(lines[0][len(PREFIX):]+b'\n',role)
    except (AttributeError,ValueError,TypeError,UnicodeError,RecursionError,OverflowError):
        raise ValueError('invalid_worker_record') from None
