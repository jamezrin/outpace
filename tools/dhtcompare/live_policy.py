"""Fixed live-boundary destination policy shared by the relay and inner nftables."""
import ipaddress

# IANA IPv4 special-purpose and non-unicast prefixes. Anything else is a permitted
# global unicast destination. The relay and the nftables set use this same list.
DENIED_V4 = (
    '0.0.0.0/8', '10.0.0.0/8', '100.64.0.0/10', '127.0.0.0/8', '169.254.0.0/16',
    '172.16.0.0/12', '192.0.0.0/24', '192.0.2.0/24', '192.88.99.0/24',
    '192.168.0.0/16', '198.18.0.0/15', '198.51.100.0/24', '203.0.113.0/24',
    '224.0.0.0/4', '240.0.0.0/4',
)
_DENIED = tuple(ipaddress.IPv4Network(prefix) for prefix in DENIED_V4)

WORKER_DEVICE = 'dhtw0'
RELAY_DEVICE = 'dhtr0'
WORKER_ADDRESS = '10.200.0.2'
GATEWAY_ADDRESS = '10.200.0.1'
PREFIX_LENGTH = 30
MTU_PAYLOAD = 1472
TABLE = 'dht_live'
RULE_COMMENTS = ('out_lo', 'out_udp_denied', 'out_udp', 'out_icmp_drop', 'out_drop',
                 'in_lo', 'in_udp_established', 'in_drop')


def permitted(address, port):
    """Return whether one IPv4 UDP destination is a permitted global endpoint."""
    try:
        ip = ipaddress.IPv4Address(address)
    except (ValueError, TypeError):
        return False
    return type(port) is int and 0 < port <= 65535 and not any(ip in n for n in _DENIED)


def nft_ruleset():
    """Return the fixed default-deny inner ruleset for one worker namespace.

    UDP to a denied prefix is counted and passed to the relay, which drops it
    silently: a synchronous EPERM would penalize libraries for this harness rather
    than model loss. Every other non-UDP, IPv6 or unsolicited packet is dropped here.
    """
    elements = ', '.join(DENIED_V4)
    w = WORKER_DEVICE
    return f'''table inet {TABLE} {{
  set denied4 {{ type ipv4_addr; flags interval; elements = {{ {elements} }} }}
  chain output {{
    type filter hook output priority filter; policy drop;
    oifname "lo" counter accept comment "out_lo"
    oifname "{w}" meta nfproto ipv4 meta l4proto udp ip daddr @denied4 counter accept comment "out_udp_denied"
    oifname "{w}" meta nfproto ipv4 meta l4proto udp counter accept comment "out_udp"
    meta l4proto icmp counter drop comment "out_icmp_drop"
    counter drop comment "out_drop"
  }}
  chain input {{
    type filter hook input priority filter; policy drop;
    iifname "lo" counter accept comment "in_lo"
    iifname "{w}" meta nfproto ipv4 meta l4proto udp ct state established counter accept comment "in_udp_established"
    counter drop comment "in_drop"
  }}
}}
'''


def ruleset_counters(listing):
    """Validate an `nft -j list ruleset` structure and return packet counters by rule.

    Exactly one table, one interval set and two drop-policy chains with the fixed
    comment sequence are accepted. Counters are packet totals; bytes are discarded.
    """
    items = listing.get('nftables') if type(listing) is dict else None
    if type(items) is not list:
        raise ValueError('policy_structure')
    found = {'table': [], 'set': [], 'chain': [], 'rule': []}
    for item in items:
        if type(item) is not dict or len(item) != 1:
            raise ValueError('policy_structure')
        (kind, body), = item.items()
        if kind == 'metainfo':
            continue
        if kind not in found or type(body) is not dict:
            raise ValueError('policy_structure')
        found[kind].append(body)
    tables, sets, chains, rules = found['table'], found['set'], found['chain'], found['rule']
    if [(t.get('family'), t.get('name')) for t in tables] != [('inet', TABLE)]:
        raise ValueError('policy_structure')
    if len(sets) != 1 or sets[0].get('name') != 'denied4' or sets[0].get('type') != 'ipv4_addr':
        raise ValueError('policy_structure')
    loaded = []
    for element in sets[0].get('elem', ()):
        prefix = element.get('prefix') if type(element) is dict else None
        if type(prefix) is not dict:
            raise ValueError('policy_structure')
        loaded.append(f"{prefix.get('addr')}/{prefix.get('len')}")
    if tuple(loaded) != DENIED_V4:
        raise ValueError('policy_structure')
    shape = sorted((c.get('name'), c.get('hook'), c.get('policy'), c.get('type')) for c in chains)
    if shape != [('input', 'input', 'drop', 'filter'), ('output', 'output', 'drop', 'filter')]:
        raise ValueError('policy_structure')
    comments = tuple(r.get('comment') for r in rules)
    if comments != RULE_COMMENTS or any(r.get('table') != TABLE for r in rules):
        raise ValueError('policy_structure')
    counters = {}
    for rule in rules:
        counted = [e['counter'] for e in rule.get('expr', ())
                   if type(e) is dict and type(e.get('counter')) is dict]
        if len(counted) != 1 or type(counted[0].get('packets')) is not int:
            raise ValueError('policy_structure')
        counters[rule['comment']] = counted[0]['packets']
    return counters
