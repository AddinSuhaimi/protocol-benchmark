"""Main-study contracts. No Kubernetes operations at import time."""
import hashlib
import json
import math
import os
from collections import Counter
from pathlib import Path

NAMESPACES = ('ns-json', 'ns-msgpack', 'ns-grpc', 'ns-flatbuffers')
MANIFEST_HASH = '770990ea730cf66f5a64d1476983e8bf722163a5494ef90e67fe937816116de9'
BASE_HASH = 'c2cd5843d88b071fd6be7f37c6a91095d4f5e7b81b8510312bd57cdd7076156d'
APP_DIGEST = 'sha256:b7279c2e887f5384b0c259862ea763c31397cc9c237d52eda921db3b7d290c72'
CLIENT_DIGEST = 'sha256:aace7624daa62d895d616f5ccc861a486c742582e1445a1d52d3aae83bbaa614'
KEPLER_DIGEST = 'sha256:1c822681f48d8c6f66a4613987007107322e5320f695c1cd284cef2d343bf24b'
APP_NODE = 'k3d-protocol-benchmark-agent-0'
CLIENT_NODE = 'k3d-protocol-benchmark-agent-1'
REVISION = 'empirical-main-v1'
ENDPOINTS = (0, 3, 6, 9, 12)
WEIGHTS = (.25, .5, .75)

def sha(path):
    h = hashlib.sha256()
    with Path(path).open('rb') as f:
        for chunk in iter(lambda: f.read(1024*1024), b''): h.update(chunk)
    return h.hexdigest()

def write_json(path, obj):
    """Atomic, finite JSON with fsync before commit; caller holds campaign lock."""
    path = Path(path)
    temp = path.with_name(path.name + '.tmp')
    with temp.open('w') as f:
        json.dump(obj, f, indent=2, allow_nan=False)
        f.write('\n'); f.flush(); os.fsync(f.fileno())
    os.replace(temp, path)

def read_json(path): return json.loads(Path(path).read_text())

def source_hashes():
    root = Path(__file__).parent
    return {p.name: sha(p) for p in sorted(root.glob('*.py')) if not p.name.startswith('test_')}

def validate_manifest(path):
    if sha(path) != MANIFEST_HASH: raise ValueError('Manifest hash differs from prespecified schedule')
    m = read_json(path); rows = m['runs']
    if len(rows) != 128 or m['blocks'] != 8: raise ValueError('Expected 128 runs / eight blocks')
    expected = {(p,u,n) for p in (10,50) for u in (50,100) for n in NAMESPACES}
    if [r['sequence'] for r in rows] != list(range(1,129)): raise ValueError('Sequence mismatch')
    for b in range(1,9):
        group = [r for r in rows if r['macroblock'] == b]
        if len(group) != 16 or {(r['payload_size'],r['users_total'],r['namespace']) for r in group} != expected:
            raise ValueError('Incomplete or duplicated macroblock')
        for r in group:
            if r['condition_id'] != f"items{r['payload_size']}_users{r['users_total']}": raise ValueError('Condition mismatch')
            if r['reference_key'] != f"block-{b}:{r['condition_id']}": raise ValueError('Reference mismatch')
    for c in m['conditions']:
        rr = [r for r in rows if r['condition_id'] == c['id']]
        for n in NAMESPACES:
            if Counter(r['within_group_position'] for r in rr if r['namespace']==n) != Counter({1:2,2:2,3:2,4:2}):
                raise ValueError('Protocol positions unbalanced')
        if Counter(r['workload_group_position'] for r in rr if r['namespace']=='ns-json') != Counter({1:2,2:2,3:2,4:2}):
            raise ValueError('Workload positions unbalanced')
    required = dict(client='httpclient',processes=8,warmup_seconds=30,settle_seconds=12,
                    admission_seconds=120,tail_seconds=12,scrape_interval_seconds=1,
                    start_lead_seconds=3,snapshot_lead_seconds=1.5,primary_energy_endpoint_seconds=12,
                    automatic_idle_subtraction=False)
    for k,v in required.items():
        if m['common_settings'][k] != v: raise ValueError('Unsupported setting: '+k)
    return m

def metric_series(text, metric, ns, expected, dynamic=False):
    # Use the verified parser's escaped-label semantics, exact identities, uniqueness.
    import re
    out = {}
    for line in text.splitlines():
        if not line.startswith(metric+'{'): continue
        match = re.fullmatch(re.escape(metric)+r'\{(.*)\}\s+([^\s]+)(?:\s+\S+)?', line)
        if not match: raise ValueError('Malformed '+metric)
        labels = {k:json.loads('"'+v+'"') for k,v in re.findall(r'(\w+)="((?:\\.|[^"\\])*)"', match[1])}
        if labels.get('container_namespace') != ns: continue
        name=labels.get('container_name')
        if name not in expected or labels.get('pod_name') != expected[name]: continue
        if dynamic and labels.get('mode') != 'dynamic': continue
        value=float(match[2]); key=tuple(sorted(labels.items()))
        if not math.isfinite(value) or value < 0 or key in out: raise ValueError('Invalid/duplicate '+metric)
        out[key]=value
    for n in expected:
        if sum(dict(k).get('container_name')==n for k in out)!=1: raise ValueError('Missing/ambiguous '+metric+' '+n)
    return out

def metric_delta(before, after, metric, ns, expected, dynamic=False):
    x=metric_series(before,metric,ns,expected,dynamic);y=metric_series(after,metric,ns,expected,dynamic)
    if x.keys()!=y.keys() or any(y[k]<x[k] for k in x): raise ValueError('Counter identity/reset '+metric)
    return sum(y[k]-x[k] for k in x)

def node_info(text):
    import re
    out=[]
    for line in text.splitlines():
        if line.startswith('kepler_node_info{'):
            labels={k:json.loads('"'+v+'"') for k,v in re.findall(r'(\w+)="((?:\\.|[^"\\])*)"',line)}
            out.append(labels)
    if len(out)!=1: raise ValueError('Expected one Kepler node-info series from one exporter')
    return out[0]


def exporter_node(text):
    import re
    nodes=set()
    for line in text.splitlines():
        if not line.startswith('kepler_node_'): continue
        for value in re.findall(r'instance="((?:\\.|[^"\\])*)"',line):
            nodes.add(json.loads('"'+value+'"'))
    if len(nodes)!=1: raise ValueError('Expected one exact application-node instance in node metrics')
    return next(iter(nodes))


def validate_binding(ns, app, container):
    if app=='loadgenerator': return
    env={e['name']:e.get('value') for e in container.get('env',[])}
    if env.get('PROTOCOL')!=ns.removeprefix('ns-'): raise ValueError(ns+'/'+app+': incorrect PROTOCOL')
    expected='gateway' if app=='gateway-service' else 'worker'
    if env.get('SERVICE_ROLE')!=expected: raise ValueError(ns+'/'+app+': incorrect SERVICE_ROLE')
    if expected=='gateway' and env.get('WORKER_HOST')!='worker-service': raise ValueError(ns+': unexpected WORKER_HOST')
