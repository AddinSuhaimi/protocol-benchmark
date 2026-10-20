#!/usr/bin/env python3
"""Diagnostic pilot only; no deployment mutations. Python 3 stdlib on the host.
Uses requests/gevent already in the Locust container, NOT Locust histogram stats.
Closed loop, 1-5 ms think time. Warm-up connections are retained across a brief
snapshot barrier. Counter windows bracket load, not perfectly simultaneous.
Kepler values are unvalidated model estimates with exporter update lag.
"""
import argparse
import csv
import hashlib
import json
import math
from pathlib import Path
import random
import re
import subprocess
import sys
import threading
import time
import urllib.request

REMOTE = r'''
from gevent import monkey
monkey.patch_all()
import gevent, requests, time, json, sys, random, resource
users=int(sys.argv[1]); warm=float(sys.argv[2]); duration=float(sys.argv[3])
rngs=[random.Random(731+i) for i in range(users)]
sessions=[requests.Session() for _ in range(users)]
for s in sessions: s.trust_env=False
url="http://gateway-service/checkout"
r=sessions[0].get(url,timeout=15)
assert r.status_code==200 and r.content==b"OK", "readiness failed"
rows=[]
def work(i, end, collect):
    s=sessions[i]
    while time.perf_counter()<end:
        start=time.perf_counter(); wall=time.time(); code=0; error=""; ok=False
        try:
            r=s.get(url,timeout=15)
            code=r.status_code; ok=(code==200 and r.content==b"OK")
            if not ok: error="status_or_body"
        except requests.RequestException as e: error=type(e).__name__
        finish=time.perf_counter()
        if collect: rows.append([wall,start,finish,(finish-start)*1000,code,ok,error])
        gevent.sleep(rngs[i].uniform(.001,.005))
# Ramp at 30 users/s, then allow the FULL warm-up period at target concurrency.
end=time.perf_counter()+users/30.0+warm
green=[]
for i in range(users):
    green.append(gevent.spawn(work,i,end,False)); gevent.sleep(1/30.0)
gevent.joinall(green,raise_error=True)
print("READY",flush=True)
if sys.stdin.readline().strip()!="GO": raise RuntimeError("missing start signal")
t0=time.perf_counter(); epoch=time.time(); cpu0=time.process_time()
green=[gevent.spawn(work,i,t0+duration,True) for i in range(users)]
gevent.joinall(green,raise_error=True)
t1=time.perf_counter(); cpu1=time.process_time()
print("DONE",flush=True)
# Hold the process idle while host collects endpoint snapshots.
if sys.stdin.readline().strip()!="EXPORT": raise RuntimeError("missing export signal")
print(json.dumps({"rows":rows,"start_epoch":epoch,"admission_seconds":duration,
 "elapsed_with_drain":t1-t0,"client_cpu_seconds":cpu1-cpu0,
 "client_maxrss_kib":resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
 "requests_version":requests.__version__,"gevent_version":gevent.__version__}),flush=True)
'''

def command(args):
    p=subprocess.run(args,text=True,capture_output=True,timeout=45)
    if p.returncode: raise RuntimeError(p.stderr or p.stdout)
    return p.stdout

def kube(ns,*args): return command(['kubectl','-n',ns,*args])
def dump(path,obj): path.write_text(json.dumps(obj,indent=2))
def clock(): return {'epoch':time.time(),'monotonic':time.monotonic()}

def select_pods(ns):
    raw=json.loads(kube(ns,'get','pods','-o','json'))
    found={}
    for app in ['gateway-service','worker-service','loadgenerator']:
        items=[p for p in raw['items'] if p['metadata'].get('labels',{}).get('app')==app and not p['metadata'].get('deletionTimestamp')]
        if len(items)!=1: raise RuntimeError(f'{ns}: expected exactly one {app} Pod')
        p=items[0]
        statuses=p.get('status',{}).get('containerStatuses',[])
        if len(statuses)!=1 or not statuses[0].get('ready'): raise RuntimeError(f'{app} not ready')
        found[app]=p
    return found

def identity(p):
    s=p['status']['containerStatuses'][0]
    return [p['metadata']['uid'],s['containerID'],s['restartCount']]

def cpu_snapshot(ns,p):
    begin=clock()
    text=kube(ns,'exec',p['metadata']['name'],'--','cat','/sys/fs/cgroup/cpu.stat')
    return {'begin':begin,'end':clock(),'values':{k:int(v) for k,v in (l.split() for l in text.splitlines())}}

def scrape(url):
    begin=clock()
    with urllib.request.urlopen(url,timeout=10) as r: text=r.read().decode()
    return {'begin':begin,'end':clock(),'text':text}

# Prometheus series parser restricted to the expected cumulative energy metric.
# Label order is irrelevant; no substring-based namespace matching.
def energy_series(text,ns,expected):
    result={}
    for line in text.splitlines():
        if not line.startswith('kepler_container_joules_total{'): continue
        match=re.fullmatch(r'kepler_container_joules_total\{(.*)\}\s+([^\s]+)(?:\s+\S+)?',line)
        if not match: raise ValueError('malformed energy series')
        labels={k:json.loads('"'+v+'"') for k,v in re.findall(r'(\w+)="((?:\\.|[^"\\])*)"',match[1])}
        if labels.get('container_namespace')!=ns or labels.get('mode')!='dynamic': continue
        name=labels.get('container_name')
        if name not in expected or labels.get('pod_name')!=expected[name]: continue
        value=float(match[2])
        if not math.isfinite(value): raise ValueError('nonfinite energy')
        key=tuple(sorted(labels.items()))
        if key in result: raise ValueError('duplicate energy series')
        result[key]=value
    for name in expected:
        if sum(dict(k).get('container_name')==name for k in result)!=1:
            raise RuntimeError(f'Expected one dynamic energy series for current {name} Pod')
    return result

def percentile(values,q):
    if not values: return None
    return sorted(values)[max(0,math.ceil(q*len(values))-1)]

def run(a,ns,folder):
    folder.mkdir(parents=True)
    pods=select_pods(ns)
    if pods['gateway-service']['spec']['nodeName']!=pods['worker-service']['spec']['nodeName']:
        raise RuntimeError('Pilot expects colocated Gateway and Worker')
    dump(folder/'pods-before.json',pods)
    dump(folder/'deployments.json',json.loads(kube(ns,'get','deployment','-o','json')))
    for app in ['gateway-service','worker-service']:
        p=pods[app]
        quota=kube(ns,'exec',p['metadata']['name'],'--','cat','/sys/fs/cgroup/cpu.max').strip()
        (folder/(app+'-cpu.max')).write_text(quota+'\n')
        if not quota.startswith('max '): raise RuntimeError(f'{app}: CPU quota still set: {quota}')
    env=pods['gateway-service']['spec']['containers'][0].get('env',[])
    if not any(x['name']=='PAYLOAD_SIZE' and x.get('value')=='10' for x in env):
        raise RuntimeError('Pilot requires explicit PAYLOAD_SIZE=10')
    expected={'gateway':pods['gateway-service']['metadata']['name'],'worker':pods['worker-service']['metadata']['name']}
    initial=scrape(a.energy_url)
    if 'instance="'+pods['worker-service']['spec']['nodeName']+'"' not in initial['text']:
        raise RuntimeError('Kepler endpoint does not advertise the application node')
    energy_series(initial['text'],ns,expected)
    errors=[]; stop=threading.Event()
    def monitor():
        with (folder/'energy-timeseries.jsonl').open('w') as f:
            while not stop.is_set():
                try:
                    s=scrape(a.energy_url); f.write(json.dumps(s)+'\n'); f.flush()
                except Exception as e: errors.append(str(e))
                stop.wait(a.scrape_interval)
    worker=threading.Thread(target=monitor,daemon=True);worker.start()
    args=['kubectl','-n',ns,'exec','-i',pods['loadgenerator']['metadata']['name'],'--','python3','-u','-c',REMOTE,str(a.users),str(a.warmup),str(a.seconds)]
    # Output is written to a file to permit bounded waits and retain failed runs.
    try:
        with (folder/'load-output.log').open('w+') as output, (folder/'load-stderr.log').open('w') as errlog:
            proc=subprocess.Popen(args,stdin=subprocess.PIPE,stdout=output,stderr=errlog,text=True)
            def marker(word,timeout):
                deadline=time.monotonic()+timeout
                while time.monotonic()<deadline:
                    lines=(folder/'load-output.log').read_text().splitlines()
                    if word in lines: return
                    if proc.poll() is not None: raise RuntimeError('Load process exited; inspect load-stderr.log')
                    time.sleep(.1)
                raise RuntimeError(f'Timed out waiting for {word}')
            try:
                print(f'{ns}: ramp and warm-up',flush=True)
                marker('READY',a.warmup+a.users/30+60)
                before={app:cpu_snapshot(ns,pods[app]) for app in ['gateway-service','worker-service']}
                e0=scrape(a.energy_url)
                go=clock(); proc.stdin.write('GO\n');proc.stdin.flush()
                print(f'{ns}: measuring {a.seconds:g}s',flush=True)
                marker('DONE',a.seconds+45)
                done=clock(); e1=scrape(a.energy_url)
                after={app:cpu_snapshot(ns,pods[app]) for app in ['gateway-service','worker-service']}
                # Keep exporter samples after traffic stops to inspect delayed updates.
                time.sleep(6)
                dump(folder/'energy-post-drain.json',scrape(a.energy_url))
                proc.stdin.write('EXPORT\n');proc.stdin.flush()
                proc.wait(timeout=60)
                if proc.returncode: raise RuntimeError('Load process failed')
            finally:
                if proc.poll() is None:
                    # Closing stdin releases a remote process blocked at either barrier.
                    proc.stdin.close()
                    proc.terminate()
        lines=(folder/'load-output.log').read_text().splitlines()
        data=json.loads(next(l for l in reversed(lines) if l.startswith('{')))
        rows=data.pop('rows')
        with (folder/'requests.csv').open('w',newline='') as f:
            w=csv.writer(f);w.writerow(['start_epoch','start_monotonic','end_monotonic','latency_ms','http_status','success','error']);w.writerows(rows)
        dump(folder/'boundaries.json',{'cpu_before':before,'cpu_after':after,'energy_before':e0,'energy_after':e1,'go_sent':go,'done_observed':done})
        final=select_pods(ns);dump(folder/'pods-after.json',final)
        for app in pods:
            if identity(pods[app])!=identity(final[app]): raise RuntimeError(f'{app} restarted or changed')
        ev0=energy_series(e0['text'],ns,expected);ev1=energy_series(e1['text'],ns,expected)
        if ev0.keys()!=ev1.keys(): raise RuntimeError('Energy series identities changed')
        if any(ev1[k]<ev0[k] for k in ev0): raise RuntimeError('Energy counter reset')
        cpu=0;throttle=0
        for app in before:
            b=before[app]['values'];e=after[app]['values']
            if e['usage_usec']<b['usage_usec']: raise RuntimeError('CPU counter reset')
            cpu+=(e['usage_usec']-b['usage_usec'])/1e6
            throttle+=e.get('nr_throttled',0)-b.get('nr_throttled',0)
        ok=[r[3] for r in rows if r[5]];energy=sum(ev1[k]-ev0[k] for k in ev0)
        summary={'namespace':ns,'purpose':'diagnostic_pilot_not_main_dataset',**data,
         'attempts':len(rows),'successes':len(ok),'failures':len(rows)-len(ok),
         'successful_rps_with_drain':len(ok)/data['elapsed_with_drain'],
         'p99_success_ms_nearest_rank':percentile(ok,.99),
         'cpu_seconds_gateway_worker':cpu,'estimated_dynamic_joules_gateway_worker':energy,
         'cpu_seconds_per_success':cpu/len(ok) if ok else None,
         'estimated_joules_per_success':energy/len(ok) if ok else None,
         'delta_nr_throttled':throttle,'scrape_errors':errors,
         'limitations':['Energy estimator not independently validated; active weights unresolved',
          'Serial counter snapshots bracket traffic and include collection overhead',
          'Kepler exporter update lag remains to be assessed from time series',
          'Brief idle barrier after warm-up; CPU reads via exec perturb workload scope',
          'Closed-loop gevent/requests client; client capacity not yet established',
          'Parent cgroup quotas and shared physical host not controlled']}
        dump(folder/'summary.json',summary)
        print(json.dumps(summary,indent=2),flush=True)
    finally:
        stop.set();worker.join(timeout=15)

def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--namespaces',nargs='+',choices=['ns-json','ns-grpc'],default=['ns-json'])
    p.add_argument('--repeats',type=int,default=1);p.add_argument('--users',type=int,default=50)
    p.add_argument('--warmup',type=float,default=30);p.add_argument('--seconds',type=float,default=120)
    p.add_argument('--energy-url',default='http://127.0.0.1:19102/metrics')
    p.add_argument('--scrape-interval',type=float,default=2)
    p.add_argument('--output',default='pilot-results');p.add_argument('--seed',type=int,default=731)
    a=p.parse_args()
    if min(a.repeats,a.users,a.warmup,a.seconds,a.scrape_interval)<=0:p.error('numeric settings must be positive')
    root=Path(a.output)/time.strftime('%Y%m%d-%H%M%S');root.mkdir(parents=True,exist_ok=False)
    dump(root/'settings.json',vars(a))
    (root/'runner.sha256').write_text(hashlib.sha256(Path(__file__).read_bytes()).hexdigest()+'\n')
    rng=random.Random(a.seed);order=[]
    for block in range(1,a.repeats+1):
        nslist=list(a.namespaces);rng.shuffle(nslist)
        order.extend((block,ns) for ns in nslist)
    dump(root/'run-order.json',order)
    for block,ns in order:
        folder=root/f'block-{block}-{ns}'
        try:run(a,ns,folder)
        except Exception as e:
            folder.mkdir(parents=True,exist_ok=True);dump(folder/'FAILED.json',{'error':str(e)})
            print(f'PILOT STOPPED: {e}\nEvidence: {folder}',file=sys.stderr);return 1
    print(f'Pilot evidence: {root}')
    return 0
if __name__=='__main__':sys.exit(main())
