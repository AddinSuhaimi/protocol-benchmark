#!/usr/bin/env python3
"""Diagnostic pilot only; no deployment mutations. Python 3 stdlib on the host.
Uses gevent in the Locust container with requests or stdlib http.client;
NOT Locust histogram statistics. --client selects the adapter; neither is
assumed to have adequate capacity before calibration.
Closed loop, 1-5 ms think time. Warm-up connections are retained across a brief
settling interval. Parallel counter reads bracket load with recorded timing
uncertainty. Tail sensitivity is reported without claiming lag correction.
Kepler values are unvalidated model estimates with exporter update lag.
--users is TOTAL across --processes. Clients use a shared future start epoch;
all run on the same k3d VM clock. This is a capacity diagnostic, not final energy accounting.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
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

CLIENT_ADAPTER = r'''
# Each virtual user owns one persistent client; no automatic retry or redirect.
# Both adapters use identical explicit request headers and read the full body.
import http.client
CLIENT_HEADERS = {"User-Agent":"psi-pilot/2", "Accept":"*/*", "Accept-Encoding":"identity"}
class PilotHTTPClient:
    def __init__(self, kind, host="gateway-service", port=80):
        self.kind=kind; self.host=host; self.port=port
        if kind=="requests":
            import requests
            self.client=requests.Session()
            self.client.trust_env=False
            self.client.headers.clear()
        elif kind=="httpclient":
            self.client=http.client.HTTPConnection(host,port,timeout=15)
        else: raise ValueError("unknown client")
    def fetch(self):
        try:
            if self.kind=="requests":
                r=self.client.get("http://%s:%s/checkout"%(self.host,self.port),
                                  headers=CLIENT_HEADERS,timeout=15,allow_redirects=False)
                try: return r.status_code,r.content
                finally:r.close()
            self.client.request("GET","/checkout",headers=CLIENT_HEADERS)
            r=self.client.getresponse()
            try:return r.status,r.read()
            finally:r.close()
        except Exception:
            # Discard uncertain connection state. Do NOT retry this request.
            self.client.close()
            raise
    def close(self):self.client.close()
'''

REMOTE = r'''
from gevent import monkey
monkey.patch_all()
import gevent, time, json, sys, random, resource, select, os
# CLIENT_ADAPTER_INSERT
users=int(sys.argv[1]); warm=float(sys.argv[2]); duration=float(sys.argv[3])
index=int(sys.argv[4]); processes=int(sys.argv[5]); offset=int(sys.argv[6])
client_kind=sys.argv[7]
idle_control=(sys.argv[8]=="idle")
rngs=[random.Random(731+offset+i) for i in range(users)]
cancelled=False
warm_attempts=0; warm_failures=0
sessions=[PilotHTTPClient(client_kind) for _ in range(users)]
code,body=sessions[0].fetch()
assert code==200 and body==b"OK", "readiness failed"
rows=[]
def work(i, end, collect):
    global warm_attempts, warm_failures
    s=sessions[i]
    while not cancelled and time.perf_counter()<(end() if callable(end) else end):
        start=time.perf_counter(); wall=time.time(); code=0; error=""; ok=False
        try:
            code,body=s.fetch()
            ok=(code==200 and body==b"OK")
            if not ok: error="status_or_body"
        except Exception as e: error=type(e).__name__
        finish=time.perf_counter()
        if collect: rows.append([wall,start,finish,(finish-start)*1000,code,ok,error])
        else:
            warm_attempts+=1
            if not ok: warm_failures+=1
        gevent.sleep(rngs[i].uniform(.001,.005))
# Ramp at 30 users/s, then allow the FULL warm-up period at target concurrency.
rate=30.0/processes
warm_deadline=[float("inf")]
green=[]
for i in range(users):
    green.append(gevent.spawn(work,i,lambda:warm_deadline[0],False)); gevent.sleep(1/rate)
full_concurrency_epoch=time.time()
warm_deadline[0]=time.perf_counter()+warm
gevent.joinall(green,raise_error=True)
warmup_drained_epoch=time.time()
print("READY",flush=True)
signal=json.loads(sys.stdin.readline())
if signal.get("command")!="GO": raise RuntimeError("missing start signal")
target_epoch=float(signal["start_epoch"])
target_monotonic=time.perf_counter()+(target_epoch-time.time())
if target_monotonic<time.perf_counter(): raise RuntimeError("start signal arrived late")
gevent.sleep(max(0,target_monotonic-time.perf_counter()))
t0=time.perf_counter(); epoch=time.time(); cpu0=time.process_time()
if abs(epoch-target_epoch)>.100: raise RuntimeError("start skew exceeds 100ms")
def watch_disconnect():
    global cancelled
    while True:
        if select.select([sys.stdin],[],[],0)[0]:
            if os.read(sys.stdin.fileno(),1)==b"":
                cancelled=True; return
            raise RuntimeError("unexpected input during measurement")
        gevent.sleep(.1)
watcher=gevent.spawn(watch_disconnect)
if idle_control:
    gevent.sleep(max(0,target_monotonic+duration-time.perf_counter()))
    green=[]
else:
    green=[gevent.spawn(work,i,target_monotonic+duration,True) for i in range(users)]
    gevent.joinall(green,raise_error=True)
t1=time.perf_counter(); end_epoch=time.time(); cpu1=time.process_time()
watcher.kill()
if cancelled: raise RuntimeError("host disconnected")
print("DONE",flush=True)
# Hold the process idle while host collects endpoint snapshots.
if sys.stdin.readline().strip()!="EXPORT": raise RuntimeError("missing export signal")
requests_version=None
if client_kind=="requests":
    import requests
    requests_version=requests.__version__
for session in sessions:session.close()
print(json.dumps({"rows":rows,"idle_control":idle_control,"http_client":client_kind,"python_version":sys.version,"process_index":index,"users":users,"start_epoch":epoch,
 "target_start_epoch":target_epoch,"start_lateness_ms":(epoch-target_epoch)*1000,
 "end_epoch":end_epoch,"full_concurrency_epoch":full_concurrency_epoch,
 "warmup_drained_epoch":warmup_drained_epoch,"warmup_attempts":warm_attempts,"warmup_failures":warm_failures,"admission_seconds":duration,
 "elapsed_with_drain":t1-t0,"client_cpu_seconds":cpu1-cpu0,
 "client_maxrss_kib":resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
 "requests_version":requests_version,"gevent_version":gevent.__version__}),flush=True)
'''
REMOTE = REMOTE.replace('# CLIENT_ADAPTER_INSERT', CLIENT_ADAPTER)

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

def split_users(total,processes):
    if processes<1 or total<processes:raise ValueError('Need at least one user per process')
    q,r=divmod(total,processes)
    return [q+(i<r) for i in range(processes)]

def aggregate_clients(data):
    starts=[d['start_epoch'] for d in data]
    elapsed=max(d['end_epoch'] for d in data)-min(starts)
    if elapsed<=0:raise ValueError('Invalid measurement duration')
    skew=(max(starts)-min(starts))*1000
    if skew>100:raise RuntimeError('Cross-process start skew exceeds 100ms')
    return {'start_epoch':min(starts),'elapsed_with_drain':elapsed,
            'admission_seconds':data[0]['admission_seconds'],
            'client_cpu_seconds':sum(d['client_cpu_seconds'] for d in data),
            'client_cpu_cores_aggregate':sum(d['client_cpu_seconds'] for d in data)/elapsed,
            'client_maxrss_kib_sum':sum(d['client_maxrss_kib'] for d in data),
            'start_skew_ms':skew,'warmup_failures':sum(d['warmup_failures'] for d in data)}

def collect_boundary(ns,pods,url):
    """Independent counter reads in parallel; each retains an uncertainty interval."""
    names=['gateway-service','worker-service','loadgenerator']
    with ThreadPoolExecutor(max_workers=4) as pool:
        cpu={name:pool.submit(cpu_snapshot,ns,pods[name]) for name in names}
        energy=pool.submit(scrape,url)
        return {'cpu':{name:f.result() for name,f in cpu.items()},'energy':energy.result()}

def energy_delta(before,after,ns,expected):
    x=energy_series(before['text'],ns,expected);y=energy_series(after['text'],ns,expected)
    if x.keys()!=y.keys():raise RuntimeError('Energy identities changed')
    if any(y[k]<x[k] for k in x):raise RuntimeError('Energy counter reset')
    return sum(y[k]-x[k] for k in x)

def boundary_diagnostics(before,after,process_data,rows):
    start=min(d['start_epoch'] for d in process_data)
    end=max(d['end_epoch'] for d in process_data)
    result={'request_admission_seconds':process_data[0]['admission_seconds'],
            'client_phase_start_epoch':start,'client_phase_end_with_drain_epoch':end,
            'first_request_start_epoch':min((r[0] for r in rows),default=None),
            'last_request_completion_epoch':max((r[0]+r[3]/1000 for r in rows),default=None),
            'counter_read_intervals':{}}
    for name in ['gateway-service','worker-service','loadgenerator','energy']:
        b=before['energy'] if name=='energy' else before['cpu'][name]
        e=after['energy'] if name=='energy' else after['cpu'][name]
        result['counter_read_intervals'][name]={
          'start_read_begin_offset_s':b['begin']['epoch']-start,
          'start_read_end_offset_s':b['end']['epoch']-start,
          'end_read_begin_offset_s':e['begin']['epoch']-end,
          'end_read_end_offset_s':e['end']['epoch']-end,
          'counter_interval_duration_lower_s':e['begin']['epoch']-b['end']['epoch'],
          'counter_interval_duration_upper_s':e['end']['epoch']-b['begin']['epoch']}
    return result

def run(a,ns,folder):
    folder.mkdir(parents=True)
    pods=select_pods(ns)
    if pods['gateway-service']['spec']['nodeName']!=pods['worker-service']['spec']['nodeName']:
        raise RuntimeError('Pilot expects colocated Gateway and Worker')
    dump(folder/'pods-before.json',pods)
    dump(folder/'deployments.json',json.loads(kube(ns,'get','deployment','-o','json')))
    for app in ['gateway-service','worker-service','loadgenerator']:
        p=pods[app]
        quota=kube(ns,'exec',p['metadata']['name'],'--','cat','/sys/fs/cgroup/cpu.max').strip()
        (folder/(app+'-cpu.max')).write_text(quota+'\n')
        if not quota.startswith('max '): raise RuntimeError(f'{app}: CPU quota still set: {quota}')
    env=pods['gateway-service']['spec']['containers'][0].get('env',[])
    if not any(x['name']=='PAYLOAD_SIZE' and x.get('value')==str(a.payload_size) for x in env):
        raise RuntimeError(f'Pilot requires explicit PAYLOAD_SIZE={a.payload_size}')
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
    # All clients share one Pod but are independent Python processes.
    clients=[]
    try:
        offset=0
        for i,users in enumerate(split_users(a.users,a.processes)):
            log=folder/f'load-output-{i}.log';err=folder/f'load-stderr-{i}.log'
            output=log.open('w');errlog=err.open('w')
            args=['kubectl','-n',ns,'exec','-i',pods['loadgenerator']['metadata']['name'],
                  '--','python3','-u','-c',REMOTE,str(users),str(a.warmup),str(a.seconds),
                  str(i),str(a.processes),str(offset),a.client,'idle' if a.idle_control else 'load']
            proc=subprocess.Popen(args,stdin=subprocess.PIPE,stdout=output,stderr=errlog,text=True)
            clients.append({'proc':proc,'log':log,'output':output,'errlog':errlog})
            offset+=users
        def marker_all(word,timeout):
            deadline=time.monotonic()+timeout
            while time.monotonic()<deadline:
                ready=True
                for c in clients:
                    lines=c['log'].read_text().splitlines()
                    if word not in lines:
                        ready=False
                        if c['proc'].poll() is not None:
                            raise RuntimeError('Load process exited; inspect load-stderr-*.log')
                if ready:return
                time.sleep(.05)
            raise RuntimeError(f'Timed out waiting for all clients: {word}')
        print(f'{ns}: {a.processes} processes, {a.users} TOTAL users; ramp and warm-up',flush=True)
        marker_all('READY',a.warmup+a.users/30+90)
        # All warm-up requests have drained before READY. This fixed idle guard
        # lets delayed updates arrive; it is NOT proof the exporter has settled.
        settling_begin=clock(); settling_initial=scrape(a.energy_url)
        print(f'{ns}: observing {a.settle:g}s after warm-up (no client requests)',flush=True)
        stop.wait(a.settle)
        settling_final=scrape(a.energy_url); settling_end=clock()
        dump(folder/'settling.json',{'begin':settling_begin,'end':settling_end,
             'energy_initial':settling_initial,'energy_final':settling_final})
        go=clock(); target_epoch=go['epoch']+a.start_lead
        for c in clients:
            c['proc'].stdin.write(json.dumps({'command':'GO','start_epoch':target_epoch})+'\n')
            c['proc'].stdin.flush()
        # Read near the common future start, not before the entire start lead.
        wait=max(0,target_epoch-a.snapshot_lead-time.time())
        stop.wait(wait)
        boundary0=collect_boundary(ns,pods,a.energy_url)
        before=boundary0['cpu'];e0=boundary0['energy']
        dump(folder/'boundary-before.json',boundary0)
        if max([v['end']['epoch'] for v in before.values()]+[e0['end']['epoch']])>=target_epoch:
            raise RuntimeError('Start snapshots overlapped scheduled load. Increase --snapshot-lead and --start-lead.')
        print(f'{ns}: measuring {a.seconds:g}s (idle control={a.idle_control})',flush=True)
        marker_all('DONE',a.seconds+a.start_lead+45)
        done=clock(); tail_origin=time.monotonic()
        boundary1=collect_boundary(ns,pods,a.energy_url)
        after=boundary1['cpu'];e1=boundary1['energy']
        dump(folder/'boundary-after.json',boundary1)
        # Retain independent endpoint candidates, never silently choose a tail
        # or assume its entire increase belongs to measured requests.
        tail_samples=[{'target_offset_s':0,'actual_offset_s':e1['end']['monotonic']-tail_origin,'sample':e1}]
        offsets=sorted(set([min(3.,a.tail),min(6.,a.tail),min(9.,a.tail),a.tail]))
        for offset_s in offsets:
            stop.wait(max(0,tail_origin+offset_s-time.monotonic()))
            sample=scrape(a.energy_url)
            tail_samples.append({'target_offset_s':offset_s,
                'actual_offset_s':sample['end']['monotonic']-tail_origin,'sample':sample})
        dump(folder/'energy-tail-samples.json',tail_samples)
        dump(folder/'energy-post-drain.json',tail_samples[-1]['sample'])
        for c in clients:
            c['proc'].stdin.write('EXPORT\n');c['proc'].stdin.flush()
        process_data=[];rows=[]
        for i,c in enumerate(clients):
            c['proc'].wait(timeout=60)
            if c['proc'].returncode:raise RuntimeError('Load process failed')
            lines=c['log'].read_text().splitlines()
            data=json.loads(next(l for l in reversed(lines) if l.startswith('{')))
            rr=data.pop('rows');rows.extend([r+[i] for r in rr])
            data['attempts']=len(rr);data['successes']=sum(r[5] for r in rr)
            data['client_cpu_cores']=data['client_cpu_seconds']/data['elapsed_with_drain']
            process_data.append(data)
        rows.sort(key=lambda r:r[0])
        with (folder/'requests.csv').open('w',newline='') as f:
            w=csv.writer(f);w.writerow(['start_epoch','start_monotonic','end_monotonic','latency_ms','http_status','success','error','process_index']);w.writerows(rows)
        data=aggregate_clients(process_data)
        dump(folder/'clients.json',process_data)
        dump(folder/'boundaries.json',{'cpu_before':before,'cpu_after':after,'energy_before':e0,'energy_after':e1,'go_sent':go,'done_observed':done,'scheduled_start_epoch':target_epoch})
        final=select_pods(ns);dump(folder/'pods-after.json',final)
        for app in pods:
            if identity(pods[app])!=identity(final[app]): raise RuntimeError(f'{app} restarted or changed')
        energy=energy_delta(e0,e1,ns,expected)
        sensitivity=[]
        for sample in tail_samples:
            delta=energy_delta(e0,sample['sample'],ns,expected)
            sensitivity.append({'target_tail_seconds':sample['target_offset_s'],
                'actual_tail_seconds':sample['actual_offset_s'],
                'raw_estimated_joules_from_start_snapshot':delta,
                'raw_estimated_joules_per_success':delta/len(rows) if rows and all(r[5] for r in rows) else None,
                'increase_over_immediate_joules':delta-energy,
                'increase_over_immediate_percent':100*(delta-energy)/energy if energy>0 else None})
        diagnostics=boundary_diagnostics(boundary0,boundary1,process_data,rows)
        dump(folder/'window-diagnostics.json',diagnostics)
        dump(folder/'energy-sensitivity.json',{'candidates':sensitivity,
            'selected_final_energy_joules':None,
            'interpretation':'Raw endpoint sensitivity only. Exporter lag, idle drift, and measurement overhead are not separable from these observations alone.'})
        cpu=0;throttle=0
        for app in ['gateway-service','worker-service']:
            b=before[app]['values'];e=after[app]['values']
            if e['usage_usec']<b['usage_usec']: raise RuntimeError('CPU counter reset')
            cpu+=(e['usage_usec']-b['usage_usec'])/1e6
            throttle+=e.get('nr_throttled',0)-b.get('nr_throttled',0)
        ok=[r[3] for r in rows if r[5]]
        summary={'namespace':ns,'http_client':a.client,'idle_control':a.idle_control,'payload_size_items':a.payload_size,'users_total':a.users,'client_processes':a.processes,'runner_revision':'coverage-pilot-v2','purpose':'diagnostic_pilot_not_main_dataset',**data,
         'attempts':len(rows),'successes':len(ok),'failures':len(rows)-len(ok),
         'successful_rps_with_drain':len(ok)/data['elapsed_with_drain'],
         'mean_success_ms':sum(ok)/len(ok) if ok else None,
         'p99_success_ms_nearest_rank':percentile(ok,.99),
         'failure_rate':(len(rows)-len(ok))/len(rows) if rows else None,
         'successful_requests_per_admission_second':len(ok)/a.seconds,
         'drain_seconds':max(0,data['elapsed_with_drain']-a.seconds),
         'energy_candidates':sensitivity,
         'energy_metric_name':'kepler_container_joules_total',
         'energy_series_labels':[dict(k) for k in energy_series(e0['text'],ns,expected)],
         'verification_flags':{
             'no_measured_failures':len(rows)==len(ok),
             'no_warmup_failures':data['warmup_failures']==0,
             'no_scrape_errors':not errors,
             'no_application_throttling':throttle==0,
             'client_headroom_requires_review':True},
         'cpu_seconds_gateway_worker':cpu,'estimated_dynamic_joules_gateway_worker':energy,
         'energy_estimate_status':'raw_immediate_endpoint_delta_not_final_PSI_input',
         'settling_seconds':a.settle,'tail_observation_seconds':a.tail,
         'tail_increase_percent':sensitivity[-1]['increase_over_immediate_percent'],
         'cpu_seconds_per_success':cpu/len(ok) if ok else None,
         'estimated_joules_per_success':energy/len(ok) if ok else None,
         'delta_nr_throttled':throttle,'scrape_errors':list(errors),
         'loadgenerator_cpu_seconds_cgroup':(after['loadgenerator']['values']['usage_usec']-before['loadgenerator']['values']['usage_usec'])/1e6,
         'loadgenerator_delta_nr_throttled':after['loadgenerator']['values'].get('nr_throttled',0)-before['loadgenerator']['values'].get('nr_throttled',0),
         'limitations':['Energy estimator not independently validated; active weights unresolved',
          'Parallel counter reads still bracket traffic and include exec/collection overhead',
          'Kepler exporter update lag remains to be assessed from time series',
          'Idle guard interrupts warm-up load; steady-state re-establishment is not proven',
          'Closed-loop gevent client; client capacity not yet established',
          'Parent cgroup quotas and shared physical host not controlled']}
        dump(folder/'summary.json',summary)
        print(json.dumps(summary,indent=2),flush=True)
        print('Energy deltas are raw diagnostic estimates; no final PSI energy input has been selected.',flush=True)
    finally:
        for c in clients:
            proc=c['proc']
            if proc.stdin and not proc.stdin.closed:
                try:proc.stdin.close()
                except BrokenPipeError:pass
            if proc.poll() is None:
                try:proc.wait(timeout=20)
                except subprocess.TimeoutExpired:
                    proc.terminate()
                    try:proc.wait(timeout=5)
                    except subprocess.TimeoutExpired:proc.kill();proc.wait()
            c['output'].close();c['errlog'].close()
        stop.set();worker.join(timeout=15)

def make_order(namespaces,repeats,seed,mode='balanced'):
    """Randomized cycles with equal positions over each complete n-block cycle.
    Position balance is not a guarantee of first-order carryover balance.
    """
    rng=random.Random(seed); order=[]; n=len(namespaces)
    if not n or len(set(namespaces))!=n:raise ValueError('Need distinct namespaces')
    for first in range(1,repeats+1,n):
        base=list(namespaces);rng.shuffle(base)
        shifts=list(range(n));rng.shuffle(shifts)
        for j in range(min(n,repeats-first+1)):
            if mode=='balanced':
                shift=shifts[j]; sequence=base[shift:]+base[:shift]
            else:
                sequence=list(namespaces);rng.shuffle(sequence)
            order.extend((first+j,ns) for ns in sequence)
    return order

def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--namespaces',nargs='+',choices=['ns-json','ns-msgpack','ns-grpc','ns-flatbuffers'],default=['ns-json'])
    p.add_argument('--payload-size',type=int,choices=[10,50],default=10,help='Expected deployed PAYLOAD_SIZE; runner does not mutate deployments')
    p.add_argument('--order',choices=['balanced','shuffle'],default='balanced',help='Balanced position rotations per complete cycle, or legacy independent shuffles')
    p.add_argument('--plan-only',action='store_true',help='Write settings and run order without contacting Kubernetes')
    p.add_argument('--repeats',type=int,default=1);p.add_argument('--users',type=int,default=50)
    p.add_argument('--client',choices=['requests','httpclient'],default='requests',help='requests baseline or lower-level stdlib HTTP client')
    p.add_argument('--processes',type=int,default=1,help='Independent clients; --users is TOTAL across them')
    p.add_argument('--start-lead',type=float,default=3,help='Seconds before shared scheduled start')
    p.add_argument('--snapshot-lead',type=float,default=1.5,help='Start parallel snapshots this many seconds before load')
    p.add_argument('--settle',type=float,default=12,help='Fixed idle observation after warm-up; not a guarantee of settled exporter')
    p.add_argument('--tail',type=float,default=12,help='Post-load energy observation seconds')
    p.add_argument('--idle-control',action='store_true',help='Warm up normally, then omit measured requests to observe background/counter overhead')
    p.add_argument('--warmup',type=float,default=30);p.add_argument('--seconds',type=float,default=120)
    p.add_argument('--energy-url',default='http://127.0.0.1:19102/metrics')
    p.add_argument('--scrape-interval',type=float,default=2)
    p.add_argument('--output',default='pilot-results');p.add_argument('--seed',type=int,default=731)
    a=p.parse_args()
    if min(a.repeats,a.users,a.processes,a.warmup,a.seconds,a.scrape_interval,a.start_lead,a.snapshot_lead,a.settle,a.tail)<=0:p.error('numeric settings must be positive')
    if a.start_lead<=a.snapshot_lead:p.error('--start-lead must exceed --snapshot-lead')
    if len(set(a.namespaces))!=len(a.namespaces):p.error('duplicate namespaces are not allowed')
    if a.processes>a.users:p.error('--processes cannot exceed total users')
    root=Path(a.output)/(time.strftime('%Y%m%d-%H%M%S')+f'-{a.client}-p{a.processes}-'+('idle-' if a.idle_control else 'load-')+str(time.time_ns()%1000000));root.mkdir(parents=True,exist_ok=False)
    dump(root/'settings.json',{**vars(a),'runner_revision':'coverage-pilot-v2'})
    (root/'runner.sha256').write_text(hashlib.sha256(Path(__file__).read_bytes()).hexdigest()+'\n')
    order=make_order(a.namespaces,a.repeats,a.seed,a.order)
    dump(root/'run-order.json',order)
    if a.plan_only:
        print(f'Plan only; no cluster calls. Evidence: {root}')
        return 0
    for block,ns in order:
        folder=root/f'block-{block}-{ns}'
        try:run(a,ns,folder)
        except Exception as e:
            folder.mkdir(parents=True,exist_ok=True);dump(folder/'FAILED.json',{'error':str(e)})
            print(f'PILOT STOPPED: {e}\nEvidence: {folder}',file=sys.stderr);return 1
    print(f'Pilot evidence: {root}')
    return 0
if __name__=='__main__':sys.exit(main())

