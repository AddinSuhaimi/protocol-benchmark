#!/usr/bin/env python3
"""Sequential frozen main study. Commands: plan, freeze, run, status. No import side effects."""
import argparse
import copy
import datetime as dt
import fcntl
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time
from types import SimpleNamespace

import collection_main as collector
from study_common import *

ROOT=Path(__file__).resolve().parent
DEFAULT_MANIFEST=ROOT/'PSI_Main_Study_Order_v0_1.json'


def now(): return dt.datetime.now(dt.timezone.utc).isoformat()

def kubectl(*args, timeout=240):
    p=subprocess.run(['kubectl',*args],capture_output=True,text=True,timeout=timeout)
    if p.returncode: raise RuntimeError(p.stderr or p.stdout)
    return p.stdout


def reject_overlapping_clients(ns,pod):
    # Scan argv without requiring ps/pgrep in the image; exclude this inspection process.
    code='''import os,json
out=[]
for name in os.listdir('/proc'):
 if not name.isdigit() or int(name)==os.getpid(): continue
 try:
  a=open('/proc/'+name+'/cmdline','rb').read().split(b'\\0')
  if any(b'gevent' in x and b'PilotHTTPClient' in x for x in a) or any(x.endswith((b'locust',b'locust.py')) for x in a): out.append(name)
 except (OSError,ProcessLookupError): pass
print(json.dumps(out))'''
    if json.loads(collector.kube(ns,'exec',pod,'--','python3','-c',code)):
        raise RuntimeError('Existing load client detected in '+ns+'; investigate before collection')


def capture_environment(url, check_clients=True):
    contexts=kubectl('config','current-context').strip()
    nodes=json.loads(kubectl('get','nodes','-o','json'))
    kepler=json.loads(kubectl('-n','kepler','get','daemonset','kepler','-o','json'))
    kp=json.loads(kubectl('-n','kepler','get','pods','-o','json'))
    matching=[p for p in kp['items'] if p['spec'].get('nodeName')==APP_NODE and
              p['metadata'].get('labels',{}).get('app.kubernetes.io/name')=='kepler' and not p['metadata'].get('deletionTimestamp')]
    if len(matching)!=1: raise RuntimeError('Expected exactly one Kepler Pod on application node')
    ps=matching[0]['status'].get('containerStatuses',[])
    if len(ps)!=1 or not ps[0].get('ready'): raise RuntimeError('Kepler not ready')
    initial=collector.scrape(url);info=node_info(initial['text'])
    if exporter_node(initial['text'])!=APP_NODE: raise RuntimeError('Energy URL is not the application-node exporter')
    if info.get('components_power_source')!='estimator' or info.get('platform_power_source')!='none':
        raise RuntimeError('Kepler power-source path differs from audited configuration; amend protocol first')
    declarations={};pods_all={}
    for ns in NAMESPACES:
        pods=collector.select_pods(ns);pods_all[ns]=pods;declarations[ns]={}
        for app,p in pods.items():
            expected_node=CLIENT_NODE if app=='loadgenerator' else APP_NODE
            if p['spec']['nodeName']!=expected_node: raise RuntimeError(ns+'/'+app+': unexpected node')
            c=p['spec']['containers'][0];s=p['status']['containerStatuses'][0]
            validate_binding(ns,app,c)
            if app=='loadgenerator' and not s.get('imageID','').endswith(CLIENT_DIGEST):
                raise RuntimeError('Load-generator image differs from verified coverage client')
            resources=c.get('resources',{})
            if resources.get('limits',{}).get('cpu'): raise RuntimeError(ns+'/'+app+': CPU limit present')
            if app!='loadgenerator':
                if resources.get('requests',{}).get('cpu')!='250m': raise RuntimeError('Application request must be 250m')
                if not s.get('imageID','').endswith(APP_DIGEST): raise RuntimeError('Application image differs from verified v3.0')
            quota=collector.kube(ns,'exec',p['metadata']['name'],'--','cat','/sys/fs/cgroup/cpu.max').strip()
            if not quota.startswith('max '): raise RuntimeError(ns+'/'+app+': effective CPU quota '+quota)
            # Pod UID changes are expected on payload rollout; stable runtime declarations are frozen.
            container=copy.deepcopy(c)
            container['env']=[e for e in container.get('env',[]) if e['name']!='PAYLOAD_SIZE']
            declarations[ns][app]={'node':expected_node,'imageID':s['imageID'],'container':container,'cpu.max':quota}
            if app!='gateway-service':declarations[ns][app]['identity']=collector.identity(p)
        expected={'gateway':pods['gateway-service']['metadata']['name'],'worker':pods['worker-service']['metadata']['name']}
        collector.energy_series(initial['text'],ns,expected)
        if check_clients: reject_overlapping_clients(ns,pods['loadgenerator']['metadata']['name'])
    for ns in NAMESPACES:
        for app in ('gateway-service','worker-service','loadgenerator'):
            if declarations[ns][app]['container'].get('resources',{})!=declarations['ns-json'][app]['container'].get('resources',{}):
                raise RuntimeError('Resource declarations differ across protocols for '+app)
    if not ps[0].get('imageID','').endswith(KEPLER_DIGEST):
        raise RuntimeError('Kepler image differs from audited release; document an amendment before collection')
    stable={'context':contexts,'declarations':declarations,'kepler_imageID':ps[0]['imageID'],'kepler_identity':collector.identity(matching[0]),
            'kepler_node_info':info,'kepler_container_spec':kepler['spec']['template']['spec']['containers'],
            'nodes':{n['metadata']['name']:{'uid':n['metadata']['uid'],'nodeInfo':n['status']['nodeInfo']} for n in nodes['items']}}
    return {'captured_at':now(),'stable':stable,'pods':pods_all,'kepler_pod':matching[0],
            'kepler_daemonset':kepler,'nodes':nodes,'initial_metrics':initial}


def check_frozen(freeze,manifest,url,check_clients=True):
    if freeze['source_hashes']!=source_hashes() or freeze['manifest_sha256']!=sha(manifest):
        raise RuntimeError('Frozen sources/manifest changed; create a documented new freeze before collection')
    if freeze['energy_url']!=url: raise RuntimeError('Energy URL differs from freeze')
    live=capture_environment(url,check_clients)
    if live['stable']!=freeze['environment']['stable']:
        raise RuntimeError('Runtime configuration differs from freeze (images/resources/nodes/exporter); inspect snapshots')
    return live


def set_payload(payload,folder):
    transitions=[]
    for ns in NAMESPACES:
        deployment=json.loads(kubectl('-n',ns,'get','deployment','gateway-service','-o','json'))
        containers=deployment['spec']['template']['spec']['containers']
        if len(containers)!=1: raise RuntimeError('Expected single Gateway container')
        env=containers[0].get('env',[])
        current=next((e.get('value') for e in env if e['name']=='PAYLOAD_SIZE'),None)
        changed=current!=str(payload)
        if changed:
            kubectl('-n',ns,'set','env','deployment/gateway-service','PAYLOAD_SIZE='+str(payload))
        kubectl('-n',ns,'rollout','status','deployment/gateway-service','--timeout=180s')
        transitions.append({'namespace':ns,'from':current,'to':str(payload),'changed':changed,'time':now()})
    write_json(folder/'payload-transitions.json',transitions)
    # Equivalent application completion via Gateway's Worker-validated OK. Short-lived probe outside measurement.
    for ns in NAMESPACES:
        pods=collector.select_pods(ns)
        code="import urllib.request; r=urllib.request.urlopen('http://gateway-service/checkout',timeout=20); assert r.status==200 and r.read()==b'OK'"
        deadline=time.monotonic()+60
        while True:
            try:
                collector.kube(ns,'exec',pods['loadgenerator']['metadata']['name'],'--','python3','-c',code)
                break
            except Exception:
                if time.monotonic()>deadline: raise
                time.sleep(1)


def record_event(out,event,**fields):
    with (out/'events.jsonl').open('a') as f:
        f.write(json.dumps({'time':now(),'event':event,**fields})+'\n');f.flush();os.fsync(f.fileno())


def begin_attempt(state,block):
    attempts=[a for a in state['attempts'] if a['macroblock']==block]
    if any(a['status']=='accepted' for a in attempts): raise ValueError('Block already accepted')
    if attempts and sum(max(0,len([x for x in state['attempts'] if x['macroblock']==b])-1) for b in range(1,9))>=2:
        raise RuntimeError('Two replacement-block budget exhausted; stop and document repairs/amendment')
    a={'macroblock':block,'attempt':len(attempts)+1,'status':'running','started':now(),'runs':[]}
    state['attempts'].append(a);return a


def collect_one(row,folder,url):
    a=SimpleNamespace(payload_size=row['payload_size'],users=row['users_total'],processes=8,client='httpclient',
                      warmup=30,seconds=120,settle=12,tail=12,start_lead=3,snapshot_lead=1.5,
                      scrape_interval=1,energy_url=url,idle_control=False)
    collector.run(a,row['namespace'],folder)
    # Raw immediate fields preserved in summary.json; qualified selected outcome is separate.
    from validate_run import audit_run
    result=audit_run(folder,row)
    write_json(folder/'main-outcomes.json',result)
    if not result['technical_valid']: raise RuntimeError('; '.join(result['technical_reasons']))
    return result


def execute_campaign(out,manifest,freeze,url):
    m=validate_manifest(manifest)
    state_path=out/'campaign.json'
    if state_path.exists():
        state=read_json(state_path)
        if state['freeze_sha256']!=sha(out/'freeze.json') or state['manifest_sha256']!=sha(manifest): raise RuntimeError('Campaign identity changed')
        for a in state['attempts']:
            if a['status']=='running':
                a.update(status='technical_invalid',ended=now(),reason='Interrupted process/session; whole macroblock replacement required')
                record_event(out,'interrupted_macroblock',macroblock=a['macroblock'],attempt=a['attempt'])
    else:
        state={'schema':REVISION,'created_at':now(),'freeze_sha256':sha(out/'freeze.json'),
               'manifest_sha256':sha(manifest),'attempts':[]}
    write_json(state_path,state)
    for block in range(1,9):
        if any(a['macroblock']==block and a['status']=='accepted' for a in state['attempts']): continue
        # A technically invalid attempt stops immediately. Resume is explicit; never an automatic favorable rerun.
        attempt=begin_attempt(state,block);write_json(state_path,state)
        base=out/f'block-{block:02d}-attempt-{attempt["attempt"]:02d}';base.mkdir()
        record_event(out,'macroblock_started',macroblock=block,attempt=attempt['attempt'])
        try:
            for row in [r for r in m['runs'] if r['macroblock']==block]:
                folder=base/f'run-{row["sequence"]:03d}-{row["namespace"]}'
                transition=base/f'transition-{row["sequence"]:03d}';transition.mkdir()
                if row['within_group_position']==1: set_payload(row['payload_size'],transition)
                live=check_frozen(freeze,manifest,url)
                write_json(transition/'environment-before.json',live)
                record_event(out,'run_started',macroblock=block,attempt=attempt['attempt'],planned=row)
                result=collect_one(row,folder,url)
                after=check_frozen(freeze,manifest,url)
                write_json(folder/'environment-after.json',after)
                write_json(folder/'planned-run.json',row)
                attempt['runs'].append({'sequence':row['sequence'],'relative_path':str(folder.relative_to(out)),
                                        'application_failures':result['failures'],'technical_valid':True})
                write_json(state_path,state)
                record_event(out,'run_completed',sequence=row['sequence'],macroblock=block,attempt=attempt['attempt'],failures=result['failures'])
            attempt.update(status='accepted',ended=now());write_json(state_path,state)
            record_event(out,'macroblock_accepted',macroblock=block,attempt=attempt['attempt'])
        except BaseException as e:
            if 'folder' in locals() and folder.exists():
                write_json(folder/'FAILED.json',{'planned':row,'time':now(),'technical_failure':type(e).__name__+': '+str(e)})
            attempt.update(status='technical_invalid',ended=now(),reason=type(e).__name__+': '+str(e))
            write_json(state_path,state);record_event(out,'macroblock_stopped',**{k:attempt[k] for k in ('macroblock','attempt','reason')})
            raise
    state['completed_at']=now();write_json(state_path,state)
    print('COMPLETE: 8 accepted macroblocks / 128 main runs. Analyze before making claims.')


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('command',choices=['plan','freeze','run','status'])
    p.add_argument('--manifest',type=Path,default=DEFAULT_MANIFEST)
    p.add_argument('--output',type=Path,default=Path('main-study-data'))
    p.add_argument('--energy-url',default='http://127.0.0.1:19102/metrics')
    p.add_argument('--provenance',type=Path,help='Required at freeze: operator-supplied bounded model provenance JSON')
    a=p.parse_args();m=validate_manifest(a.manifest)
    if a.command=='plan':
        print(json.dumps({'planned_runs':len(m['runs']),'blocks':8,'manifest_sha256':sha(a.manifest),'source_hashes':source_hashes()},indent=2));return
    if a.command=='status':
        state=read_json(a.output/'campaign.json')
        accepted=[x for x in state['attempts'] if x['status']=='accepted']
        print(json.dumps({'accepted_blocks':len(accepted),'accepted_runs':sum(len(x['runs']) for x in accepted),'attempts':state['attempts']},indent=2));return
    a.output.mkdir(parents=True,exist_ok=True)
    with (a.output/'.runner.lock').open('a') as lock:
        fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        if a.command=='freeze':
            if (a.output/'freeze.json').exists(): raise RuntimeError('Freeze already exists; use a fresh output directory')
            if not a.provenance: raise RuntimeError('--provenance required; complete supplied template with knowns and unknowns')
            provenance=read_json(a.provenance)
            for k in ['active_model_weights','physical_reference','model_files','estimator_path','limitations']:
                if k not in provenance: raise ValueError('Provenance missing '+k)
            env=capture_environment(a.energy_url)
            (a.output/'manifest.json').write_bytes(a.manifest.read_bytes())
            write_json(a.output/'freeze.json',{'schema':REVISION,'created_at':now(),'source_hashes':source_hashes(),
                       'manifest_sha256':sha(a.manifest),'energy_url':a.energy_url,'environment':env,
                       'model_provenance':provenance,'model_provenance_sha256':sha(a.provenance),
                       'interpretation':'Model estimates, not physically calibrated energy. CPU/energy dependence must be assessed.'})
            print('Freeze created; no benchmark or deployment mutation performed.');return
        freeze=read_json(a.output/'freeze.json');check_frozen(freeze,a.manifest,a.energy_url)
        # Cluster-wide cooperative guard, also covers different output directories / SSH sessions.
        guard='psi-main-study-active'
        kubectl('-n','kepler','create','configmap',guard,'--from-literal=output='+str(a.output.resolve()),'--from-literal=started='+now())
        try:
            execute_campaign(a.output,a.manifest,freeze,a.energy_url)
        finally:
            kubectl('-n','kepler','delete','configmap',guard,'--ignore-not-found=true')

if __name__=='__main__':
    def interrupted(signum,frame): raise KeyboardInterrupt('Signal '+str(signum))
    signal.signal(signal.SIGTERM,interrupted)
    try: main()
    except (Exception,KeyboardInterrupt) as e:
        print('MAIN STUDY STOPPED: '+str(e),file=sys.stderr);sys.exit(1)
