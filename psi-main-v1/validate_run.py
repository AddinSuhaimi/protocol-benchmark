"""Independent reconciliation of framed clients, requests, counters and main endpoints."""
import csv
import hashlib
import json
import math
from pathlib import Path
import collection_main as c
from study_common import *

MOD=1<<256

def row_token(row):
    # CSV and JSON representations round-trip to the same typed record.
    canonical=[float(row[i]) for i in range(4)]+[int(row[4]),bool(row[5]),str(row[6])]
    if not all(math.isfinite(v) for v in canonical[:4]) or canonical[3]<0:
        raise ValueError('Nonfinite/negative request timing')
    if abs((canonical[2]-canonical[1])*1000-canonical[3])>1e-6: raise ValueError('Request duration mismatch')
    if canonical[5] and (canonical[4]!=200 or canonical[6]): raise ValueError('Invalid success record')
    return int.from_bytes(hashlib.sha256(json.dumps(canonical,separators=(',',':')).encode()).digest(),'big')


def reconcile_exports(folder):
    folder=Path(folder);clients=read_json(folder/'clients.json');receipts=read_json(folder/'export-integrity.json')
    if len(clients)!=8 or len(receipts)!=8: raise ValueError('Expected eight complete client exports')
    counts={};digests={};exports=[]
    for i in range(8):
        parsed=c.parse_export((folder/f'load-output-{i}.log').read_bytes())
        if parsed is None: raise ValueError('Incomplete client export')
        data,receipt=parsed
        if receipt!=receipts[i] or not receipt['checksum_verified']: raise ValueError('Export receipt mismatch')
        if data['process_index']!=i or data['http_client']!='httpclient' or data['idle_control']:
            raise ValueError('Client identity/mode mismatch')
        rows=data['rows'];counts[i]=len(rows);digests[i]=sum(row_token(r) for r in rows)%MOD
        if clients[i]['attempts']!=len(rows) or clients[i]['successes']!=sum(r[5] for r in rows): raise ValueError('Client count mismatch')
        for k in ('users','start_epoch','end_epoch','admission_seconds','warmup_failures','client_cpu_seconds'):
            if clients[i][k]!=data[k]: raise ValueError('Client metadata mismatch: '+k)
        start=data['target_start_epoch'];end=start+data['admission_seconds']
        for r in rows:
            if r[0]<start-.001 or r[0]>=end+.001: raise ValueError('Request outside admission interval')
        exports.append({k:v for k,v in data.items() if k!='rows'})
    csvcounts={i:0 for i in range(8)};csvdigests={i:0 for i in range(8)};latencies=[];attempts=0;status={}
    with (folder/'requests.csv').open(newline='') as f:
        for r in csv.DictReader(f):
            if r['success'] not in ('True','False'): raise ValueError('Invalid CSV success value')
            i=int(r['process_index'])
            if i not in csvcounts: raise ValueError('Unexpected client index')
            row=[r['start_epoch'],r['start_monotonic'],r['end_monotonic'],r['latency_ms'],r['http_status'],r['success']=='True',r['error']]
            csvcounts[i]+=1;csvdigests[i]=(csvdigests[i]+row_token(row))%MOD;attempts+=1
            status[r['http_status']]=status.get(r['http_status'],0)+1
            if row[5]:latencies.append(float(row[3]))
    if csvcounts!=counts or csvdigests!=digests: raise ValueError('Merged CSV differs from framed exports')
    return clients,{'attempts':attempts,'successes':len(latencies),'failures':attempts-len(latencies),
                    'mean_success_ms':sum(latencies)/len(latencies) if latencies else None,
                    'p99_success_ms_nearest_rank':c.percentile(latencies,.99),'status_counts':status}


def audit_run(folder,planned):
    folder=Path(folder);s=read_json(folder/'summary.json');reasons=[]
    def require(condition,reason):
        if not condition: reasons.append(reason)
    settings=read_json(folder/'settings.json')
    fixed={'client':'httpclient','processes':8,'warmup':30,'seconds':120,'settle':12,'tail':12,'start_lead':3,'snapshot_lead':1.5,'scrape_interval':1,'idle_control':False,'payload_size':planned['payload_size'],'users':planned['users_total']}
    for k,v in fixed.items():require(settings.get(k)==v,'Collector setting mismatch '+k)
    require((folder/'runner.sha256').read_text().strip()==sha(Path(c.__file__)),'Collector hash changed')
    clients,counts=reconcile_exports(folder)
    for k in ['attempts','successes','failures','mean_success_ms','p99_success_ms_nearest_rank']:
        require(s[k]==counts[k],'Summary/request mismatch '+k)
    require(s['namespace']==planned['namespace'],'Namespace mismatch')
    require(s['payload_size_items']==planned['payload_size'],'Payload mismatch')
    require(s['users_total']==planned['users_total'] and sum(x['users'] for x in clients)==planned['users_total'],'User count mismatch')
    require([x['users'] for x in clients]==c.split_users(planned['users_total'],8),'User allocation changed')
    require(s['client_processes']==8 and s['http_client']=='httpclient','Client setting mismatch')
    require(all(x['admission_seconds']==120 for x in clients),'Admission mismatch')
    require(s['settling_seconds']==12 and s['tail_observation_seconds']==12,'Window settings mismatch')
    require(s['purpose']=='main_dataset_collection' and s['runner_revision']==REVISION,'Not a main collector result')
    require(s['attempts']>0,'No measured requests')
    # Recompute synchronization and preserve actual observed drain/read intervals.
    aggregate=c.aggregate_clients(clients)
    require(aggregate['start_skew_ms']<=100,'Start skew')
    for x in clients: require(abs(x['start_epoch']-x['target_start_epoch'])<=.100,'Late client start')
    before=read_json(folder/'boundary-before.json');after=read_json(folder/'boundary-after.json')
    last_client_end=max(x['end_epoch'] for x in clients)
    require(min([v['begin']['epoch'] for v in after['cpu'].values()]+[after['energy']['begin']['epoch']])>=last_client_end,'End snapshots precede client drain')
    start=min(x['start_epoch'] for x in clients);target=clients[0]['target_start_epoch']
    require(all(x['target_start_epoch']==target for x in clients),'Different scheduled starts')
    require(max([v['end']['epoch'] for v in before['cpu'].values()]+[before['energy']['end']['epoch']])<min(start,target),'Start snapshots overlap load')
    pods=read_json(folder/'pods-before.json');final=read_json(folder/'pods-after.json')
    for app in pods:
        require(c.identity(pods[app])==c.identity(final[app]),'Pod/container changed '+app)
        require((folder/(app+'-cpu.max')).read_text().startswith('max '),'CPU quota '+app)
        b=before['cpu'][app]['values'];e=after['cpu'][app]['values']
        require(all(e.get(k,-1)>=v for k,v in b.items()),'CPU counter reset '+app)
        require(e.get('nr_throttled',0)==b.get('nr_throttled',0),'Container throttled '+app)
        require(e.get('throttled_usec',0)==b.get('throttled_usec',0),'Throttle time '+app)
    expected={'gateway':pods['gateway-service']['metadata']['name'],'worker':pods['worker-service']['metadata']['name']}
    samples=read_json(folder/'energy-tail-samples.json')
    require([x['target_offset_s'] for x in samples]==list(ENDPOINTS),'Missing/duplicate energy endpoints')
    energy={};bpf={};btext=before['energy']['text']
    def optional_bpf(text):
        # Unavailable is explicit. Malformed/present resets are not silently treated as unavailable.
        if not any(line.startswith('kepler_container_bpf_cpu_time_ms_total{') and 'container_namespace="'+planned['namespace']+'"' in line for line in text.splitlines()):return None
        return metric_delta(btext,text,'kepler_container_bpf_cpu_time_ms_total',planned['namespace'],expected)/1000
    prior=None
    for x in samples:
        text=x['sample']['text'];endpoint=str(int(x['target_offset_s']))
        require(exporter_node(text)==APP_NODE,'Wrong energy exporter')
        require(node_info(text)==node_info(btext),'Exporter source metadata changed')
        energy[endpoint]=c.energy_delta(before['energy'],x['sample'],planned['namespace'],expected)
        if prior is not None: require(energy[endpoint]>=prior,'Energy decreases between endpoints')
        prior=energy[endpoint]
        bpf[endpoint]=optional_bpf(text)
    cpu=sum((after['cpu'][a]['values']['usage_usec']-before['cpu'][a]['values']['usage_usec'])/1e6 for a in ('gateway-service','worker-service'))
    require(math.isclose(cpu,s['cpu_seconds_gateway_worker'],rel_tol=1e-12,abs_tol=1e-12),'CPU summary mismatch')
    require(math.isclose(energy['0'],s['estimated_dynamic_joules_gateway_worker'],rel_tol=1e-12,abs_tol=1e-12),'Immediate energy mismatch')
    monitor=read_json(folder/'monitor-status.json')
    require(monitor['thread_stopped'] and not monitor['scrape_errors'] and not s['scrape_errors'],'Scrape collection errors')
    times=[];last=None
    with (folder/'energy-timeseries.jsonl').open() as f:
        for line in f:
            trace=json.loads(line);times.append(trace['end']['epoch'])
            values=c.energy_series(trace['text'],planned['namespace'],expected)
            if last is not None:require(values.keys()==last.keys() and all(values[k]>=last[k] for k in values),'Trace identity/reset')
            require(exporter_node(trace['text'])==APP_NODE,'Trace exporter identity')
            require(node_info(trace['text'])==node_info(btext),'Trace source metadata changed')
            last=values
    require(len(times)>=2,'No usable telemetry trace')
    if len(times)>=2:require(all(y>=x for x,y in zip(times,times[1:])),'Trace timestamp reversal')
    n=counts['successes'];primary=energy['12']
    return {'schema':REVISION,'planned':planned,'technical_valid':not reasons,'technical_reasons':reasons,**counts,
            'application_failure_gate_pass':counts['failures']==0,'warmup_failures':aggregate['warmup_failures'],
            'elapsed_with_drain':aggregate['elapsed_with_drain'],'admission_seconds':120,
            'successful_rps_with_drain':n/aggregate['elapsed_with_drain'],
            'successful_requests_per_admission_second':n/120,'failure_rate':counts['failures']/counts['attempts'] if counts['attempts'] else None,
            'cpu_seconds_gateway_worker':cpu,'cpu_seconds_per_success':cpu/n if n else None,
            'estimated_dynamic_joules_primary_12s':primary,'estimated_joules_per_success_primary_12s':primary/n if n else None,
            'energy_joules_by_tail':energy,'energy_per_success_by_tail':{k:v/n if n else None for k,v in energy.items()},
            'bpf_cpu_seconds_by_tail':bpf,'bpf_cpu_seconds_per_success_12s':bpf['12']/n if n and bpf['12'] is not None else None,
            'start_skew_ms':aggregate['start_skew_ms'],
            'scrape_cadence_seconds':{'count':len(times),'min':min((y-x for x,y in zip(times,times[1:])),default=None),
                                     'max':max((y-x for x,y in zip(times,times[1:])),default=None)},
            'input_sha256':{p.name:sha(p) for p in folder.iterdir() if p.is_file() and p.name not in ('main-outcomes.json','environment-after.json','planned-run.json')},
            'interpretation':'Fixed +12s padded batch model estimate. No idle subtraction; no physical validation; no independent CPU-energy claim.'}
