"""Offline tests: no Kubernetes, network endpoint, or deployment changes."""
import copy
import csv
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import collection_main as c
import run_main as runner
import analyze_main as analysis
from study_common import *
from validate_run import audit_run

MANIFEST=Path(__file__).parent/'PSI_Main_Study_Order_v0_1.json'


def metrics(ns,pods,e,bpf):
    lines=[f'kepler_node_info{{components_power_source="estimator",cpu_architecture="K8",platform_power_source="none",source="os"}} 1',
           f'kepler_node_cpu_info{{instance="{APP_NODE}"}} 1']
    for name,app in [('gateway','gateway-service'),('worker','worker-service')]:
        labels=f'container_namespace="{ns}",pod_name="{pods[app]["metadata"]["name"]}",container_name="{name}"'
        lines += [f'kepler_container_joules_total{{{labels},mode="dynamic",source=""}} {e/2}',
                  f'kepler_container_bpf_cpu_time_ms_total{{{labels},source="bpf"}} {bpf*500}']
    return '\n'.join(lines)+'\n'


def fixture(folder,row,fail=False):
    """Small full-evidence synthetic run, deliberately not physical/pilot data."""
    folder.mkdir(parents=True)
    write_json(folder/'settings.json',{'client':'httpclient','processes':8,'warmup':30,'seconds':120,'settle':12,'tail':12,'start_lead':3,'snapshot_lead':1.5,'scrape_interval':1,'idle_control':False,'payload_size':row['payload_size'],'users':row['users_total']})
    (folder/'runner.sha256').write_text(sha(Path(c.__file__))+'\n')
    pods={a:{'metadata':{'name':a+'-pod','uid':a+'-uid'},'spec':{'nodeName':CLIENT_NODE if a=='loadgenerator' else APP_NODE},
             'status':{'containerStatuses':[{'containerID':a+'-id','restartCount':0}]}} for a in ('gateway-service','worker-service','loadgenerator')}
    sizes=c.split_users(row['users_total'],8);clients=[];requests=[];receipts=[]
    starts=1000.0;finish=1120.0;counts=0
    # Factor structure with a known multiplicative interaction and nondegenerate CPU/energy relation.
    factor={'ns-json':1,'ns-msgpack':.9,'ns-grpc':.6,'ns-flatbuffers':.8}[row['namespace']]
    p=2 if row['payload_size']==50 else 1;u=1.5 if row['users_total']==100 else 1
    interaction=1.25 if p==2 and u==1.5 else 1
    energy=100*factor*p*u*interaction*(1+.01*row['macroblock'])
    cpu=energy/20
    for i in range(8):
        ok=not(fail and i==0)
        latency=10*factor*p*u*interaction
        r=[1000.1+i*.001,10.0,10.0+latency/1000,latency,200 if ok else 502,ok,'' if ok else 'status_or_body']
        data={'rows':[r],'idle_control':False,'http_client':'httpclient','process_index':i,'users':sizes[i],
              'start_epoch':starts,'target_start_epoch':starts,'end_epoch':finish,'admission_seconds':120,
              'warmup_failures':0,'client_cpu_seconds':1,'elapsed_with_drain':120,'client_maxrss_kib':1000}
        payload=json.dumps(data).encode();digest=hashlib.sha256(payload).hexdigest()
        frame=f'READY\nDONE\nEXPORT_BEGIN {len(payload)} {digest}\n'.encode()+payload+b'\nEXPORT_END\n'
        (folder/f'load-output-{i}.log').write_bytes(frame);(folder/f'load-stderr-{i}.log').write_text('')
        receipts.append({'payload_bytes':len(payload),'sha256':digest,'checksum_verified':True})
        clients.append({**{k:v for k,v in data.items() if k!='rows'},'attempts':1,'successes':int(ok)})
        requests.append(r+[i])
    def boundary(t,after=False):
        return {'cpu':{a:{'begin':{'epoch':t,'monotonic':t},'end':{'epoch':t+.01,'monotonic':t+.01},
                          'values':{'usage_usec':int(1e6+(cpu/2*1e6 if after and a!='loadgenerator' else 0)),
                                    'nr_throttled':0,'throttled_usec':0}} for a in pods},
                'energy':{'begin':{'epoch':t,'monotonic':t},'end':{'epoch':t+.01,'monotonic':t+.01},
                          'text':metrics(row['namespace'],pods,100+(.95*energy if after else 0),1+(cpu if after else 0))}}
    before=boundary(999);after=boundary(1120,True);samples=[]
    for tail in ENDPOINTS:
        sample=copy.deepcopy(after['energy']);sample['text']=metrics(row['namespace'],pods,100+energy*(.95+.05*tail/12),1+cpu)
        sample['begin']['epoch']+=tail;sample['end']['epoch']+=tail
        samples.append({'target_offset_s':tail,'actual_offset_s':tail+.01,'sample':sample})
    # The immediate endpoint agrees with boundary-after.
    n=sum(r[5] for r in requests);lat=[r[3] for r in requests if r[5]]
    actual_cpu=sum((after['cpu'][a]['values']['usage_usec']-before['cpu'][a]['values']['usage_usec'])/1e6 for a in ('gateway-service','worker-service'))
    summary={'namespace':row['namespace'],'payload_size_items':row['payload_size'],'users_total':row['users_total'],
             'client_processes':8,'http_client':'httpclient','settling_seconds':12,'tail_observation_seconds':12,
             'purpose':'main_dataset_collection','runner_revision':REVISION,'attempts':8,'successes':n,'failures':8-n,
             'mean_success_ms':sum(lat)/n if n else None,'p99_success_ms_nearest_rank':c.percentile(lat,.99),
             'cpu_seconds_gateway_worker':actual_cpu,'estimated_dynamic_joules_gateway_worker':energy*.95,'scrape_errors':[]}
    for name,obj in [('pods-before.json',pods),('pods-after.json',pods),('boundary-before.json',before),('boundary-after.json',after),
                     ('energy-tail-samples.json',samples),('clients.json',clients),('export-integrity.json',receipts),('summary.json',summary),
                     ('monitor-status.json',{'scrape_errors':[],'thread_stopped':True})]:write_json(folder/name,obj)
    for a in pods:(folder/(a+'-cpu.max')).write_text('max 100000\n')
    with (folder/'energy-timeseries.jsonl').open('w') as f:
        for x in [before['energy']]+[s['sample'] for s in samples]:f.write(json.dumps(x)+'\n')
    with (folder/'requests.csv').open('w',newline='') as f:
        w=csv.writer(f);w.writerow(['start_epoch','start_monotonic','end_monotonic','latency_ms','http_status','success','error','process_index']);w.writerows(requests)
    return audit_run(folder,row)


class StudyTests(unittest.TestCase):
    def setUp(self):self.tmp=tempfile.TemporaryDirectory();self.root=Path(self.tmp.name);self.manifest=validate_manifest(MANIFEST)
    def tearDown(self):self.tmp.cleanup()
    def test_manifest_and_plan_no_cluster(self):
        self.assertEqual(len(self.manifest['runs']),128)
        with patch.object(runner,'kubectl',side_effect=AssertionError('no Kubernetes')):
            with patch('sys.argv',['run_main.py','plan']):runner.main()
    def test_frame_checksum_and_truncation(self):
        r=self.manifest['runs'][0];fixture(self.root/'r',r)
        raw=(self.root/'r/load-output-0.log').read_bytes()
        self.assertIsNone(c.parse_export(raw[:-5]))
        raw=raw.replace(b'"users": 7',b'"users": 8')
        with self.assertRaises(RuntimeError):c.parse_export(raw)
    def test_full_run_reconciles_primary_and_csv(self):
        row=self.manifest['runs'][0];out=fixture(self.root/'r',row)
        self.assertTrue(out['technical_valid'],out['technical_reasons'])
        self.assertGreater(out['estimated_dynamic_joules_primary_12s'],read_json(self.root/'r/summary.json')['estimated_dynamic_joules_gateway_worker'])
        path=self.root/'r/requests.csv';path.write_text(path.read_text().replace('200,True','201,True',1))
        with self.assertRaises(ValueError):audit_run(self.root/'r',row)
    def test_reset_and_duplicate_rejected(self):
        ns='ns-json';expected={'gateway':'g','worker':'w'}
        text='\n'.join(f'kepler_container_joules_total{{container_namespace="{ns}",container_name="{n}",pod_name="{p}",mode="dynamic"}} 10' for n,p in expected.items())
        with self.assertRaises(ValueError):metric_series(text+'\n'+text,'kepler_container_joules_total',ns,expected,True)
        with self.assertRaises(ValueError):metric_delta(text,text.replace(' 10',' 9'),'kepler_container_joules_total',ns,expected,True)
    def test_application_failure_is_not_technical_failure(self):
        row=self.manifest['runs'][0];out=fixture(self.root/'r',row,True)
        self.assertTrue(out['technical_valid'],out['technical_reasons']);self.assertEqual(out['failures'],1)
        self.assertFalse(out['application_failure_gate_pass'])
    def test_throttle_and_scrape_errors_are_technical(self):
        row=self.manifest['runs'][0];fixture(self.root/'r',row)
        p=self.root/'r/boundary-after.json';d=read_json(p);d['cpu']['loadgenerator']['values']['nr_throttled']=1;write_json(p,d)
        write_json(self.root/'r/monitor-status.json',{'scrape_errors':['timeout'],'thread_stopped':True})
        out=audit_run(self.root/'r',row);self.assertFalse(out['technical_valid']);self.assertIn('Scrape collection errors',out['technical_reasons'])
    def test_replacement_budget_and_no_application_replacement(self):
        state={'attempts':[]}
        a=runner.begin_attempt(state,1);a['status']='technical_invalid'
        a=runner.begin_attempt(state,1);a['status']='accepted'
        with self.assertRaises(ValueError):runner.begin_attempt(state,1)
        a=runner.begin_attempt(state,2);a['status']='technical_invalid'
        a=runner.begin_attempt(state,2);a['status']='technical_invalid'
        with self.assertRaises(RuntimeError):runner.begin_attempt(state,2)
    def test_complete_campaign_orchestration_and_resume(self):
        out=self.root/'campaign';out.mkdir();write_json(out/'freeze.json',{})
        calls=[]
        def fake(row,folder,url):
            calls.append(row['sequence']);return {'failures':1 if row['sequence']==1 else 0}
        with patch.object(runner,'set_payload'),patch.object(runner,'check_frozen',return_value={}),patch.object(runner,'collect_one',side_effect=fake):
            # Simulated collector must create its evidence directory, as the real one does.
            def collect(row,folder,url):folder.mkdir();return fake(row,folder,url)
            with patch.object(runner,'collect_one',side_effect=collect):runner.execute_campaign(out,MANIFEST,{},'fake')
            runner.execute_campaign(out,MANIFEST,{},'fake')
        self.assertEqual(calls,list(range(1,129)))
        self.assertEqual(len([a for a in read_json(out/'campaign.json')['attempts'] if a['status']=='accepted']),8)
    def test_interruption_restarts_whole_macroblock(self):
        out=self.root/'campaign';out.mkdir();write_json(out/'freeze.json',{})
        state={'freeze_sha256':sha(out/'freeze.json'),'manifest_sha256':sha(MANIFEST),'attempts':[{'macroblock':1,'attempt':1,'status':'running','runs':[{'sequence':1}]}]}
        write_json(out/'campaign.json',state);calls=[]
        def fail(row,folder,url):calls.append(row['sequence']);raise RuntimeError('technical fault')
        with patch.object(runner,'set_payload'),patch.object(runner,'check_frozen',return_value={}),patch.object(runner,'collect_one',side_effect=fail):
            with self.assertRaises(RuntimeError):runner.execute_campaign(out,MANIFEST,{},'fake')
        s=read_json(out/'campaign.json');self.assertEqual(calls,[1]);self.assertEqual(s['attempts'][1]['attempt'],2);self.assertEqual(s['attempts'][0]['status'],'technical_invalid')
    def test_analysis_known_effects_gating_and_substitution(self):
        rows=[]
        for plan in self.manifest['runs']:
            outcome=fixture(self.root/f'r{plan["sequence"]}',plan)
            self.assertTrue(outcome['technical_valid'],outcome['technical_reasons'])
            rows.append({**outcome,**plan,'bpf_cpu_seconds_12s':outcome['bpf_cpu_seconds_by_tail']['12']})
        result=analysis.compute(rows,list(range(1,9)),1000)
        c1=next(x for x in result['workload_contrasts'] if x['namespace']=='ns-json' and x['metric']=='estimated_J_per_completion' and x['contrast']=='interaction')
        self.assertAlmostEqual(c1['geometric_effect'],1.25,places=10)
        self.assertEqual(result['ranking_substitution']['weighted_checks'],96)
        self.assertEqual(result['ranking_substitution']['weighted_agreements'],96)
        # One adverse run makes the ENTIRE protocol/workload cell ineligible.
        rows[0]['failures']=1
        bad=analysis.compute(rows,list(range(1,9)),1000)
        gate=next(x for x in bad['eligibility'] if x['condition_id']==rows[0]['condition_id'] and x['namespace']==rows[0]['namespace'])
        self.assertFalse(gate['eligible_clean'])
        paired=[x for x in bad['paired_JSON_ratios'] if x['condition_id']==rows[0]['condition_id'] and x['namespace']==rows[0]['namespace']]
        self.assertTrue(all(x['n_blocks']==0 for x in paired))
        analysis.figures(rows,result,self.root)
        self.assertTrue((self.root/'tradeoffs.png').exists())
    def test_zero_reference_and_duplicate_inputs(self):
        self.assertFalse(analysis.positive(0));self.assertFalse(analysis.positive(None))
        self.assertEqual(analysis.rank_groups({'a':1,'b':1}),[['a','b']])
        self.assertIsNone(analysis.association([], 'x','y')['pearson_r'])
        row={**self.manifest['runs'][0]}
        with self.assertRaises(ValueError):analysis.compute([row,row],[1],1000)
    def test_protocol_role_binding_and_exact_node_identity(self):
        c1={'env':[{'name':'SERVICE_ROLE','value':'gateway'},{'name':'PROTOCOL','value':'grpc'},{'name':'WORKER_HOST','value':'worker-service'}]}
        validate_binding('ns-grpc','gateway-service',c1)
        with self.assertRaises(ValueError):validate_binding('ns-json','gateway-service',c1)
        text='kepler_node_cpu_info{instance="'+APP_NODE+'"} 1\n'
        self.assertEqual(exporter_node(text),APP_NODE)
        with self.assertRaises(ValueError):exporter_node(text+'kepler_node_cpu_info{instance="other-node"} 1\n')
    def test_payload_rollout_only_changes_when_needed(self):
        commands=[]
        def fake(*args,**kwargs):
            commands.append(args)
            if 'get' in args:return json.dumps({'spec':{'template':{'spec':{'containers':[{'env':[{'name':'PAYLOAD_SIZE','value':'10'}]}]}}}})
            return ''
        pods={'loadgenerator':{'metadata':{'name':'client'}}}
        with patch.object(runner,'kubectl',side_effect=fake),patch.object(c,'select_pods',return_value=pods),patch.object(c,'kube',return_value=''):
            runner.set_payload(10,self.root)
            self.assertFalse(any('set' in command for command in commands))
            commands.clear();runner.set_payload(50,self.root)
            self.assertEqual(sum('set' in command for command in commands),4)
            self.assertEqual(sum('rollout' in command for command in commands),4)
    def test_frozen_configuration_drift_rejected(self):
        freeze={'source_hashes':source_hashes(),'manifest_sha256':sha(MANIFEST),'energy_url':'fake','environment':{'stable':{'image':'a'}}}
        with patch.object(runner,'capture_environment',return_value={'stable':{'image':'a'}}):
            runner.check_frozen(freeze,MANIFEST,'fake')
        with patch.object(runner,'capture_environment',return_value={'stable':{'image':'b'}}):
            with self.assertRaises(RuntimeError):runner.check_frozen(freeze,MANIFEST,'fake')
        freeze['source_hashes']={}
        with self.assertRaises(RuntimeError):runner.check_frozen(freeze,MANIFEST,'fake')
    def test_end_to_end_analysis_reaudit_and_cli(self):
        root=self.root/'campaign';root.mkdir()
        write_json(root/'freeze.json',{'source_hashes':source_hashes()})
        state={'freeze_sha256':sha(root/'freeze.json'),'manifest_sha256':sha(MANIFEST),'attempts':[]}
        for block in range(1,9):
            a={'macroblock':block,'attempt':1,'status':'accepted','runs':[]};state['attempts'].append(a)
            for row in [r for r in self.manifest['runs'] if r['macroblock']==block]:
                folder=root/f'r{row["sequence"]}'
                o=fixture(folder,row);write_json(folder/'main-outcomes.json',o);write_json(folder/'planned-run.json',row)
                a['runs'].append({'sequence':row['sequence'],'relative_path':folder.name})
        write_json(root/'campaign.json',state)
        with patch('sys.argv',['analyze_main.py',str(root),'--output',str(self.root/'report'),'--bootstrap','1000']):analysis.main()
        report=read_json(self.root/'report/analysis.json');self.assertEqual(report['n_runs'],128);self.assertFalse(report['partial'])
        # A missing block requires an explicitly partial report; default is fail-closed.
        state['attempts'][-1]['status']='technical_invalid';write_json(root/'campaign.json',state)
        with self.assertRaises(ValueError):analysis.load_campaign(root,MANIFEST)
        # A raw export corruption must not pass on the strength of a saved audit file.
        state['attempts'][-1]['status']='accepted';write_json(root/'campaign.json',state)
        (root/'r1/load-output-0.log').write_bytes(b'broken')
        with self.assertRaises(ValueError):analysis.load_campaign(root,MANIFEST)
    def test_measurement_client_unchanged_from_verified_base(self):
        # Frozen hashes of the two injected source constants established from the original base file.
        frozen=read_json(Path(__file__).parent/'verified-client-contract.json')
        self.assertEqual(hashlib.sha256(c.REMOTE.encode()).hexdigest(),frozen['REMOTE_sha256'])
        self.assertEqual(hashlib.sha256(c.CLIENT_ADAPTER.encode()).hexdigest(),frozen['CLIENT_ADAPTER_sha256'])

if __name__=='__main__':unittest.main()
