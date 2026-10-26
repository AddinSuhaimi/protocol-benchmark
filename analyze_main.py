#!/usr/bin/env python3
"""Run-level, matched-block empirical analysis; energy is model-estimated throughout."""
import argparse
import csv
import json
import math
from pathlib import Path
import numpy as np
from scipy import stats
from study_common import *

METRICS={'estimated_joules_per_success_primary_12s':'estimated_J_per_completion',
         'p99_success_ms_nearest_rank':'p99_success_ms','mean_success_ms':'mean_success_ms',
         'cpu_seconds_per_success':'CPU_s_per_completion',
         'successful_rps_with_drain':'completion_rate_with_drain'}


def positive(x): return x is not None and math.isfinite(x) and x>0

def summarize_logs(values,bootstrap=5000,seed=20261024):
    """Each entry is one whole-block log outcome/contrast, never one request."""
    x=np.asarray(values,dtype=float)
    if not len(x):return {'n_blocks':0,'geometric_effect':None,'t95':None,'bootstrap95':None}
    mean=float(np.mean(x));ci=None;boot=None
    if len(x)>=2:
        half=float(stats.t.ppf(.975,len(x)-1)*np.std(x,ddof=1)/math.sqrt(len(x)))
        ci=[float(np.exp(mean-half)),float(np.exp(mean+half))]
        rng=np.random.default_rng(seed)
        # Index resampling preserves the block-level pairing/contrast already constructed.
        draws=x[rng.integers(0,len(x),size=(bootstrap,len(x)))].mean(axis=1)
        boot=np.exp(np.quantile(draws,[.025,.975])).tolist()
    return {'n_blocks':len(x),'block_log_values':x.tolist(),'geometric_effect':float(np.exp(mean)),
            't95':ci,'bootstrap95':boot,'interval_scope':'pointwise; blocks from one shared VM'}


def association(rows,xkey,ykey,center=False):
    pairs=[r for r in rows if r.get(xkey) is not None and r.get(ykey) is not None and math.isfinite(r[xkey]) and math.isfinite(r[ykey])]
    x=np.asarray([r[xkey] for r in pairs],float);y=np.asarray([r[ykey] for r in pairs],float)
    if center:
        for n in NAMESPACES:
            mask=np.asarray([r['namespace']==n for r in pairs])
            if mask.any():x[mask]-=x[mask].mean();y[mask]-=y[mask].mean()
    result={'n_runs':len(x),'centered_within_protocol':center,'pearson_r':None,'spearman_r':None}
    if len(x)>2 and np.ptp(x)>0 and np.ptp(y)>0:
        result.update(pearson_r=float(np.corrcoef(x,y)[0,1]),spearman_r=float(stats.spearmanr(x,y).statistic))
    return result


def cpu_predictor(rows):
    usable=[r for r in rows if positive(r.get('cpu_seconds_per_success')) and positive(r.get('estimated_joules_per_success_primary_12s'))]
    predictions=[]
    for b in sorted({r['macroblock'] for r in usable}):
        train=[r for r in usable if r['macroblock']!=b];test=[r for r in usable if r['macroblock']==b]
        if len(train)<4 or len(test)!=4 or np.ptp([r['cpu_seconds_per_success'] for r in train])==0:continue
        x=np.array([[r['cpu_seconds_per_success'],1] for r in train]);y=np.array([r['estimated_joules_per_success_primary_12s'] for r in train])
        beta=np.linalg.lstsq(x,y,rcond=None)[0]
        for r in test:
            predicted=float(beta[0]*r['cpu_seconds_per_success']+beta[1]);observed=r['estimated_joules_per_success_primary_12s']
            predictions.append({'held_out_macroblock':b,'namespace':r['namespace'],'observed_estimated_J_per_completion':observed,
                                'predicted_estimated_J_per_completion':predicted,'absolute_percentage_error':100*abs(predicted-observed)/observed})
    return {'interpretation':'Agreement with Kepler model output; NOT physical-energy accuracy or recovered active weights',
            'leave_whole_macroblock_out_predictions':predictions,
            'MAPE_percent':float(np.mean([p['absolute_percentage_error'] for p in predictions])) if predictions else None}


def rank_groups(values):
    """Retain exact ties; never break a tie alphabetically and call it agreement."""
    return [[k for k,v in sorted(values.items()) if v==level] for level in sorted(set(values.values()))]


def substitute_rankings(rows,eligible):
    out=[]
    for (q,b) in sorted({(r['condition_id'],r['macroblock']) for r in rows}):
        group={r['namespace']:r for r in rows if r['condition_id']==q and r['macroblock']==b and eligible[(q,r['namespace'])]}
        if set(group)!=set(NAMESPACES):
            out.append({'condition_id':q,'macroblock':b,'defined':False,'reason':'All four clean eligible protocols and positive references required'});continue
        ref=group['ns-json'];e='estimated_joules_per_success_primary_12s';cpu='cpu_seconds_per_success';lat='p99_success_ms_nearest_rank'
        if not all(positive(r[k]) for r in group.values() for k in (e,cpu,lat)):
            out.append({'condition_id':q,'macroblock':b,'defined':False,'reason':'Nonpositive/undefined input'});continue
        checks=[]
        for w in WEIGHTS:
            # log scores are monotonic equivalents; avoid multiplying small measurements.
            es={n:w*math.log(r[e]/ref[e])+(1-w)*math.log(r[lat]/ref[lat]) for n,r in group.items()}
            cs={n:w*math.log(r[cpu]/ref[cpu])+(1-w)*math.log(r[lat]/ref[lat]) for n,r in group.items()}
            er,cr=rank_groups(es),rank_groups(cs)
            entry={'weight_energy':w,'energy_order':er,'cgroup_CPU_order':cr,'same_order':er==cr}
            bk='bpf_cpu_seconds_per_success_12s'
            if all(positive(r.get(bk)) for r in group.values()):
                bs={n:w*math.log(r[bk]/ref[bk])+(1-w)*math.log(r[lat]/ref[lat]) for n,r in group.items()}
                entry.update(BPF_CPU_order=rank_groups(bs),BPF_same_order=er==rank_groups(bs))
            checks.append(entry)
        out.append({'condition_id':q,'macroblock':b,'defined':True,'weighted_checks':checks,
                    'energy_only_order':rank_groups({n:r[e] for n,r in group.items()}),
                    'CPU_only_order':rank_groups({n:r[cpu] for n,r in group.items()})})
    defined=[x for x in out if x['defined']]
    return {'sets':out,'defined_sets':len(defined),'weighted_checks':sum(len(x['weighted_checks']) for x in defined),
            'weighted_agreements':sum(c['same_order'] for x in defined for c in x['weighted_checks']),
            'interpretation':'Finite preference redundancy, not physical ranking or metric novelty validation'}


def compute(rows,blocks,bootstrap=5000,allow_partial=False):
    keys=[(r['macroblock'],r['condition_id'],r['namespace']) for r in rows]
    if len(keys)!=len(set(keys)):raise ValueError('Duplicate matched observation')
    index={(r['macroblock'],r['payload_size'],r['users_total'],r['namespace']):r for r in rows}
    conditions=sorted({r['condition_id'] for r in rows})
    eligible={};gates=[]
    for q in conditions:
        for n in NAMESPACES:
            cell=[r for r in rows if r['condition_id']==q and r['namespace']==n]
            reasons=[]
            if len(cell)!=len(blocks):reasons.append('Missing block observation')
            if any(r['failures'] for r in cell):reasons.append('Measured application failures in this cell; all repetitions excluded from clean comparison')
            if any(r['successes']==0 for r in cell):reasons.append('No validated completions')
            eligible[(q,n)]=not reasons
            gates.append({'condition_id':q,'namespace':n,'observed_runs':len(cell),'failures':sum(r['failures'] for r in cell),
                          'attempts':sum(r['attempts'] for r in cell),'eligible_clean':not reasons,'reasons':reasons,
                          'status':'provisional_partial' if allow_partial else 'main'})
    raw=[];paired=[];contrasts=[];descriptive=[];scores=[]
    for q in conditions:
        for n in NAMESPACES:
            cell=[r for r in rows if r['condition_id']==q and r['namespace']==n]
            for key,label in METRICS.items():
                vals=[r[key] for r in cell if r.get(key) is not None and math.isfinite(r[key]) and r[key]>=0]
                log_summary=summarize_logs([math.log(v) for v in vals],bootstrap) if vals and all(v>0 for v in vals) else {'n_blocks':0,'geometric_effect':None,'t95':None,'bootstrap95':None,'reason':'Undefined for zero/nonpositive observations; zeros retained in raw arithmetic summary'}
                raw.append({'condition_id':q,'namespace':n,'metric':label,'conditional_on_success':key!='successful_rps_with_drain',
                            'n_defined_runs':len(vals),'run_values':[r.get(key) for r in cell],
                            'arithmetic_mean':float(np.mean(vals)) if vals else None,
                            'run_geometric_summary':log_summary,
                            'clean_cell':eligible[(q,n)],'note':'Run-level p99 summaries; not pooled-request p99'})
                logs=[];missing=[]
                for r in cell:
                    ref=next((x for x in rows if x['condition_id']==q and x['namespace']=='ns-json' and x['macroblock']==r['macroblock']),None)
                    if not eligible[(q,n)] or not eligible[(q,'ns-json')] or not ref or not positive(r.get(key)) or not positive(ref.get(key)):
                        missing.append(r['macroblock']);continue
                    logs.append(math.log(r[key]/ref[key]))
                paired.append({'condition_id':q,'namespace':n,'metric':label,'missing_or_ineligible_blocks':missing,
                               **summarize_logs(logs,bootstrap)})
    for n in NAMESPACES:
        for key,label in METRICS.items():
            vals={'payload':[],'concurrency':[],'interaction':[]};undefined=[]
            for b in blocks:
                quartet=[index.get((b,p,u,n)) for p,u in ((10,50),(10,100),(50,50),(50,100))]
                if any(r is None or not positive(r.get(key)) or not eligible[(r['condition_id'],n)] for r in quartet):
                    undefined.append(b);continue
                a,c,d,e=[math.log(r[key]) for r in quartet]
                vals['payload'].append(((d-a)+(e-c))/2)
                vals['concurrency'].append(((c-a)+(e-d))/2)
                vals['interaction'].append((e-c)-(d-a))
            for contrast,x in vals.items():
                contrasts.append({'namespace':n,'metric':label,'contrast':contrast,'undefined_blocks':undefined,**summarize_logs(x,bootstrap)})
    # Endpoint sensitivity: fixed eligibility, matched denominators, fixed primary +12.
    tails=[]
    for tail in ENDPOINTS:
        for q in conditions:
            for n in NAMESPACES:
                logs=[]
                for b in blocks:
                    g={r['namespace']:r for r in rows if r['macroblock']==b and r['condition_id']==q}
                    if n not in g or 'ns-json' not in g or not eligible[(q,n)] or not eligible[(q,'ns-json')]:continue
                    a=g[n]['energy_per_success_by_tail'].get(str(tail));ref=g['ns-json']['energy_per_success_by_tail'].get(str(tail))
                    if positive(a) and positive(ref):logs.append(math.log(a/ref))
                tails.append({'tail_seconds':tail,'primary':tail==12,'condition_id':q,'namespace':n,**summarize_logs(logs,bootstrap)})
    # Optional composite scenarios, per block before geometric aggregation. No score tuning.
    for q in conditions:
        for n in NAMESPACES:
            for tail in ENDPOINTS:
                for weight in WEIGHTS:
                    logs=[];products=[];meanproducts=[]
                    for b in blocks:
                        g={r['namespace']:r for r in rows if r['macroblock']==b and r['condition_id']==q}
                        if n not in g or 'ns-json' not in g or not eligible[(q,n)] or not eligible[(q,'ns-json')]:continue
                        r,ref=g[n],g['ns-json'];e=r['energy_per_success_by_tail'].get(str(tail));e0=ref['energy_per_success_by_tail'].get(str(tail))
                        l=r['p99_success_ms_nearest_rank'];l0=ref['p99_success_ms_nearest_rank'];mean=r['mean_success_ms'];mean0=ref['mean_success_ms']
                        if not all(positive(v) for v in (e,e0,l,l0,mean,mean0)):continue
                        le,ll,lm=math.log(e/e0),math.log(l/l0),math.log(mean/mean0)
                        logs.append(weight*le+(1-weight)*ll);products.append(le+ll);meanproducts.append(le+lm)
                    scores.append({'condition_id':q,'namespace':n,'tail_seconds':tail,'weight_energy':weight,
                                   'score':summarize_logs(logs,bootstrap),'tail_product':summarize_logs(products,bootstrap),
                                   'mean_product':summarize_logs(meanproducts,bootstrap)})
    # Point-estimate Pareto sets for positive, clean component geometric means, separately by workload.
    pareto=[]
    for q in conditions:
        points={}
        for n in NAMESPACES:
            cell=[r for r in rows if r['condition_id']==q and r['namespace']==n]
            if eligible[(q,n)] and all(positive(r.get(k)) for r in cell for k in ('estimated_joules_per_success_primary_12s','p99_success_ms_nearest_rank')):
                points[n]=[math.exp(np.mean([math.log(r[k]) for r in cell])) for k in ('estimated_joules_per_success_primary_12s','p99_success_ms_nearest_rank')]
        dominated={n:[m for m,y in points.items() if m!=n and all(a<=b for a,b in zip(y,x)) and any(a<b for a,b in zip(y,x))] for n,x in points.items()}
        pareto.append({'condition_id':q,'component_geometric_means':points,'point_estimate_frontier':[n for n in points if not dominated[n]],'dominated_by':dominated,
                       'interpretation':'Descriptive point-estimate dominance; no distributional or physical-energy dominance claim'})
    dependence=[]
    for q in conditions:
        group=[r for r in rows if r['condition_id']==q]
        checks={}
        for label,x,y in [('cgroup_per_completion','cpu_seconds_per_success','estimated_joules_per_success_primary_12s'),
                          ('cgroup_totals','cpu_seconds_gateway_worker','estimated_dynamic_joules_primary_12s'),
                          ('BPF_per_completion_matched_scrapes','bpf_cpu_seconds_per_success_12s','estimated_joules_per_success_primary_12s'),
                          ('BPF_totals_matched_scrapes','bpf_cpu_seconds_12s','estimated_dynamic_joules_primary_12s')]:
            checks[label]={'raw':association(group,x,y),'centered_within_protocol':association(group,x,y,True)}
        dependence.append({'condition_id':q,'checks':checks,'cpu_only_output_agreement':cpu_predictor(group)})
    return {'schema':REVISION,'partial':allow_partial,'accepted_macroblocks':blocks,'n_runs':len(rows),
            'eligibility':gates,'raw_summaries':raw,'paired_JSON_ratios':paired,'workload_contrasts':contrasts,
            'endpoint_sensitivity':tails,'optional_preference_scenarios':scores,'point_estimate_Pareto':pareto,
            'CPU_energy_dependence':dependence,'ranking_substitution':substitute_rankings(rows,eligible),
            'limitations':['Model estimates not physical measurements; exact active weights unresolved',
                           'Shared-VM blocks, not independent hardware; no production SLA',
                           'Closed-loop and client sensitivity; padded windows include background',
                           'Pointwise intervals, not simultaneous hypothesis tests; run-level p99, not pooled p99',
                           'CPU/output agreement is not physical calibration; weights are preference scenarios']}


def load_campaign(root,manifest,partial=False):
    from validate_run import audit_run
    root=Path(root);m=validate_manifest(manifest);state=read_json(root/'campaign.json');freeze=read_json(root/'freeze.json')
    if state['freeze_sha256']!=sha(root/'freeze.json') or state['manifest_sha256']!=sha(manifest): raise ValueError('Campaign/freeze hash mismatch')
    if freeze['source_hashes']!=source_hashes(): raise ValueError('Use the frozen package to analyze, or document the changed analysis as an amendment')
    accepted=[a for a in state['attempts'] if a['status']=='accepted']
    blocks=sorted(a['macroblock'] for a in accepted)
    if len(blocks)!=len(set(blocks)):raise ValueError('Duplicate accepted block')
    if not partial and blocks!=list(range(1,9)):raise ValueError('Eight accepted blocks required. --partial is explicitly exploratory.')
    rows=[];history=[]
    for a in state['attempts']:
        history.append({k:a.get(k) for k in ('macroblock','attempt','status','reason','started','ended')})
    for a in accepted:
        planned=[r for r in m['runs'] if r['macroblock']==a['macroblock']]
        if [x['sequence'] for x in a['runs']]!=[r['sequence'] for r in planned]:raise ValueError('Accepted block has incomplete/changed order')
        for rr,plan in zip(a['runs'],planned):
            folder=(root/rr['relative_path']).resolve()
            if not folder.is_relative_to(root.resolve()):raise ValueError('Run path escapes campaign')
            # Full independent re-audit, including all raw framed/CSV rows, not only summary trust.
            outcome=audit_run(folder,plan)
            if not outcome['technical_valid']:raise ValueError('Accepted run failed re-audit: '+str(folder)+' '+str(outcome['technical_reasons']))
            saved=read_json(folder/'main-outcomes.json')
            if saved!=outcome:raise ValueError('Saved audit differs from current raw evidence: '+str(folder))
            if read_json(folder/'planned-run.json')!=plan:raise ValueError('Planned identity changed')
            r={**outcome,**plan,'bpf_cpu_seconds_12s':outcome['bpf_cpu_seconds_by_tail']['12']}
            r['input_path']=str(folder);rows.append(r)
    if not rows:raise ValueError('No accepted runs to analyze')
    return rows,blocks,history


def figures(rows,result,out):
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    colors=dict(zip(NAMESPACES,['#2764a5','#cc7733','#258348','#9b57a5']))
    conditions=sorted({r['condition_id'] for r in rows})
    for kind in ('tradeoffs','cpu_energy'):
        fig,axes=plt.subplots(2,2,figsize=(11,8),layout='constrained')
        for ax,q in zip(axes.flat,conditions):
            for n in NAMESPACES:
                cell=[r for r in rows if r['condition_id']==q and r['namespace']==n]
                gate=next(g for g in result['eligibility'] if g['condition_id']==q and g['namespace']==n)
                points=[r for r in cell if positive(r['estimated_joules_per_success_primary_12s']) and positive(r['p99_success_ms_nearest_rank']) and positive(r['cpu_seconds_per_success'])]
                if not points:continue
                xkey='p99_success_ms_nearest_rank' if kind=='tradeoffs' else 'cpu_seconds_per_success'
                scale=1 if kind=='tradeoffs' else 1000
                ax.scatter([r[xkey]*scale for r in points],[r['estimated_joules_per_success_primary_12s']*1000 for r in points],
                           color=colors[n],label=n.removeprefix('ns-')+(' [failures]' if not gate['eligible_clean'] else ''),marker='o' if gate['eligible_clean'] else 'x',alpha=.8)
            ax.set_title(q);ax.set_xlabel('Successful-request p99 (ms)' if kind=='tradeoffs' else 'Cgroup CPU / completion (ms)')
            ax.set_ylabel('Model-estimated energy / completion (mJ)');ax.grid(alpha=.2)
        handles,labels=axes.flat[0].get_legend_handles_labels()
        fig.legend(handles,labels,loc='outside lower center',ncol=4)
        fig.suptitle(('PARTIAL EXPLORATORY — ' if result['partial'] else '')+'Model-estimated outcomes; no physical energy validation')
        for extension in ('png','svg'):fig.savefig(out/(kind+'.'+extension),dpi=160)
        plt.close(fig)


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('campaign',type=Path);p.add_argument('--output',type=Path,required=True)
    p.add_argument('--manifest',type=Path,default=Path(__file__).parent/'PSI_Main_Study_Order_v0_1.json')
    p.add_argument('--partial',action='store_true',help='Exploratory partial report; never presented as completed main study')
    p.add_argument('--bootstrap',type=int,default=5000)
    a=p.parse_args()
    if a.bootstrap<1000:raise ValueError('At least 1000 whole-block bootstrap draws')
    if a.output.resolve()==a.campaign.resolve() or a.output.resolve().is_relative_to(a.campaign.resolve()):
        raise ValueError('Put analysis in a separate directory from immutable campaign evidence')
    if a.output.exists():raise ValueError('Output already exists; choose a fresh analysis directory')
    rows,blocks,history=load_campaign(a.campaign,a.manifest,a.partial)
    a.output.mkdir(parents=True)
    result=compute(rows,blocks,a.bootstrap,a.partial)
    result['attempt_history']=history;result['analysis_source_hashes']=source_hashes()
    result['campaign_freeze_sha256']=sha(a.campaign/'freeze.json')
    write_json(a.output/'analysis.json',result);write_json(a.output/'run-level.json',rows)
    fields=['macroblock','condition_id','namespace','payload_size','users_total','attempts','successes','failures','failure_rate',
            'estimated_dynamic_joules_primary_12s','estimated_joules_per_success_primary_12s','p99_success_ms_nearest_rank',
            'mean_success_ms','cpu_seconds_gateway_worker','cpu_seconds_per_success','successful_rps_with_drain',
            'successful_requests_per_admission_second','bpf_cpu_seconds_12s','bpf_cpu_seconds_per_success_12s','input_path']
    with (a.output/'run-level.csv').open('w',newline='') as f:
        w=csv.DictWriter(f,fieldnames=fields,extrasaction='ignore');w.writeheader();w.writerows(rows)
    figures(rows,result,a.output)
    bad=sum(not g['eligible_clean'] for g in result['eligibility'])
    note=f'''# {'Partial exploratory' if a.partial else 'Main-study'} analysis\n\n{len(blocks)} accepted macroblocks; {len(rows)} audited runs. {bad} protocol/workload cells ineligible for clean comparisons.\n\nEnergy is a fixed +12-second padded-batch model estimate, not physically validated energy. Raw immediate values remain in collection evidence. Failures are retained; an affected cell's repetitions are not selectively averaged into clean comparisons.\n\nSee analysis.json for pointwise t/whole-block bootstrap intervals, paired JSON ratios, payload/concurrency/interaction contrasts, endpoint sensitivity, optional preference scenarios, Pareto points, CPU dependence, and ranking substitutions. run-level.csv is run-level evidence, not pooled request statistics. No overall winner or physical energy saving is inferred automatically.\n\nAll raw exports and requests were re-audited. Technical replacement attempts are listed in analysis.json; excluded attempts remain in the campaign directory. Client calibration remains separate from main repetitions.\n'''
    (a.output/'REPORT.md').write_text(note)
    print(note)

if __name__=='__main__':main()
