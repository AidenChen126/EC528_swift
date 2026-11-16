#!/usr/bin/env python3

"""Validate atomic saves during five sequential sustained local SAIO cases.



Defaults: smoke/small/medium/large/mixed, 300 seconds per traffic phase,

one round of exp2-4 and exp6-12 per case (50 required executions).

Seeding, final verification and cleanup add wall time. No live ring changes.

The workload is copied and instrumented with monotonic request timestamps.

Unfamiliar instrumentation formats are refused, rather than guessed.



Each case uses its own private scratch directory on mounted /mnt/sdb1,

exported as TMPDIR/TEMP/TMP. Helpers must honor these; this is not a sandbox.

Only this run's private scratch directories are removed.



Verdicts separate atomic checks, background traffic and cleanup. Object errors

are recorded; causal attribution is NOT_ESTABLISHED. Overlap is at experiment

PROCESS interval level, not failure-hook level. No proof of running server

revision, power-loss survival, or industrial/multi-host capacity is made.



Exit: 0 clean; 1 experiment failure; 2 incomplete/runner error;

3 atomic checks pass with traffic/cleanup warnings; 77 preflight refused.

"""

import argparse

import ast

import csv

import hashlib

import json

import os

from pathlib import Path

import shutil

import signal

import subprocess

import sys

import time

import uuid



NAMES={2:'exp2_interrupted_save.py',3:'exp3_save_failure_cleanup.py',

       4:'exp4_save_durability_order.py',6:'exp6_composite_partial_dump.py',

       7:'exp7_composite_interrupted_save.py',8:'exp8_composite_save_failure_cleanup.py',

       9:'exp9_composite_save_durability_order.py',10:'exp10_composite_save_edge_cases.py',

       11:'exp11_composite_save_file_mode.py',12:'exp12_topology_change_atomic_save.py'}

CASES=('smoke','small','medium','large','mixed')



def stop(proc):

    if proc is None or proc.poll() is not None:return

    try:os.killpg(proc.pid,signal.SIGTERM)

    except ProcessLookupError:return

    try:proc.wait(timeout=10)

    except subprocess.TimeoutExpired:

        try:os.killpg(proc.pid,signal.SIGKILL)

        except ProcessLookupError:pass

        proc.wait()



def rows(path):

    if path is None or not path.is_file():return []

    result=[]

    with path.open(newline='') as f:

        for r in csv.DictReader(f):

            try:

                r['start']=float(r['start_monotonic']);r['end']=float(r['end_monotonic'])

                r['successful']=r['ok']=='True'

            except (KeyError,ValueError,TypeError):continue

            result.append(r)

    return result



def main():

    ap=argparse.ArgumentParser(description=__doc__,formatter_class=argparse.RawDescriptionHelpFormatter)

    ap.add_argument('--seconds',type=int,default=300,help='sustained traffic seconds per case, excluding seeding/cleanup')

    ap.add_argument('--case',choices=(*CASES,'all'),default='all')

    ap.add_argument('--workers',type=int,help='override workers for every selected case; default uses CASES')

    ap.add_argument('--experiment-timeout',type=int,default=180)

    ap.add_argument('--ready-timeout',type=int,default=600,help='seeding/readiness timeout per case')

    ap.add_argument('--finish-timeout',type=int,default=600,help='extra allowance for final verification/cleanup')

    ap.add_argument('--scratch-parent',default='/mnt/sdb1/atomic-save-scratch')

    ap.add_argument('--output',default='experiment-results')

    a=ap.parse_args()

    if any(v is not None and v<=0 for v in (a.seconds,a.workers,a.experiment_timeout,a.ready_timeout,a.finish_timeout)):ap.error('positive settings required')

    if a.workers and a.workers>32:ap.error('maximum 32 workers')

    root=Path(__file__).resolve().parents[1]

    original=root/'experiments/saio_scale_tests_expanded.py'

    sources=[original,*[root/'experiments'/n for n in NAMES.values()],root/'swift/common/ring/builder.py',root/'swift/common/ring/composite_builder.py']

    parent=Path(a.scratch_parent)

    def refuse(s):print('PREFLIGHT REFUSED:',s);return 77

    if not sys.platform.startswith('linux'):return refuse('Linux required')

    if not os.path.ismount('/mnt/sdb1'):return refuse('/mnt/sdb1 must already be mounted; do not reset storage')

    if parent.is_symlink():return refuse('scratch parent must not be a symlink')

    parent=parent.resolve()

    if not parent.is_dir() or not os.access(parent,os.W_OK|os.X_OK):return refuse('scratch parent missing/not writable; use launcher')

    if parent.stat().st_dev!=Path('/mnt/sdb1').stat().st_dev:return refuse('scratch must be on SAIO storage filesystem')

    missing=[str(p) for p in sources if not p.is_file()]

    if missing:return refuse('missing: '+', '.join(missing))

    source=original.read_text()

    if 'ATOMIC_LOAD_READY' not in source or "'--sustain'" not in source:return refuse('apply the supplied sustained-mode updater first')

    header="writer.writerow(['operation','object','seconds','ok','bytes','error'])"

    row="writer.writerow([kind,i,elapsed,not bool(error),nbytes,error])"

    if source.count(header)!=1 or source.count(row)!=1:return refuse('unexpected workload CSV source; review instrumentation')

    instrumented=source.replace(header,"writer.writerow(['operation','object','seconds','ok','bytes','error','start_monotonic','end_monotonic'])").replace(row,"writer.writerow([kind,i,elapsed,not bool(error),nbytes,error,started,time.monotonic()])")

    compile(instrumented,str(original),'exec')

    tree=ast.parse(source);assignment=next(n for n in tree.body if isinstance(n,ast.Assign) and any(isinstance(t,ast.Name) and t.id=='CASES' for t in n.targets))

    namespace={}

    for n in tree.body:

        if isinstance(n,ast.Assign) and any(isinstance(t,ast.Name) and t.id in ('K','M','CASES') for t in n.targets):

            exec(compile(ast.Module(body=[n],type_ignores=[]),str(original),'exec'),{'__builtins__':{}},namespace)

    definitions=namespace['CASES']

    selected=list(CASES) if a.case=='all' else [a.case]

    rid=time.strftime('%Y%m%d-%H%M%S')+'-'+uuid.uuid4().hex[:8]

    dest=(Path(a.output)/('atomic-under-load-'+rid)).resolve();dest.mkdir(parents=True)

    report={'schema_version':3,'settings':vars(a),'run_directory':str(dest),'cases':[],

            'atomic_save':{'expected_executions':len(selected)*len(NAMES)},

            'limitations':['Experiment-process overlap, not failure-hook overlap','Single host; not power-loss or capacity evidence','TMPDIR is not a sandbox','Workload traffic does not itself invoke builder saves']}

    report['source_sha256']={str(p.relative_to(root)):hashlib.sha256(p.read_bytes()).hexdigest() for p in sources}

    report['runner_sha256']=hashlib.sha256(Path(__file__).read_bytes()).hexdigest()

    try:

        report['revision']={k:subprocess.check_output(['git','-C',str(root),*v],text=True,timeout=15).strip() for k,v in {'commit':['rev-parse','HEAD'],'branch':['branch','--show-current'],'status':['status','--porcelain']}.items()}

    except Exception as e:report['revision_error']=str(e)

    def save():

        tmp=dest/'summary.json.tmp';tmp.write_text(json.dumps(report,indent=2)+'\n');tmp.replace(dest/'summary.json')

    save();interrupted=False

    for case in selected:

        count,size,workers,containers=definitions[case];workers=a.workers or workers

        sizes=[64*1024 if i%10<7 else 1024**2 if i%10<9 else 8*1024**2 for i in range(count)] if case=='mixed' else [size]*count

        histogram={str(s):sizes.count(s) for s in sorted(set(sizes))}

        cd=dest/case;cd.mkdir();scratch=parent/('atomic-under-load-'+rid+'-'+case)

        entry={'case':case,'count':count,'workers':workers,'containers':containers,'object_size_bytes_by_count':histogram,

               'logical_seed_bytes':sum(sizes),'traffic_seconds':a.seconds,'experiments':[],

               'operation_policy':'70% GET / 20% overwrite / 5% HEAD / 5% DELETE for occupied slots; PUT for empty slots',

               'access_policy':'80% selection branch targets first 20% of slots',

               'background_traffic':{'status':'INCOMPLETE','attribution':'NOT_ESTABLISHED'},'cleanup':{'status':'WARNING','errors':[]}}

        report['cases'].append(entry);save()

        print('\nCASE %s: %d slots, sizes %s, %d workers, %ds sustained traffic; 10 checks' %(case,count,histogram,workers,a.seconds),flush=True)

        traffic=None;experiment=None;csvpath=None;normal=False;log=None

        try:

            scratch.mkdir(mode=0o700)

            env=os.environ.copy();env.update(TMPDIR=str(scratch),TEMP=str(scratch),TMP=str(scratch),PYTHONUNBUFFERED='1')

            env['PYTHONPATH']=str(root)+os.pathsep+env.get('PYTHONPATH','')

            check=subprocess.check_output([sys.executable,'-c','import tempfile; print(tempfile.gettempdir())'],env=env,text=True,timeout=15).strip()

            if Path(check).resolve()!=scratch:raise RuntimeError('temporary directory mismatch')

            ts=cd/'traffic_timestamped.py';ts.write_text(instrumented)

            cmd=[sys.executable,str(ts),'--case',case,'--sustain','--seconds',str(a.seconds),'--workers',str(workers),'--output',str(cd/'traffic')]

            entry['traffic_command']=cmd;save()

            log=(cd/'traffic.log').open('w')

            traffic=subprocess.Popen(cmd,cwd=root,env=env,stdout=log,stderr=subprocess.STDOUT,start_new_session=True)

            limit=time.monotonic()+a.ready_timeout;ready=None

            while True:

                for line in (cd/'traffic.log').read_text().splitlines():

                    if line.startswith('ATOMIC_LOAD_READY '):ready=float(line.split()[1])

                matches=list((cd/'traffic').glob('saio-expanded-*/'+case+'-requests.csv'))

                csvpath=matches[0] if len(matches)==1 else None

                if ready is not None and any(r['successful'] and r['operation']=='GET' and r['start']>=ready for r in rows(csvpath)):break

                if traffic.poll() is not None:raise RuntimeError('traffic exited before sustained readiness; inspect traffic.log')

                if time.monotonic()>=limit:raise RuntimeError('seeding/readiness timeout')

                time.sleep(.25)

            entry['ready_monotonic']=ready;save()

            for number,name in NAMES.items():

                remaining=ready+a.seconds-time.monotonic()

                if traffic.poll() is not None or remaining<=0:raise RuntimeError('sustained phase ended before all checks')

                label='exp%d'%number

                cmd=[sys.executable,str(root/'experiments'/name),'--swift-src',str(root),'--timeout',str(a.experiment_timeout),'--results-json',str(cd/(label+'.json'))]

                item={'name':label,'command':cmd,'start_monotonic':time.monotonic(),'status':'RUNNING'}

                entry['experiments'].append(item);save()

                print('%s: %s running; %.0fs traffic budget left'%(case,label,remaining),flush=True)

                with (cd/(label+'.log')).open('w') as elog:

                    experiment=subprocess.Popen(cmd,cwd=root,env=env,stdout=elog,stderr=subprocess.STDOUT,start_new_session=True)

                    try:item['exit_code']=experiment.wait(timeout=min(a.experiment_timeout+30,max(.01,remaining)))

                    except subprocess.TimeoutExpired:

                        stop(experiment);item.update(exit_code=experiment.returncode,timeout=True)

                item['end_monotonic']=time.monotonic();item['traffic_alive_at_end']=traffic.poll() is None

                during=[r for r in rows(csvpath) if r['start']>=item['start_monotonic'] and r['end']<=min(item['end_monotonic'],ready+a.seconds)]

                successes=sum(r['successful'] for r in during)

                item.update(successful_requests_fully_during_experiment=successes,failed_requests_fully_during_experiment=len(during)-successes,

                            overlap_requests_per_second=successes/max(.000001,item['end_monotonic']-item['start_monotonic']))

                code=item['exit_code']

                item['test_execution']='INCOMPLETE' if item.get('timeout') or code==77 or code<0 else 'PASS' if code==0 else 'FAIL'

                item['overlap_verified']=successes>0 and item['traffic_alive_at_end'] and item['end_monotonic']<=ready+a.seconds

                item['status']='FAIL' if item['test_execution']=='FAIL' else 'PASS' if item['test_execution']=='PASS' and item['overlap_verified'] else 'INCOMPLETE'

                save();print('%s %s: %s; %d successful overlapping requests'%(case,label,item['status'],successes),flush=True)

            print(case+': waiting for traffic completion, verification and cleanup',flush=True)

            traffic.wait(timeout=max(0,ready+a.seconds-time.monotonic())+a.finish_timeout);normal=True

        except KeyboardInterrupt:

            entry['runner_error']='Interrupted; traffic cleanup may be incomplete';interrupted=True

        except Exception as e:entry['runner_error']=type(e).__name__+': '+str(e)

        finally:

            stop(experiment);stop(traffic)

            if log:log.close()

            entry['traffic_exit_code']=traffic.returncode if traffic else None

            summaries=list((cd/'traffic').glob('saio-expanded-*/summary.json'));tr={}

            if len(summaries)==1:

                try:tr=json.loads(summaries[0].read_text());entry['traffic_result']=tr

                except Exception as e:entry['traffic_report_error']=str(e)

            tc=tr.get('cases',[]);final=tc[0] if len(tc)==1 else {}

            reached=normal and isinstance(final.get('worker_iterations'),list) and final.get('status') in ('PASS','FAIL')

            failures=[r for r in rows(csvpath) if not r['successful']]

            annotations=[dict(r,experiments_active_at_completion=[e['name'] for e in entry['experiments'] if e['start_monotonic']<=r['end']<=e.get('end_monotonic',float('inf'))],attribution='NOT_ESTABLISHED') for r in failures]

            (cd/'object_errors.json').write_text(json.dumps(annotations,indent=2)+'\n')

            entry['background_traffic'].update(status='PASS' if reached and entry['traffic_exit_code']==0 and tr.get('status')=='PASS' else 'COMPLETED_WITH_ERRORS' if reached else 'INCOMPLETE',request_failures=len(failures),configured_duration_completion_evidence=bool(reached))

            cleanup=list(final.get('cleanup_errors',[]))

            if scratch.exists():

                try:shutil.rmtree(scratch)

                except OSError as e:cleanup.append(str(e))

            if not reached:cleanup.append('Traffic verification/cleanup not fully verified; inspect traffic.log and container_names')

            entry['cleanup']={'status':'WARNING' if cleanup else 'PASS','errors':cleanup}

            checks=entry['experiments']

            entry['atomic_save']={'status':'FAIL' if any(e.get('test_execution')=='FAIL' for e in checks) else 'PASS' if len(checks)==len(NAMES) and all(e.get('status')=='PASS' for e in checks) else 'INCOMPLETE','passed_executions':sum(e.get('status')=='PASS' for e in checks),'expected_executions':len(NAMES)}

            save()

            print('%s: atomic=%s, traffic=%s, cleanup=%s'%(case,entry['atomic_save']['status'],entry['background_traffic']['status'],entry['cleanup']['status']),flush=True)

        if interrupted:break

    checks=[e for c in report['cases'] for e in c['experiments']];expected=len(selected)*len(NAMES)

    atomic='FAIL' if any(e.get('test_execution')=='FAIL' for e in checks) else 'PASS' if len(checks)==expected and all(e.get('status')=='PASS' for e in checks) else 'INCOMPLETE'

    traffic_status='INCOMPLETE' if len(report['cases'])!=len(selected) or any(c['background_traffic']['status']=='INCOMPLETE' for c in report['cases']) else 'PASS' if all(c['background_traffic']['status']=='PASS' for c in report['cases']) else 'COMPLETED_WITH_ERRORS'

    cleanup_status='PASS' if len(report['cases'])==len(selected) and all(c['cleanup']['status']=='PASS' for c in report['cases']) else 'WARNING'

    report['atomic_save'].update(status=atomic,passed_executions=sum(e.get('status')=='PASS' for e in checks),completed_executions=sum('end_monotonic' in e for e in checks))

    report['background_traffic']={'status':traffic_status,'request_failures':sum(c['background_traffic']['request_failures'] for c in report['cases']),'attribution':'NOT_ESTABLISHED'}

    report['cleanup']={'status':cleanup_status}

    rc=1 if atomic=='FAIL' else 2 if atomic!='PASS' or traffic_status=='INCOMPLETE' or any('runner_error' in c for c in report['cases']) else 3 if traffic_status!='PASS' or cleanup_status!='PASS' else 0

    report['exit_code']=rc;save()

    print('\nATOMIC-SAVE CHECKS:',atomic)

    print('  Passed %d/%d required executions with verified overlap.'%(report['atomic_save']['passed_executions'],expected))

    print('BACKGROUND TRAFFIC:',traffic_status,'| Failed object requests:',report['background_traffic']['request_failures'])

    print('CLEANUP:',cleanup_status);print('Exit code:',rc);print('Report:',dest/'summary.json')

    return rc



if __name__=='__main__':sys.exit(main())

