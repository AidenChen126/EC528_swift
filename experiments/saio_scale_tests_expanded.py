#!/usr/bin/env python3

"""Expanded local SAIO scale/integrity suite. No ring/configuration changes.

0=PASS, 1=FAIL/incomplete, 77=preflight refused. Real cluster execution required.

"""

import argparse

from concurrent.futures import ThreadPoolExecutor

import csv

import hashlib

import json

import math

import os

from pathlib import Path

import platform

import random

import shutil

import subprocess

import sys

import threading

import time

import uuid



K=1024

M=K*K

CASES={'smoke':(8,16*K,2,1), 'small':(2000,16*K,8,4),

       'medium':(512,M,8,4), 'large':(32,32*M,4,2),

       'mixed':(256,M,8,4)}





def body_for(name, generation, size):

    seed=hashlib.sha256(('%s/%s'%(name,generation)).encode()).digest()

    block=hashlib.shake_256(seed).digest(min(size,64*K))

    return (block*((size+len(block)-1)//len(block)))[:size]





def main():

    ap=argparse.ArgumentParser(description=__doc__)

    ap.add_argument('--case',choices=list(CASES)+['all'],default='smoke')

    ap.add_argument('--seconds',type=int,default=180,help='mixed workload duration')

    ap.add_argument('--workers',type=int)

    ap.add_argument('--count',type=int,help='object count override; single case only')

    ap.add_argument('--size-mib',type=int,help='object size override; single case only')

    ap.add_argument('--output',default='experiment-results')

    ap.add_argument('--max-errors',type=int,default=20)

    a=ap.parse_args()

    if any(v is not None and v<=0 for v in [a.seconds,a.workers,a.count,a.size_mib,a.max_errors]):

        ap.error('numeric settings must be positive')

    if a.case=='all' and (a.count or a.size_mib):

        ap.error('count/size overrides require one case')

    if a.workers and a.workers>32:

        ap.error('local safety limit: 32 workers')

    if a.size_mib and a.size_mib>128:

        ap.error('local safety limit: 128 MiB per object')

    try:

        from swiftclient.client import Connection, ClientException

    except ImportError:

        print('Install python-swiftclient in your venv');return 77

    auth=os.environ.get('ST_AUTH','http://127.0.0.1:8080/auth/v1.0')

    if auth not in ['http://127.0.0.1:8080/auth/v1.0','http://localhost:8080/auth/v1.0']:

        ap.error('Only local SAIO endpoints are permitted')

    def connection():

        return Connection(authurl=auth,user=os.environ.get('ST_USER','test:tester'),

                          key=os.environ.get('ST_KEY','testing'),auth_version='1',retries=0,timeout=30)

    runid=time.strftime('%Y%m%d-%H%M%S')+'-'+uuid.uuid4().hex[:8]

    dest=Path(a.output).resolve()/('saio-expanded-'+runid);dest.mkdir(parents=True)

    def git(*args):

        try:return subprocess.check_output(['git',*args],text=True,stderr=subprocess.DEVNULL,timeout=10).strip()

        except Exception:return None

    report={'run_id':runid,'settings':vars(a),'python':sys.version,'platform':platform.platform(),

            'client_commit':git('rev-parse','HEAD'),'client_branch':git('branch','--show-current'),

            'client_dirty':git('status','--porcelain'),'cases':[], 'status':'RUNNING',

            'limitations':['Not proof of running server revision','No direct builder-save exercise',

                          'Single host; no power-loss or multi-host validation']}

    def save(): (dest/'summary.json').write_text(json.dumps(report,indent=2)+'\n')

    save()

    admin=connection()

    selected=['small','medium','large','mixed'] if a.case=='all' else [a.case]

    try:

        admin.head_account()

    except Exception as e:

        report.update(status='NOT_RUN',error=str(e));save();admin.close();return 77

    try:

        for case in selected:

            count,size,workers,containers=CASES[case]

            count=a.count or count;size=(a.size_mib*M) if a.size_mib else size

            workers=a.workers or workers

            sizes=[size]*count

            if case=='mixed' and not a.size_mib:

                sizes=[64*K if i%10<7 else M if i%10<9 else 8*M for i in range(count)]

            logical=sum(sizes)

            entry={'case':case,'count':count,'workers':workers,'containers':containers,

                   'logical_bytes':logical,'max_object_bytes':max(sizes),'status':'RUNNING'}

            report['cases'].append(entry);save()

            # Conservative estimate, not a reservation or disk-usage guarantee.

            required=6*logical+2*1024**3

            frees={path:shutil.disk_usage(path).free for path in ['/','/mnt/sdb1']}

            entry.update(required_free_bytes=required,free_bytes=frees)

            if min(frees.values())<required:

                entry.update(status='NOT_RUN',error='Insufficient free space for conservative budget');save();break

            names=['saio-expanded-%s-%s-%d'%(runid,case,i) for i in range(containers)]

            entry['container_names']=names;save()

            lock=threading.Lock();local=threading.local();conns=[];stop=threading.Event()

            stats={};errors=[0];versions=[None]*count;slotlocks=[threading.Lock() for _ in range(count)]

            csvfile=(dest/(case+'-requests.csv')).open('w',newline='')

            writer=csv.writer(csvfile);writer.writerow(['operation','object','seconds','ok','bytes','error'])

            def conn():

                if not hasattr(local,'c'):

                    local.c=connection()

                    with lock:conns.append(local.c)

                return local.c

            def location(i):return names[i%containers], 'object-%07d'%i

            def op(kind,i,generation=None):

                if stop.is_set():return False

                container,name=location(i);started=time.monotonic();error='';nbytes=0

                try:

                    c=conn()

                    if kind in ('PUT','OVERWRITE'):

                        body=body_for(name,generation,sizes[i]);c.put_object(container,name,contents=body);nbytes=len(body)

                    elif kind=='GET':

                        _,body=c.get_object(container,name)

                        if body!=body_for(name,generation,sizes[i]):raise AssertionError('content mismatch')

                        nbytes=len(body)

                    elif kind=='HEAD':

                        headers=c.head_object(container,name)

                        if int(headers['content-length'])!=sizes[i]:raise AssertionError('size mismatch')

                    elif kind=='DELETE':c.delete_object(container,name)

                    elif kind=='ABSENT':

                        try:c.head_object(container,name)

                        except ClientException as e:

                            if e.http_status!=404:raise

                        else:raise AssertionError('expected object 404')

                except Exception as e:error=('%s: %s'%(type(e).__name__,e))[:500]

                elapsed=time.monotonic()-started

                with lock:

                    st=stats.setdefault(kind,{'attempts':0,'failures':0,'bytes':0,'latencies':[]})

                    st['attempts']+=1

                    if error:

                        st['failures']+=1;errors[0]+=1

                        if errors[0]>=a.max_errors:stop.set()

                    else:st['bytes']+=nbytes;st['latencies'].append(elapsed)

                    writer.writerow([kind,i,elapsed,not bool(error),nbytes,error]);csvfile.flush()

                return not error

            def phase(pool,label,fn):

                print(case+': '+label,flush=True)

                list(pool.map(fn,range(count)))

                if stop.is_set():raise RuntimeError('error budget exhausted')

            def seed(i):

                if op('PUT',i,0):versions[i]=0

            def overwrite(i):

                if op('OVERWRITE',i,1):versions[i]=1

            def mixed_worker(worker):

                rng=random.Random(worker);attempts=0

                while time.monotonic()<deadline and not stop.is_set():

                    # 80% of selections target a 20% hot subset.

                    i=rng.randrange(max(1,count//5) if rng.random()<.8 else count)

                    with slotlocks[i]:

                        v=versions[i]

                        if v is None:

                            if op('PUT',i,0):versions[i]=0

                        else:

                            choice=rng.random()

                            if choice<.70:op('GET',i,v)

                            elif choice<.90:

                                if op('OVERWRITE',i,v+1):versions[i]=v+1

                            elif choice<.95:op('HEAD',i)

                            else:

                                if op('DELETE',i):versions[i]=None

                        attempts+=1

                return attempts

            start=time.monotonic()

            try:

                for name in names:admin.put_container(name)

                with ThreadPoolExecutor(max_workers=workers) as pool:

                    phase(pool,'seed dataset',seed)

                    if case=='mixed':

                        print('mixed: sustained traffic (%ds)'%a.seconds,flush=True)

                        deadline=time.monotonic()+a.seconds

                        entry['worker_iterations']=list(pool.map(mixed_worker,range(workers)))

                        phase(pool,'final integrity check',lambda i:op('GET',i,versions[i]) if versions[i] is not None else op('ABSENT',i))

                    else:

                        phase(pool,'verify initial contents',lambda i:op('GET',i,0))

                        phase(pool,'verify metadata',lambda i:op('HEAD',i))

                        phase(pool,'overwrite',overwrite)

                        phase(pool,'verify overwritten contents',lambda i:op('GET',i,1))

                        phase(pool,'delete',lambda i:op('DELETE',i))

                        phase(pool,'verify deletion',lambda i:op('ABSENT',i))

            except Exception as e:entry['error']=str(e)

            finally:

                entry['workload_seconds']=time.monotonic()-start

                for c in conns:c.close()

                csvfile.close()

                entry['cleanup_errors']=[]

                for name in names:

                    try:

                        _,objects=admin.get_container(name,full_listing=True)

                        for obj in objects:admin.delete_object(name,obj['name'])

                        admin.delete_container(name)

                    except Exception as e:entry['cleanup_errors'].append(name+': '+str(e))

                for st in stats.values():

                    values=sorted(st.pop('latencies'))

                    st['successful_latency_seconds']={str(q):values[math.ceil(q*len(values)/100)-1] for q in [50,95,99]} if values else {}

                entry['metrics']=stats

                entry['successful_requests_per_second']=sum(v['attempts']-v['failures'] for v in stats.values())/entry['workload_seconds']

                entry['status']='PASS' if stats and errors[0]==0 and not entry['cleanup_errors'] and 'error' not in entry else 'FAIL'

                save();print(case+': '+entry['status'],flush=True)

                if stop.is_set():break

    except Exception as e:report['error']=str(e)

    finally:

        admin.close()

        report['status']='PASS' if len(report['cases'])==len(selected) and all(c['status']=='PASS' for c in report['cases']) and 'error' not in report else 'FAIL_OR_INCOMPLETE'

        save();print('RESULT:',report['status']);print('Reports:',dest)

    return 0 if report['status']=='PASS' else 1



if __name__=='__main__':sys.exit(main())

