#!/usr/bin/env python3

"""exp12: real topology changes followed by atomic-save failure checks.



Run first on its own:

    python experiments/exp12_topology_change_atomic_save.py --swift-src "$PWD" --timeout 180 --results-json exp12.json



Tests both individual and composite builder saves, for each of:

  (a) adding a device; (b) setting an existing device's weight to zero.

Real rebalance and compose calls must produce changed partition assignments.

The failure matrix then tests successful save, partial serialization, file-fsync

error, replacement error, SIGKILL before rename and SIGKILL after rename.

Every subcase has its own freshly constructed fixture (24 scenarios total).



The composite uses immutable old/new component-builder snapshots with stable

builder IDs. This keeps both metadata versions independently reconstructible.

It does NOT claim a transaction across component builders, composite metadata,

and runtime ring files. ring.gz save/load below is a fixture consistency check,

not failure injection into RingData.save or a live-ring deployment test.



All files are created under tempfile.TemporaryDirectory (honors TMPDIR).

Nothing is read from or written into /etc/swift. Device addresses are synthetic.

Default part power 10 is a correctness fixture, not industrial-scale evidence.

Increase via EC528_TOPOLOGY_PART_POWER, allowed range 8..14.

No power-loss simulation; kill tests leave the OS running.



Supports the integrated runner CLI: --swift-src, --timeout, --results-json.

0 PASS; 1 assertions/execution failure; 77 prerequisite/timeout incomplete.

The supervisor has a hard timeout and terminates its worker process group.

"""

import argparse

import contextlib

import importlib

import inspect

import json

import multiprocessing

import os

from pathlib import Path

import pickle

import signal

import subprocess

import sys

import tempfile

import time

import traceback

from unittest import mock





def require(condition,message):

    if not condition:raise AssertionError(message)





def table(ring):

    # Compare physical device identities, not composite-local integer offsets.

    result=[]

    for row in ring._replica2part2dev_id:

        result.append(tuple((ring.devs[i]['region'],ring.devs[i]['zone'],

                             ring.devs[i]['ip'],ring.devs[i]['port'],

                             ring.devs[i]['device']) for i in row))

    return tuple(result)





def changed_slots(old,new):

    require(len(old)==len(new),'replica count changed unexpectedly')

    return sum(a!=b for x,y in zip(old,new) for a,b in zip(x,y))





def new_builder(module,region,power):

    builder=module.RingBuilder(power,2,0)

    for i in range(4):builder.add_dev(device(region,i))

    builder.rebalance(seed=100+region);builder.validate()

    return builder





def device(region,i):

    return {'id':i,'region':region,'zone':i+1,'ip':'127.%d.0.%d'%(region,i+1),

            'port':6200,'replication_ip':'127.%d.0.%d'%(region,i+1),

            'replication_port':6200,'device':'scratch-%d-%d'%(region,i),

            'weight':100.0,'meta':'exp12 synthetic device; not deployed'}





def modify(builder,change,region):

    if change=='add_device':builder.add_dev(device(region,len(builder.devs)))

    else:builder.set_dev_weight(0,0.0)

    # Zero-hour movement restriction is deliberate for the isolated fixture.

    builder.rebalance(seed=200+region);builder.validate()





def fixture(directory,kind,change,power,bm,cm,RingData):

    target=directory/('current.builder' if kind=='builder' else 'current.composite')

    if kind=='builder':

        candidate=new_builder(bm,1,power);candidate.save(str(target))

        old_ring=candidate.get_ring();old=table(old_ring);modify(candidate,change,1)

        new_ring=candidate.get_ring();new=table(new_ring)

        def load():

            b=bm.RingBuilder.load(str(target));b.validate();return table(b.get_ring())

    else:

        paths=[]

        for region in (1,2):

            b=new_builder(bm,region,power)

            path=directory/('old-region%d.builder'%region);b.save(str(path));paths.append(str(path))

        base=cm.CompositeRingBuilder(paths);old_ring=base.compose();old=table(old_ring);base.save(str(target))

        nextpaths=[]

        for region,path in enumerate(paths,1):

            b=bm.RingBuilder.load(path)

            if region==1:modify(b,change,region)

            newer=directory/('new-region%d.builder'%region);b.save(str(newer));nextpaths.append(str(newer))

        candidate=cm.CompositeRingBuilder.load(str(target))

        new_ring=candidate.compose(nextpaths,require_modified=True);new=table(new_ring)

        def load():

            c=cm.CompositeRingBuilder.load(str(target));return table(c.compose())

    moved=changed_slots(old,new)

    require(moved>0,'fixture changed metadata but no partition assignments')

    # Generate/load both runtime-ring snapshots to validate complete mappings.

    for name,mapping,ring in [('old',old,old_ring),('new',new,new_ring)]:

        if ring is not None:

            path=directory/(name+'.ring.gz');ring.save(str(path))

            require(table(RingData.load(str(path)))==mapping,'runtime ring round-trip mismatch')

    require(load()==old,'baseline does not reconstruct old placement')

    return target,candidate,old,new,load,moved





def kill_save(candidate,target,stage,ready):

    real_rename=os.rename

    def gated(old,new):

        if stage=='kill_before_rename':

            ready.set()

            while True:time.sleep(1)

        value=real_rename(old,new)

        ready.set()

        while True:time.sleep(1)

        return value

    with mock.patch.object(os,'rename',gated):candidate.save(str(target))





def one_case(directory,kind,change,stage,power,bm,cm,RingData):

    target,candidate,old,new,load,moved=fixture(directory,kind,change,power,bm,cm,RingData)

    before=target.read_bytes();beforefiles=set(directory.iterdir())

    if stage=='success':

        candidate.save(str(target));require(load()==new,'successful save lost changed placement')

    elif stage.startswith('kill_'):

        ctx=multiprocessing.get_context('fork');ready=ctx.Event()

        child=ctx.Process(target=kill_save,args=(candidate,target,stage,ready));child.start()

        try:

            end=time.monotonic()+20

            while not ready.wait(.05):

                require(child.is_alive(),'writer exited before reaching rename hook')

                require(time.monotonic()<end,'writer did not reach rename hook')

            child.kill();child.join(10)

            require(child.exitcode==-signal.SIGKILL,'writer was not killed as intended')

            expected=old if stage=='kill_before_rename' else new

            require(load()==expected,'kill exposed wrong/incomplete placement')

            if stage=='kill_before_rename':require(target.read_bytes()==before,'pre-rename kill altered baseline bytes')

        finally:

            if child.is_alive():child.kill();child.join(10)

    else:

        def fail(*args,**kwargs):raise OSError('exp12 injected '+stage)

        if stage=='partial_serialization':

            if kind=='builder':

                def partial(obj,handle,*args,**kwargs):

                    payload=pickle.dumps(obj,*args,**kwargs);handle.write(payload[:len(payload)//2]);handle.flush();fail()

                patch=mock.patch.object(bm.pickle,'dump',partial)

            else:

                def partial(obj,handle,*args,**kwargs):

                    payload=json.dumps(obj,*args,**kwargs);handle.write(payload[:len(payload)//2]);handle.flush();fail()

                patch=mock.patch.object(cm.json,'dump',partial)

        elif stage=='file_fsync_error':patch=mock.patch.object(os,'fsync',fail)

        else:patch=mock.patch.object(os,'rename',fail)

        with patch:

            try:candidate.save(str(target))

            except OSError as exc:require(str(exc)=='exp12 injected '+stage,'wrong error propagated: '+str(exc))

            else:raise AssertionError('injected failure was not raised')

        require(target.read_bytes()==before,'handled failure altered baseline bytes')

        require(load()==old,'handled failure did not preserve old placement')

        require(set(directory.iterdir())==beforefiles,'handled failure left unexpected files')

    # Recovery after every scenario, including killed writers with stale temps.

    candidate.save(str(target));require(load()==new,'retry did not install complete changed placement')

    return {'changed_replica_partition_assignments':moved,'partitions':2**power}





def worker(args):

    sys.path.insert(0,str(Path(args.swift_src).resolve()))

    try:

        bm=importlib.import_module('swift.common.ring.builder')

        cm=importlib.import_module('swift.common.ring.composite_builder')

        from swift.common.ring.ring import RingData

        root=Path(args.swift_src).resolve()

        for module in (bm,cm):require(Path(inspect.getsourcefile(module)).resolve().is_relative_to(root),'imported wrong Swift checkout')

        power=int(os.environ.get('EC528_TOPOLOGY_PART_POWER','10'))

        require(8<=power<=14,'part power must be between 8 and 14')

    except Exception:

        traceback.print_exc();return 77

    report={'result':'RUNNING','part_power':power,'cases':[],

            'sources':[inspect.getsourcefile(bm),inspect.getsourcefile(cm)],

            'scope':'scratch topology change + save atomicity; not live deployment or multi-file transaction'}

    def save():

        if args.results_json:

            p=Path(args.results_json);p.parent.mkdir(parents=True,exist_ok=True);p.write_text(json.dumps(report,indent=2)+'\n')

    save()

    stages=['success','partial_serialization','file_fsync_error','replacement_error','kill_before_rename','kill_after_rename']

    for kind in ('builder','composite'):

        for change in ('add_device','zero_weight'):

            for stage in stages:

                item={'kind':kind,'change':change,'stage':stage};started=time.monotonic()

                try:

                    with tempfile.TemporaryDirectory(prefix='exp12-') as tmp:

                        item['fixture_directory']=tmp

                        item.update(one_case(Path(tmp),kind,change,stage,power,bm,cm,RingData))

                    item['status']='PASS'

                except Exception as exc:

                    item.update(status='FAIL',error='%s: %s'%(type(exc).__name__,exc));traceback.print_exc()

                item['seconds']=time.monotonic()-started;report['cases'].append(item);save()

                print('%s/%s/%s: %s'%(kind,change,stage,item['status']),flush=True)

    report['result']='PASS' if len(report['cases'])==24 and all(c['status']=='PASS' for c in report['cases']) else 'FAIL';save()

    print('RESULT:',report['result'],flush=True)

    return 0 if report['result']=='PASS' else 1





def main():

    ap=argparse.ArgumentParser(description=__doc__,formatter_class=argparse.RawDescriptionHelpFormatter)

    ap.add_argument('--swift-src',default=str(Path(__file__).resolve().parents[1]))

    ap.add_argument('--timeout',type=int,default=180)

    ap.add_argument('--results-json')

    ap.add_argument('--worker',action='store_true',help=argparse.SUPPRESS)

    a=ap.parse_args()

    if not sys.platform.startswith('linux'):print('Linux required');return 77

    if a.timeout<=0:ap.error('positive timeout required')

    if a.worker:return worker(a)

    cmd=[sys.executable,str(Path(__file__).resolve()),'--worker','--swift-src',str(Path(a.swift_src).resolve()),'--timeout',str(a.timeout)]

    if a.results_json:cmd+=['--results-json',str(Path(a.results_json).resolve())]

    proc=subprocess.Popen(cmd,start_new_session=True)

    try:return proc.wait(timeout=a.timeout)

    except (subprocess.TimeoutExpired,KeyboardInterrupt):

        with contextlib.suppress(ProcessLookupError):os.killpg(proc.pid,signal.SIGKILL)

        proc.wait()

        if a.results_json:

            path=Path(a.results_json);path.parent.mkdir(parents=True,exist_ok=True)

            try:data=json.loads(path.read_text())

            except Exception:data={}

            data.update(result='INCOMPLETE',error='timeout or user interruption');path.write_text(json.dumps(data,indent=2)+'\n')

        print('RESULT: INCOMPLETE (timeout or interruption)');return 77



if __name__=='__main__':sys.exit(main())

