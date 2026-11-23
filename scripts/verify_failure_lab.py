"""Run an opt-in local crash drill and print measured results."""

import argparse
import collections
import json
import time
import urllib.request
from urllib.parse import urlsplit

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--url', default='http://127.0.0.1:5173')
parser.add_argument('--slot', choices=['worker-1', 'worker-2', 'worker-3'], default='worker-1')
args = parser.parse_args()
origin = urlsplit(args.url)
if origin.scheme != 'http' or origin.hostname not in ('127.0.0.1', 'localhost', '::1') or origin.path not in ('', '/'):
    parser.error('--url must be a loopback HTTP origin')

def api(path, data=None):
    req = urllib.request.Request(args.url.rstrip('/') + path,
        data=json.dumps(data).encode() if data is not None else None,
        headers={'Content-Type':'application/json','X-QueueForge-Demo':'1'})
    with urllib.request.urlopen(req, timeout=5) as response:
        return json.load(response)

def until(check, timeout):
    deadline=time.monotonic()+timeout
    while time.monotonic()<deadline:
        result=check()
        if result is not None:
            return result
        time.sleep(.1)
    raise AssertionError('timed out')

assert api('/api/failure-lab')['enabled'], 'start the opt-in lab before running the drill'
def idle_cluster():
    metrics = api('/api/metrics')
    idle = [w for w in api('/api/workers') if w['health'] == 'healthy' and w['state'] == 'idle']
    return idle if len(idle) == 3 and metrics['queued'] == metrics['running'] == 0 else None

try:
    until(idle_cluster, 75)
except AssertionError as error:
    raise SystemExit('The drill requires three healthy, idle workers and an empty queue.') from error
jobs=api('/api/demo',{'count':9,'profile':'failure'})['jobs']
ids={j['id'] for j in jobs}

def busy_workers():
    busy=[w for w in api('/api/workers') if w['health']=='healthy' and w['state']=='busy' and w['current_job_id'] in ids]
    return busy if len(busy)==3 else None

busy=until(busy_workers,8)
target=next(w for w in busy if w['demo_slot']==args.slot)
request_time=time.monotonic()
crash=api('/api/failure-lab/crash',{'slot':args.slot})
assert crash['worker_id']==target['id'] and crash['job_id']==target['current_job_id']
job_id=crash['job_id']
attempt=api(f'/api/jobs/{job_id}')
lease_deadline=attempt['lease_expires_at']
def observe():
    worker=next(w for w in api('/api/workers') if w['id']==target['id'])
    events=api(f'/api/jobs/{job_id}/history')
    now=time.monotonic()
    if worker['health'] in ('stale','offline'):
        times.setdefault(worker['health'],now-request_time)
    for event in events:
        if event['kind'] in ('lease_expired','recovered') or (event['kind']=='claimed' and event['attempt']==2):
            times.setdefault('reassigned' if event['kind']=='claimed' else event['kind'],now-request_time)
    return events if events and events[-1]['kind'] in ('completed','permanently_failed') else None

times={}
events=until(observe,45)
def all_finished():
    current=[j for j in api('/api/jobs?limit=100')['jobs'] if j['id'] in ids]
    return current if len(current)==len(ids) and all(j['status'] in ('completed','failed') for j in current) else None

finished=until(all_finished,65)
claims=[e for e in events if e['kind']=='claimed']
assert len(claims)>=2 and claims[0]['worker_id']==target['id'] and claims[1]['worker_id']!=target['id'], claims
assert len([e for e in events if e['kind'] in ('completed','permanently_failed')])==1, events
extra_retries = [
    {'job_id': job['id'], 'attempts': job['attempt_count'], 'events': api(f"/api/jobs/{job['id']}/history")}
    for job in finished if job['id'] != job_id and job['attempt_count'] > 1
]
assert {'offline','lease_expired','recovered','reassigned'} <= times.keys()
print(json.dumps({'submitted':len(ids),'crash':crash,'elapsed_seconds':{k:round(v,2) for k,v in times.items()},'outcomes':collections.Counter(j['status'] for j in finished),'total_attempts':sum(j['attempt_count'] for j in finished),'concurrent_workers_before_crash':len(busy),'lease_deadline':lease_deadline,'duplicate_attempts':sum(j['attempt_count']-1 for j in finished),'additional_retried_jobs':extra_retries,'events':events},indent=2))
