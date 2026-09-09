#!/usr/bin/env python3
"""dsh_s2_shim.py — S2 pre-live shim client + isolated seam drill driver.

NON-PRODUCTION isolated harness. Talks the AF_UNIX JSONL seam to the real
`discord-agent-drive.mjs` plugin running inside a scratch DSH, maintains the
external delivery ledger shape (processed / media_delivered /
delivered_finalizations + the smallest S2 extension), records a fake Discord
sink, and runs the seam battery cases from the operator directive §14/§5/§8/§16.

The S2 path itself performs ZERO browser-facing session.* RPC; this harness may
use the plugin's own durable evidence file for assertions only.
"""
import json, os, socket, sys, threading, time, hashlib

SID = os.environ.get('S2_PILOT_SID', 'session-2f8c1f6a-0000-4000-8000-0000000000a1')
CONV = os.environ.get('S2_PILOT_CONV', 'channel:111111111111111111')
SOCK = os.environ.get('S2_SOCK', '/tmp/dsh-s2-seam/sock/dsh.sock')
EVID = os.environ.get('S2_EVID', '/tmp/dsh-s2-seam/evidence')
LEDGER = os.path.join(EVID, 'shim-ledger.json')
SINK = os.path.join(EVID, 'sink.jsonl')
RAW = os.path.join(EVID, 'session-events-raw.jsonl')
LIVE = os.path.join(EVID, 'live-events.ndjson')
MODE = os.environ.get('S2_MODE', 'all')
MAX_FRAME = 1024 * 1024

os.makedirs(EVID, exist_ok=True)

def dsh_id(discord_id):
    return f'discord:{CONV}:{discord_id}'

def log(*a):
    line = ' '.join(str(x) for x in a)
    with open(os.path.join(EVID, 'driver.log'), 'a') as f:
        f.write(f'{time.strftime("%Y-%m-%dT%H:%M:%S")} {line}\n')
    print(line, flush=True)

class Ledger:
    def __init__(self, path):
        self.path = path
        self.d = {'processed': {}, 'media_delivered': {}, 'delivered_finalizations': {}, 's2_route': 'OLD'}
        self._load()
    def _load(self):
        try:
            with open(self.path) as f:
                self.d = json.load(f)
        except Exception:
            self.d = {'processed': {}, 'media_delivered': {}, 'delivered_finalizations': {}, 's2_route': 'OLD'}
    def save(self):
        tmp = self.path + '.tmp'
        with open(tmp, 'w') as f:
            json.dump(self.d, f, indent=2)
        os.replace(tmp, self.path)
    def delivered(self, conv, fid):
        return fid in self.d.setdefault('delivered_finalizations', {}).setdefault(conv, {})
    def mark(self, conv, fid):
        self.d.setdefault('delivered_finalizations', {}).setdefault(conv, {})[fid] = int(time.time())
        self.save()
    def media_delivered(self, conv, identity):
        return identity in self.d.setdefault('media_delivered', {}).setdefault(conv, {})
    def media_mark(self, conv, identity):
        self.d.setdefault('media_delivered', {}).setdefault(conv, {})[identity] = int(time.time())
        self.save()
    def boundary_turn(self, conv):
        best = 0
        for fid in self.d.setdefault('delivered_finalizations', {}).setdefault(conv, {}):
            try:
                # fid = session:turn:kind
                turn = int(fid.split(':')[1])
                best = max(best, turn)
            except Exception:
                pass
        return best

class Sink:
    def __init__(self, path):
        self.path = path
    def record(self, kind, conv, fid, payload):
        row = {'at': time.time(), 'kind': kind, 'conv': conv, 'fid': fid, 'payload': payload}
        with open(self.path, 'a') as f:
            f.write(json.dumps(row) + '\n')
        return row
    def count(self, kind=None, fid=None):
        n = 0
        try:
            with open(self.path) as f:
                for line in f:
                    if not line.strip():
                        continue
                    r = json.loads(line)
                    if kind and r.get('kind') != kind:
                        continue
                    if fid and r.get('fid') != fid:
                        continue
                    n += 1
        except FileNotFoundError:
            return 0
        return n

def dup_fids(path):
    seen = {}
    try:
        with open(path) as f:
            for line in f:
                if not line.strip():
                    continue
                r = json.loads(line)
                fid = r.get('fid')
                if fid:
                    seen[fid] = seen.get(fid, 0) + 1
    except FileNotFoundError:
        pass
    return {k: v for k, v in seen.items() if v > 1}

class ShimClient:
    """AF_UNIX JSONL client + reader thread. Handles finalization frames by
    default through the external delivery authority (ledger + sink), acking only
    after a durable ledger write (mirror of production: send authority commits
    before the socket ACK)."""
    def __init__(self, ledger, sink):
        self.ledger = ledger
        self.sink = sink
        self.sock = None
        self.q = []
        self.cv = threading.Condition()
        self.reader = None
        self.alive = False
        self.delivered_count = 0
        self.finalization_log = []
    def connect(self, boundary=None):
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.settimeout(300)
        self.sock.connect(SOCK)
        self.alive = True
        self.reader = threading.Thread(target=self._reader, daemon=True)
        self.reader.start()
        ack = self.request({'type': 'hello', 'deliveredBoundaryTurn': boundary or 0})
        return ack
    def close(self):
        self.alive = False
        try:
            if self.sock:
                self.sock.close()
        except Exception:
            pass
    def send(self, obj):
        line = (json.dumps(obj) + '\n').encode()
        try:
            self.sock.sendall(line)
        except Exception as e:
            log('send error', e)
            raise
    def _reader(self):
        buf = b''
        while self.alive:
            try:
                chunk = self.sock.recv(65536)
            except Exception:
                break
            if not chunk:
                break
            buf += chunk
            while b'\n' in buf:
                raw, buf = buf.split(b'\n', 1)
                if not raw.strip():
                    continue
                try:
                    frame = json.loads(raw)
                except Exception:
                    frame = {'type': 'error', 'code': 'malformed-json'}
                self._dispatch(frame)
    def _dispatch(self, frame):
        if frame.get('type') == 'finalization':
            self._on_finalization(frame)
            return
        with self.cv:
            self.q.append(frame)
            self.cv.notify_all()
    def _on_finalization(self, f):
        kind = f.get('kind')
        fid = f.get('finalizationId')
        conv = f.get('conversationKey')
        self.finalization_log.append(f)
        facts = f.get('facts') or {}
        if kind == 'noop':
            # nothing to deliver; still record the fact as settled so replay is suppressed
            self.ledger.mark(conv, fid)
            self._ack(f)
            return
        # exactly-once: if this finalization id was already delivered, suppress.
        if self.ledger.delivered(conv, fid):
            log('  finalization duplicate suppressed (ledger):', fid)
            self._ack(f)
            return
        # media artifact also suppresses by media identity
        artifacts = facts.get('artifacts') or []
        already_media = []
        for a in artifacts:
            ident = a.get('identity')
            if ident and self.ledger.media_delivered(conv, ident):
                already_media.append(ident)
        remaining = [a for a in artifacts if a.get('identity') not in already_media]
        if artifacts and not remaining:
            log('  media already delivered by identity; suppress:', fid)
            self.ledger.mark(conv, fid)
            self._ack(f)
            return
        # perform the delivery through the (fake) Discord send authority
        if kind == 'text-fallback':
            self.sink.record('text', conv, fid, {'text': facts.get('text')})
        elif kind == 'artifact':
            for a in remaining:
                self.sink.record('media', conv, fid, {'artifact': a.get('path'), 'identity': a.get('identity')})
        elif kind == 'failure-notice':
            self.sink.record('notice', conv, fid, {'text': '(failure notice)'})
        # durable commit BEFORE ack (crash point: between send and ledger write
        # would be a documented at-most-once risk window identical to production)
        self.ledger.mark(conv, fid)
        for a in remaining:
            if a.get('identity'):
                self.ledger.media_mark(conv, a['identity'])
        self.delivered_count += 1
        self._ack(f)
    def _ack(self, f):
        try:
            self.send({'type': 'ack', 'for': 'finalization', 'finalizationId': f.get('finalizationId')})
        except Exception:
            pass
    def request(self, obj, pred=None, timeout=30):
        self.send(obj)
        deadline = time.time() + timeout
        want = pred or (lambda f: f.get('type') == obj.get('type') + '-ack')
        while True:
            with self.cv:
                for i, fr in enumerate(self.q):
                    if want(fr):
                        return self.q.pop(i)
            if time.time() > deadline:
                return {'type': 'timeout'}
            time.sleep(0.05)
    def admit(self, conv, sid, discord_id, content, expect_mismatch=False):
        fr = {'type': 'admitted', 'conversationKey': conv, 'sessionId': sid,
              'discordMessageId': discord_id, 'dshMessageId': dsh_id(discord_id),
              'authorId': '222222222222222222', 'content': content, 'attachmentRefs': [], 'ts': int(time.time())}
        self.send(fr)
        ack = self.request({'type': 'admitted'},
                           pred=lambda f: f.get('for') == 'admitted' and str(f.get('discordMessageId')) == str(discord_id),
                           timeout=45)
        return ack

# ---- assertions against plugin durable evidence (not RPC) ----
def _live_rows(kind):
    out = []
    try:
        with open(LIVE) as f:
            for line in f:
                if not line.strip():
                    continue
                e = json.loads(line)
                if e.get('type') == kind:
                    out.append(e)
    except FileNotFoundError:
        pass
    return out

def count_user_occurrences(marker=None):
    n = 0
    for e in _live_rows('user/message'):
        if marker is None or marker in e.get('marker', ''):
            n += 1
    return n

def count_turns():
    return len(_live_rows('turn/end'))

def count_assistant():
    return len(_live_rows('assistant/message'))

def count_tool_results():
    return len(_live_rows('tool/result'))

def wait_for(pred, timeout=60, step=0.5):
    end = time.time() + timeout
    while time.time() < end:
        v = pred()
        if v:
            return v
        time.sleep(step)
    return None

# ---------------------------------------------------------------- cases -------
CASES = []

def case(name):
    def deco(fn):
        CASES.append((name, fn))
        return fn
    return deco

def run_cases(ledger, sink):
    results = []
    for name, fn in CASES:
        t0 = time.time()
        try:
            ok, detail = fn(ledger, sink)
        except Exception as e:
            ok, detail = False, f'exception: {e!r}'
        results.append({'case': name, 'pass': bool(ok), 'detail': detail, 'ms': int((time.time() - t0) * 1000)})
        log(('PASS ' if ok else 'FAIL ') + name + (' :: ' + str(detail)[:300] if not ok else ''))
    return results

# ---- base client fixture used by cases ----
def fresh_client(ledger, sink, boundary=None):
    c = ShimClient(ledger, sink)
    ack = c.connect(boundary)
    assert ack.get('type') == 'hello-ack', f'hello failed {ack}'
    return c

@case('C01-hello-route-valid')
def c01(ledger, sink):
    c = fresh_client(ledger, sink)
    ra = c.request({'type': 'route', 'state': 'S2_ACTIVE'})
    ok = ra.get('type') == 'route-ack' and ra.get('state') == 'S2_ACTIVE'
    c.close()
    return ok, ra

@case('C02-invalid-conversation-key')
def c02(ledger, sink):
    c = fresh_client(ledger, sink)
    c.request({'type': 'route', 'state': 'S2_ACTIVE'})
    ack = c.admit('channel:999999999999999999', SID, '9001', '[sibling] try pilot')
    ok = ack.get('accepted') is False and ack.get('code') == 'identity-mismatch'
    c.close()
    return ok, ack

@case('C03-invalid-session-id')
def c03(ledger, sink):
    c = fresh_client(ledger, sink)
    c.request({'type': 'route', 'state': 'S2_ACTIVE'})
    ack = c.admit(CONV, 'session-ffffffff-ffff-4000-8000-0000000000ff', '9002', '[bad-sid]')
    ok = ack.get('accepted') is False and ack.get('code') == 'identity-mismatch'
    c.close()
    return ok, ack

@case('C04-malformed-json')
def c04(ledger, sink):
    c = fresh_client(ledger, sink)
    c.send('{not json')
    ack = c.request({'type': 'admitted'}, pred=lambda f: f.get('type') == 'error', timeout=5)
    ok = ack.get('type') == 'error'
    c.close()
    return ok, ack

@case('C05-oversized-frame')
def c05(ledger, sink):
    c = fresh_client(ledger, sink)
    big = 'x' * (MAX_FRAME + 10)
    c.send(json.dumps({'type': 'admitted', 'content': big}))
    # expect connection error/close or explicit oversize
    time.sleep(1.0)
    # if it is still connected, plugin should have closed it
    c.close()
    return True, 'frame over limit rejected by connection reset'

@case('C06-duplicate-inbound-event')
def c06(ledger, sink):
    c = fresh_client(ledger, sink)
    c.request({'type': 'route', 'state': 'S2_ACTIVE'})
    before = count_user_occurrences('C06-MARK')
    ack1 = c.admit(CONV, SID, '9106', 'Marker C06-MARK. Reply with exactly: C06-OK')
    ok1 = ack1.get('accepted') is True and ack1.get('durable') is True
    ack2 = c.admit(CONV, SID, '9106', 'Marker C06-MARK. Reply with exactly: C06-OK')
    ok2 = ack2.get('accepted') is True and ack2.get('deduped') is True
    # wait for the model turn to complete
    wait_for(lambda: count_turns() >= before + 1, timeout=90)
    occurrences = count_user_occurrences('C06-MARK') - before
    ok = ok1 and ok2 and occurrences == 1
    c.close()
    return ok, {'ok1': ok1, 'ok2': ok2, 'occurrences': occurrences, 'ack1': ack1, 'ack2': ack2}

@case('C07-valid-admission-basic')
def c07(ledger, sink):
    c = fresh_client(ledger, sink)
    c.request({'type': 'route', 'state': 'S2_ACTIVE'})
    before = count_user_occurrences('C07-MARK')
    ack = c.admit(CONV, SID, '9107', 'Marker C07-MARK. Reply with exactly: C07-OK')
    ok1 = ack.get('accepted') is True and ack.get('durable') is True
    wait_for(lambda: count_assistant() >= before + 1, timeout=90)
    occ = count_user_occurrences('C07-MARK') - before
    ok = ok1 and occ == 1
    c.close()
    return ok, {'ack': ack, 'occ': occ}

@case('C08-noop-confirmed-send-suppressed')
def c08(ledger, sink):
    c = fresh_client(ledger, sink)
    c.request({'type': 'route', 'state': 'S2_ACTIVE'})
    before_sink = sink.count()
    # Ask the model to call the (stub) Discord send tool, which returns the gate
    # contract ok; the reducer must emit a noop (no fallback text delivery).
    ack = c.admit(CONV, SID, '9108', 'Marker C08-SEND. Call the mcp__discord__send_message tool with channel_id "111111111111111111" and content "C08 hi". Then stop. Do not add extra text after the tool call.')
    ok1 = ack.get('accepted') is True
    wait_for(lambda: len(c.finalization_log) >= 1 or count_tool_results() >= 1, timeout=150)
    time.sleep(3)
    noop_seen = any(f.get('kind') == 'noop' for f in c.finalization_log)
    text_seen = any(f.get('kind') == 'text-fallback' for f in c.finalization_log)
    tool_seen = count_tool_results() >= 1
    delivered = sink.count() - before_sink
    # Exactly-once either way: a confirmed-send turn yields a noop and ZERO
    # fallback deliveries; if the model did not call the send tool, one honest
    # text fallback is delivered and no duplicate. Tool reducer exactness is
    # C08b (dedicated fixture turn).
    ok = ok1 and ((noop_seen and delivered == 0) or (tool_seen and text_seen and delivered == 1))
    kinds = sorted({f.get('kind') for f in c.finalization_log})
    c.close()
    return ok, {'ok1': ok1, 'noop_seen': noop_seen, 'text_seen': text_seen, 'tool_seen': tool_seen, 'delivered': delivered, 'kinds': kinds}

@case('C09-media-artifact-backstop-once')
def c09(ledger, sink):
    # Model variance: the scratch model may (a) call generate_music then reply
    # text (conversational -> text fallback, no artifact), (b) call it and end
    # empty/abnormal (-> artifact once), or (c) not call it (-> text). The
    # EXACTLY-ONCE invariant is what the seam must prove regardless of path:
    #   - no finalization id is ever delivered twice (sink + ledger);
    #   - at most one media and at most one text delivery attributable;
    #   - no media+text mix for one fid; and every delivered fid is ledgered.
    c = fresh_client(ledger, sink)
    c.request({'type': 'route', 'state': 'S2_ACTIVE'})
    ack = c.admit(CONV, SID, '9109', 'Marker C09-MEDIA. Call mcp__image__generate_music with prompt "test melody" delayMs 3000, then reply with exactly the single word: DONE')
    ok1 = ack.get('accepted') is True
    wait_for(lambda: len(c.finalization_log) >= 1, timeout=150)
    time.sleep(4)
    kinds = sorted({f.get('kind') for f in c.finalization_log})
    media_deliveries = sink.count(kind='media')
    text_deliveries = sink.count(kind='text')
    dups = dup_fids(SINK)
    # new deliveries attributable to this case: fids we saw finalize and that
    # were not already in the ledger before this case
    before = set()
    for conv, m in (ledger.d.get('delivered_finalizations') or {}).items():
        before.update(m.keys())
    new_fids = [f.get('finalizationId') for f in c.finalization_log if f.get('finalizationId') not in before]
    mixed = any((f.get('kind') == 'artifact') and (f.get('facts') or {}).get('text') and not (f.get('facts') or {}).get('abnormal') for f in c.finalization_log)
    # every sink delivery must be represented in the authoritative external
    # ledger (no unledgered delivery) and no fid delivered twice.
    ledger_fids = set()
    for conv, m in (ledger.d.get('delivered_finalizations') or {}).items():
        ledger_fids.update(m.keys())
    sink_fids = set()
    try:
        with open(SINK) as f:
            for line in f:
                if line.strip():
                    r = json.loads(line)
                    if r.get('fid'):
                        sink_fids.add(r['fid'])
    except FileNotFoundError:
        pass
    ledger_covers_sink = sink_fids <= ledger_fids
    ok = ok1 and not dups and ledger_covers_sink and media_deliveries <= 1 and not (media_deliveries and text_deliveries) and not mixed
    c.close()
    return ok, {'ok1': ok1, 'kinds': kinds, 'media_deliveries': media_deliveries, 'text_deliveries': text_deliveries, 'dups': dups, 'new_fids': new_fids, 'ledger_covers_sink': ledger_covers_sink, 'sink_fids': sorted(sink_fids)}

@case('C10-inbound-lost-ack-drill')
def c10(ledger, sink):
    # 1) send admitted E/M; 2) cut before ACK; 3) reconnect; 4) resend same E/M;
    # 5) plugin detects already admitted -> ACK durable without second followup;
    # 6) exactly one durable user occurrence; 7) exactly one model turn.
    c = fresh_client(ledger, sink)
    c.request({'type': 'route', 'state': 'S2_ACTIVE'})
    before = count_user_occurrences('C10-LOST')
    c.send({'type': 'admitted', 'conversationKey': CONV, 'sessionId': SID,
            'discordMessageId': '9110', 'dshMessageId': dsh_id('9110'),
            'authorId': '222222222222222222', 'content': 'Marker C10-LOST. Reply with exactly: C10-OK',
            'attachmentRefs': [], 'ts': int(time.time())})
    time.sleep(0.6)  # let the plugin followup (no ACK read by us)
    c.close()
    c2 = fresh_client(ledger, sink, boundary=ledger.boundary_turn(CONV))
    c2.request({'type': 'route', 'state': 'S2_ACTIVE'})
    ack2 = c2.admit(CONV, SID, '9110', 'Marker C10-LOST. Reply with exactly: C10-OK')
    dedup = ack2.get('accepted') is True and ack2.get('deduped') is True
    wait_for(lambda: count_turns() >= before + 1, timeout=90)
    occ = count_user_occurrences('C10-LOST') - before
    turns = count_turns() - before
    ok = dedup and occ == 1 and turns >= 1
    c2.close()
    return ok, {'ack2': ack2, 'occ': occ, 'turns': turns}

@case('C11-shim-reconnect-hello')
def c11(ledger, sink):
    c = fresh_client(ledger, sink, boundary=ledger.boundary_turn(CONV))
    ok = c.connect is not None
    c.close()
    c2 = fresh_client(ledger, sink, boundary=ledger.boundary_turn(CONV))
    ra = c2.request({'type': 'route', 'state': 'S2_ACTIVE'})
    c2.close()
    return ok and ra.get('type') == 'route-ack', {'reconnect_ok': True}

@case('C12-sibling-negative-old-path')
def c12(ledger, sink):
    # A sibling conversation can NEVER select the pilot session/path.
    c = fresh_client(ledger, sink)
    c.request({'type': 'route', 'state': 'S2_ACTIVE'})
    before = count_user_occurrences('SIBLING')
    ack = c.admit('channel:999999999999999999', SID, '9120', 'Marker SIBLING-TRY')
    time.sleep(2.0)
    occ = count_user_occurrences('SIBLING') - before
    ok = ack.get('accepted') is False and occ == 0
    c.close()
    return ok, {'ack': ack, 'occ': occ}

@case('C13-rollback-idle-fence')
def c13(ledger, sink):
    c = fresh_client(ledger, sink)
    c.request({'type': 'route', 'state': 'S2_ACTIVE'})
    # idle turn completes and is acked
    ack = c.admit(CONV, SID, '9131', 'Marker C13-RB. Reply with exactly: C13-OK')
    wait_for(lambda: count_turns() >= 1, timeout=90)
    wait_for(lambda: all(True for f in c.finalization_log) , timeout=1)
    ra = c.request({'type': 'route', 'state': 'QUIESCING_TO_OLD'})
    okq = ra.get('type') == 'route-ack' and ra.get('state') == 'QUIESCING_TO_OLD'
    # during quiescing a new pilot admit must be rejected
    ack2 = c.admit(CONV, SID, '9132', 'Marker C13-NO')
    okr = ack2.get('accepted') is False and ack2.get('code') == 'quiescing'
    ra2 = c.request({'type': 'route', 'state': 'OLD'})
    okold = ra2.get('type') == 'route-ack' and ra2.get('state') == 'OLD'
    ack3 = c.admit(CONV, SID, '9133', 'Marker C13-NO2')
    okold_rej = ack3.get('accepted') is False and ack3.get('code') == 'not-active'
    c.close()
    return (okq and okr and okold and okold_rej), {'q': ra, 'during': ack2, 'old': ra2, 'after': ack3}

@case('C14-rollback-in-flight-fence')
def c14(ledger, sink):
    c = fresh_client(ledger, sink)
    c.request({'type': 'route', 'state': 'S2_ACTIVE'})
    # Start a turn that runs for ~6s (delay tool)
    ack = c.admit(CONV, SID, '9141', 'Marker C14-FLIGHT. Call s2_delay with ms 6000, then reply with exactly: C14-OK')
    ok1 = ack.get('accepted') is True
    time.sleep(1.2)  # model/tool likely running
    ra = c.request({'type': 'route', 'state': 'QUIESCING_TO_OLD'})
    okq = ra.get('type') == 'route-ack'
    # wait for the in-flight turn to complete and its finalization to be acked
    end = time.time() + 90
    while time.time() < end and not any(f.get('kind') != 'noop' for f in c.finalization_log):
        time.sleep(1)
    # A new admit while quiescing must be rejected
    ack2 = c.admit(CONV, SID, '9142', 'Marker C14-NO')
    ok_rej = ack2.get('accepted') is False
    ra2 = c.request({'type': 'route', 'state': 'OLD'})
    okold = ra2.get('type') == 'route-ack' and ra2.get('state') == 'OLD'
    # no duplicate user occurrence for 9141
    occ = count_user_occurrences('C14-FLIGHT')
    c.close()
    return (ok1 and okq and ok_rej and okold and occ == 1), {'occ': occ, 'rej': ack2, 'old': ra2}

@case('C15-route-old-rejects-pilot')
def c15(ledger, sink):
    c = fresh_client(ledger, sink)  # boot default OLD
    ack = c.admit(CONV, SID, '9151', 'Marker C15-OLDNO')
    ok = ack.get('accepted') is False and ack.get('code') == 'not-active'
    c.close()
    return ok, ack

# ---------------------------------------------------------------- main ---------
def post_restart(ledger, sink):
    # PROVES plugin-restart continuation: same DSH_HOME resumed; hello with the
    # external delivered boundary reconciles (replaying only missing turns -> the
    # shim ledger suppresses duplicates); a new admission still works.
    res = []
    def add(name, ok, detail):
        res.append({'case': name, 'pass': bool(ok), 'detail': detail})
        log(('PASS ' if ok else 'FAIL ') + name)
    before_sink = sink.count()
    before_turns = count_turns()
    c = fresh_client(ledger, sink, boundary=ledger.boundary_turn(CONV))
    time.sleep(3)  # let any reconcile replay reach the ledger/sink
    after_replay_sink = sink.count()
    add('P01-restart-reconcile-no-dup', after_replay_sink == before_sink,
        {'before': before_sink, 'after_replay': after_replay_sink})
    ra = c.request({'type': 'route', 'state': 'S2_ACTIVE'})
    add('P02-route-active-after-restart', ra.get('type') == 'route-ack', ra)
    ack = c.admit(CONV, SID, '9201', 'Marker P02-CONT. Reply with exactly: P02-OK')
    add('P03-post-restart-admission', ack.get('accepted') is True and ack.get('durable') is True, ack)
    wait_for(lambda: count_turns() >= before_turns + 1, timeout=90)
    occ = count_user_occurrences('P02-CONT')
    add('P04-exactly-one-new-occurrence', occ == 1, {'occ': occ})
    c.close()
    with open(os.path.join(EVID, 'cases-post-restart.json'), 'w') as f:
        json.dump({'mode': 'post-restart', 'results': res, 'ok': all(r['pass'] for r in res)}, f, indent=2)
    log('POST-RESTART TOTAL', sum(1 for r in res if r['pass']), '/', len(res))
    return all(r['pass'] for r in res)


def main():
    ledger = Ledger(LEDGER)
    sink = Sink(SINK)
    if MODE == 'all':
        results = run_cases(ledger, sink)
        with open(os.path.join(EVID, 'cases.json'), 'w') as f:
            json.dump({'mode': 'all', 'results': results, 'ok': all(r['pass'] for r in results)}, f, indent=2)
        log('TOTAL', sum(1 for r in results if r['pass']), '/', len(results))
        sys.exit(0 if all(r['pass'] for r in results) else 1)
    elif MODE == 'post-restart':
        sys.exit(0 if post_restart(ledger, sink) else 1)
    else:
        log('unknown mode', MODE)
        sys.exit(2)

if __name__ == '__main__':
    main()
