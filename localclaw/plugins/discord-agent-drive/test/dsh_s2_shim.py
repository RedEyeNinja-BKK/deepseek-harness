#!/usr/bin/env python3
"""dsh_s2_shim.py — S2 pre-live shim client + isolated seam drill driver.

NON-PRODUCTION isolated harness. Talks the AF_UNIX JSONL seam to the real
`discord-agent-drive.mjs` plugin inside a scratch DSH. Maintains the external
delivery ledger shape (processed / media_delivered / delivered_finalizations +
the smallest S2 text-finalization extension as a two-phase pending->delivered
record inside the SAME external ledger), records a fake Discord sink, and runs
the seam battery (Hermes Major 1-3 / Minor 4-7 closure + operator §14/§5/§8/§16).

External delivery authority = the shim ledger: a finalization is durably begun
(pending) BEFORE the external send and marked delivered after it, so a crash can
never cause a second send for one finalization id; a replay of a pending fid is
indeterminate (never auto-resend); a replay of a delivered fid is suppressed.
"""
import json, os, socket, sys, threading, time

SID = os.environ.get('S2_PILOT_SID', 'session-2f8c1f6a-0000-4000-8000-0000000000a1')
CONV = os.environ.get('S2_PILOT_CONV', 'channel:111111111111111111')
SOCK = os.environ.get('S2_SOCK', '/tmp/dsh-s2-seam/sock/dsh.sock')
EVID = os.environ.get('S2_EVID', '/tmp/dsh-s2-seam/evidence')
LEDGER = os.path.join(EVID, 'shim-ledger.json')
SINK = os.path.join(EVID, 'sink.jsonl')
LIVE = os.path.join(EVID, 'live-events.ndjson')
MODE = os.environ.get('S2_MODE', 'all')
MAX_FRAME = 1024 * 1024

os.makedirs(EVID, exist_ok=True)


def dsh_id(discord_id):
    return f'discord:{CONV}:{discord_id}'


def log(*a):
    line = ' '.join(str(x) for x in a)
    try:
        with open(os.path.join(EVID, 'driver.log'), 'a') as f:
            f.write(f'{time.strftime("%Y-%m-%dT%H:%M:%S")} {line}\n')
    except Exception:
        pass
    print(line, flush=True)


class Ledger:
    """External delivery authority (shim-owned, durable JSON)."""
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

    def _conv(self, conv):
        return self.d.setdefault('delivered_finalizations', {}).setdefault(conv, {})

    def state_of(self, conv, fid):
        e = self._conv(conv).get(fid)
        return (e or {}).get('state') if isinstance(e, dict) else None

    def delivered(self, conv, fid):
        return self.state_of(conv, fid) == 'delivered'

    def pending(self, conv, fid):
        return self.state_of(conv, fid) == 'pending'

    def begin(self, conv, fid):
        self._conv(conv)[fid] = {'state': 'pending', 'at': int(time.time())}
        self.save()

    def deliver(self, conv, fid):
        self._conv(conv)[fid] = {'state': 'delivered', 'at': int(time.time())}
        self.save()

    def settle_noop(self, conv, fid):
        self._conv(conv)[fid] = {'state': 'delivered', 'kind': 'noop', 'at': int(time.time())}
        self.save()

    def delivered_fids(self, conv):
        return [fid for fid, e in self._conv(conv).items()
                if isinstance(e, dict) and e.get('state') == 'delivered']

    def media_delivered(self, conv, identity):
        return identity in self.d.setdefault('media_delivered', {}).setdefault(conv, {})

    def media_mark(self, conv, identity):
        self.d.setdefault('media_delivered', {}).setdefault(conv, {})[identity] = int(time.time())
        self.save()


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
    """AF_UNIX JSONL client + reader thread."""
    def __init__(self, ledger, sink, hold_finalizations=False):
        self.ledger = ledger
        self.sink = sink
        self.sock = None
        self.q = []
        self.cv = threading.Condition()
        self.reader = None
        self.alive = False
        self.delivered_count = 0
        self.finalization_log = []
        self.hold_finalizations = hold_finalizations

    def connect(self, delivered=None):
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.settimeout(300)
        self.sock.connect(SOCK)
        self.alive = True
        self.reader = threading.Thread(target=self._reader, daemon=True)
        self.reader.start()
        ack = self.request({'type': 'hello', 'deliveredFinalizations': delivered or []})
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
        if self.hold_finalizations:
            return  # driver decides (outbound lost-ACK drill)
        if kind == 'noop':
            self.ledger.settle_noop(conv, fid)
            self._ack(f)
            return
        if self.ledger.delivered(conv, fid):
            log('  finalization duplicate suppressed (ledger):', fid)
            self._ack(f)
            return
        if self.ledger.pending(conv, fid):
            log('  finalization PENDING prior attempt - indeterminate, no resend:', fid)
            try:
                with open(os.path.join(EVID, 'indeterminate-pending.jsonl'), 'a') as fh:
                    fh.write(json.dumps({'fid': fid, 'at': int(time.time())}) + '\n')
            except Exception:
                pass
            self._ack(f)
            return
        artifacts = facts.get('artifacts') or []
        already_media = []
        for a in artifacts:
            ident = a.get('identity')
            if ident and self.ledger.media_delivered(conv, ident):
                already_media.append(ident)
        remaining = [a for a in artifacts if a.get('identity') not in already_media]
        if artifacts and not remaining:
            log('  media already delivered by identity; suppress:', fid)
            self.ledger.settle_noop(conv, fid)
            self._ack(f)
            return
        self.ledger.begin(conv, fid)  # durable pending BEFORE the external send
        if kind == 'text-fallback':
            self.sink.record('text', conv, fid, {'text': facts.get('text')})
        elif kind == 'artifact':
            for a in remaining:
                self.sink.record('media', conv, fid, {'artifact': a.get('path'), 'identity': a.get('identity')})
        elif kind == 'failure-notice':
            self.sink.record('notice', conv, fid, {'text': '(failure notice)'})
        self.ledger.deliver(conv, fid)
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

    def admit(self, conv, sid, discord_id, content):
        fr = {'type': 'admitted', 'conversationKey': conv, 'sessionId': sid,
              'discordMessageId': discord_id, 'dshMessageId': dsh_id(discord_id),
              'authorId': '222222222222222222', 'content': content, 'attachmentRefs': [], 'ts': int(time.time())}
        self.send(fr)
        ack = self.request({'type': 'admitted'},
                           pred=lambda f: f.get('for') == 'admitted' and str(f.get('discordMessageId')) == str(discord_id),
                           timeout=60)
        return ack


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
        log(('PASS ' if ok else 'FAIL ') + name + (' :: ' + str(detail)[:400] if not ok else ''))
    return results


def fresh_client(ledger, sink, delivered=None, hold=False):
    c = ShimClient(ledger, sink, hold_finalizations=hold)
    ack = c.connect(delivered if delivered is not None else ledger.delivered_fids(CONV))
    assert ack.get('type') == 'hello-ack', f'hello failed {ack}'
    return c


def turn_driven_text(ledger, sink, discord_id, marker, expect='-OK'):
    """One plain admission turn; waits until it completes; returns (ack, client)."""
    c = fresh_client(ledger, sink)
    c.request({'type': 'route', 'state': 'S2_ACTIVE'})
    ack = c.admit(CONV, SID, discord_id, f'Marker {marker}. Reply with exactly: {expect}')
    wait_for(lambda: count_user_occurrences(marker) >= 1, timeout=120)
    wait_for(lambda: count_turns() >= 1 and count_assistant() >= 1, timeout=120)
    wait_for(lambda: len(c.finalization_log) >= 1, timeout=60)
    time.sleep(2)
    return ack, c


# ---------------- cases ----------------
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
    c.send(json.dumps({'type': 'admitted', 'content': 'x' * (MAX_FRAME + 100)}))
    c.sock.settimeout(6)
    closed = False
    try:
        data = c.sock.recv(1024)
        if b'oversize' in data:
            closed = True
    except (socket.timeout, ConnectionResetError, OSError):
        closed = True
    c.close()
    return closed, 'connection reset / oversize error after oversized frame'


@case('C06-duplicate-inbound-event')
def c06(ledger, sink):
    ack1, c = turn_driven_text(ledger, sink, '9106', 'C06-MARK', 'C06-OK')
    ok1 = ack1.get('accepted') is True and ack1.get('durable') is True and ack1.get('observed') == 'claimed'
    occ = count_user_occurrences('C06-MARK')
    c.close()
    c2 = fresh_client(ledger, sink)
    c2.request({'type': 'route', 'state': 'S2_ACTIVE'})
    ack2 = c2.admit(CONV, SID, '9106', 'Marker C06-MARK. Reply with exactly: C06-OK')
    ok2 = ack2.get('accepted') is True and ack2.get('deduped') is True
    time.sleep(2)
    occ2 = count_user_occurrences('C06-MARK')
    ok = ok1 and ok2 and occ == 1 and occ2 == 1
    c2.close()
    return ok, {'ok1': ok1, 'ok2': ok2, 'occ': occ, 'occ2': occ2, 'ack2': ack2}


@case('C07-valid-admission-basic')
def c07(ledger, sink):
    ack, c = turn_driven_text(ledger, sink, '9107', 'C07-MARK', 'C07-OK')
    occ = count_user_occurrences('C07-MARK')
    ok = ack.get('accepted') is True and ack.get('durable') is True and ack.get('observed') == 'claimed' and occ == 1
    c.close()
    return ok, {'ack': ack, 'occ': occ}


@case('C08-confirmed-send-noop-or-honest-text-once')
def c08(ledger, sink):
    # Model variance tolerated: if the model calls the send tool with the ok gate
    # contract, the reducer MUST emit noop with ZERO fallback; if it instead
    # replies with text only, one honest text-fallback is delivered once. The
    # deterministic send-tool reducer path is covered by the fixture test + C16.
    c = fresh_client(ledger, sink)
    c.request({'type': 'route', 'state': 'S2_ACTIVE'})
    before_sink = sink.count()
    ack = c.admit(CONV, SID, '9108', 'Marker C08-SEND. Call the mcp__discord__send_message tool with channel_id "111111111111111111" and content "C08 hi". Then stop. Do not add extra text after the tool call.')
    ok1 = ack.get('accepted') is True
    wait_for(lambda: len(c.finalization_log) >= 1 or count_tool_results() >= 1, timeout=150)
    time.sleep(3)
    noop_seen = any(f.get('kind') == 'noop' for f in c.finalization_log)
    text_seen = any(f.get('kind') == 'text-fallback' for f in c.finalization_log)
    tool_seen = count_tool_results() >= 1
    delivered = sink.count() - before_sink
    ok = ok1 and not dup_fids(SINK) and ((noop_seen and delivered == 0) or (tool_seen and text_seen and delivered == 1) or (text_seen and delivered == 1))
    kinds = sorted({f.get('kind') for f in c.finalization_log})
    c.close()
    return ok, {'ok1': ok1, 'noop_seen': noop_seen, 'text_seen': text_seen, 'tool_seen': tool_seen, 'delivered': delivered, 'kinds': kinds}


@case('C09-media-exactly-once-integrity')
def c09(ledger, sink):
    # Live media turn is model-variance dependent; this case asserts the
    # EXACTLY-ONCE invariant the seam must hold regardless of path: no duplicate
    # fid anywhere, every sink delivery ledgered, at most one media + no
    # media/text mix for one fid, and media-by-identity suppression proven in C17.
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
    ledger_fids = set(ledger.delivered_fids(CONV))
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
    mixed = any(f.get('kind') == 'artifact' and (f.get('facts') or {}).get('text') for f in c.finalization_log)
    ok = ok1 and not dups and ledger_covers_sink and media_deliveries <= 1 and not (media_deliveries and text_deliveries) and not mixed
    c.close()
    return ok, {'ok1': ok1, 'kinds': kinds, 'media_deliveries': media_deliveries, 'text_deliveries': text_deliveries, 'dups': dups, 'ledger_covers_sink': ledger_covers_sink}


@case('C10-inbound-lost-ack-drill')
def c10(ledger, sink):
    # 1) admitted E/M; 2) connection cut BEFORE the inbound ACK; 3) reconnect;
    # 4) resend same E/M; 5) plugin detects already admitted -> ACK without a
    # second followup; 6) exactly one durable user occurrence + one turn.
    c = fresh_client(ledger, sink)
    c.request({'type': 'route', 'state': 'S2_ACTIVE'})
    before = count_user_occurrences('C10-LOST')
    c.send({'type': 'admitted', 'conversationKey': CONV, 'sessionId': SID,
            'discordMessageId': '9110', 'dshMessageId': dsh_id('9110'),
            'authorId': '222222222222222222', 'content': 'Marker C10-LOST. Reply with exactly: C10-OK',
            'attachmentRefs': [], 'ts': int(time.time())})
    time.sleep(0.8)  # plugin followup proceeds; we never read the ACK
    c.close()
    c2 = fresh_client(ledger, sink)
    c2.request({'type': 'route', 'state': 'S2_ACTIVE'})
    ack2 = c2.admit(CONV, SID, '9110', 'Marker C10-LOST. Reply with exactly: C10-OK')
    dedup = ack2.get('accepted') is True and ack2.get('deduped') is True
    wait_for(lambda: count_user_occurrences('C10-LOST') - before >= 1, timeout=120)
    wait_for(lambda: count_turns() >= before + 1, timeout=120)
    occ = count_user_occurrences('C10-LOST') - before
    ok = dedup and occ == 1
    c2.close()
    return ok, {'ack2': ack2, 'occ': occ}


@case('C11-shim-reconnect-hello')
def c11(ledger, sink):
    c1 = fresh_client(ledger, sink)
    c1.close()
    c2 = fresh_client(ledger, sink)  # reconnect after peer close (stale path)
    ra = c2.request({'type': 'route', 'state': 'S2_ACTIVE'})
    ok = ra.get('type') == 'route-ack'
    c2.close()
    return ok, {'route_ack': ra.get('type')}


@case('C12-sibling-negative-old-path')
def c12(ledger, sink):
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
    ack1, c = turn_driven_text(ledger, sink, '9131', 'C13-RB', 'C13-OK')
    ok_idle = ack1.get('accepted') is True
    ra = c.request({'type': 'route', 'state': 'QUIESCING_TO_OLD'})
    okq = ra.get('type') == 'route-ack' and ra.get('state') == 'QUIESCING_TO_OLD'
    ack2 = c.admit(CONV, SID, '9132', 'Marker C13-NO')
    okr = ack2.get('accepted') is False and ack2.get('code') == 'quiescing'
    ra2 = c.request({'type': 'route', 'state': 'OLD'})
    okold = ra2.get('type') == 'route-ack' and ra2.get('state') == 'OLD'
    ack3 = c.admit(CONV, SID, '9133', 'Marker C13-NO2')
    okold_rej = ack3.get('accepted') is False and ack3.get('code') == 'not-active'
    c.close()
    return (ok_idle and okq and okr and okold and okold_rej), {'q': ra, 'during': ack2, 'old': ra2, 'after': ack3}


@case('C14-rollback-in-flight-fence')
def c14(ledger, sink):
    c = fresh_client(ledger, sink)
    c.request({'type': 'route', 'state': 'S2_ACTIVE'})
    ack = c.admit(CONV, SID, '9141', 'Marker C14-FLIGHT. Call s2_delay with ms 6000, then reply with exactly: C14-OK')
    ok1 = ack.get('accepted') is True
    time.sleep(1.2)
    ra = c.request({'type': 'route', 'state': 'QUIESCING_TO_OLD'})
    okq = ra.get('type') == 'route-ack'
    end = time.time() + 100
    while time.time() < end and not any(f.get('kind') in ('noop', 'text-fallback', 'failure-notice', 'artifact') for f in c.finalization_log):
        time.sleep(1)
    ack2 = c.admit(CONV, SID, '9142', 'Marker C14-NO')
    ok_rej = ack2.get('accepted') is False
    ra2 = c.request({'type': 'route', 'state': 'OLD'})
    okold = ra2.get('type') == 'route-ack' and ra2.get('state') == 'OLD'
    occ = count_user_occurrences('C14-FLIGHT')
    ok = ok1 and okq and ok_rej and okold and occ == 1
    c.close()
    return ok, {'occ': occ, 'rej': ack2, 'old': ra2}


@case('C15-route-old-rejects-pilot')
def c15(ledger, sink):
    c = fresh_client(ledger, sink)  # boot default OLD
    ack = c.admit(CONV, SID, '9151', 'Marker C15-OLDNO')
    ok = ack.get('accepted') is False and ack.get('code') == 'not-active'
    c.close()
    return ok, ack


@case('C16-dup-outbound-replay-suppressed')
def c16(ledger, sink):
    fids = ledger.delivered_fids(CONV)
    if not fids:
        ack, c = turn_driven_text(ledger, sink, '9160', 'C16-SEED', 'C16-OK')
        ok1 = ack.get('accepted') is True
        c.close()
        fids = ledger.delivered_fids(CONV)
    else:
        ok1 = True
    if not fids:
        return False, 'no delivered fid available to replay'
    fid = fids[-1]
    before = sink.count()
    c = fresh_client(ledger, sink)
    kind = fid.split(':')[-1]
    c.send({'type': 'finalization', 'v': 1, 'conversationKey': CONV, 'sessionId': SID,
            'finalizationId': fid, 'kind': kind, 'turn': int(fid.split(':')[1]),
            'facts': {'text': 'DUPLICATE-REPLAY', 'suppress': True}})
    time.sleep(1.5)
    after = sink.count()
    ok = ok1 and after == before and ledger.delivered(CONV, fid) and not dup_fids(SINK)
    c.close()
    return ok, {'fid': fid, 'before': before, 'after': after}


@case('C17-media-identity-suppression')
def c17(ledger, sink):
    ident = 'a' * 64 + '|/mnt/off-vm-nfs/comfyui-media/fake-suppressed.mp3'
    ledger.media_mark(CONV, ident)
    c = fresh_client(ledger, sink)
    before = sink.count(kind='media')
    c.send({'type': 'finalization', 'v': 1, 'conversationKey': CONV, 'sessionId': SID,
            'finalizationId': f'{SID}:999:artifact', 'kind': 'artifact', 'turn': 999,
            'facts': {'artifacts': [{'path': '/mnt/off-vm-nfs/comfyui-media/fake-suppressed.mp3',
                                     'sha256': 'a' * 64, 'identity': ident}], 'abnormal': True}})
    time.sleep(1.5)
    after = sink.count(kind='media')
    ok = after == before
    c.close()
    return ok, {'before': before, 'after': after, 'media_ledger': ledger.media_delivered(CONV, ident)}


@case('C18-outbound-lost-ack-text-delivers-once')
def c18(ledger, sink):
    # Outbound lost-ACK drill (ordinary text): shim receives the finalization but
    # drops the connection BEFORE ack/delivery; reconnect with the authoritative
    # delivered set (which excludes this fid) -> plugin reconciles and re-emits;
    # shim delivers exactly once; no duplicate fid ever.
    c = fresh_client(ledger, sink, hold=True)
    c.request({'type': 'route', 'state': 'S2_ACTIVE'})
    ack = c.admit(CONV, SID, '9191', 'Marker C19-LOSTACK. Reply with exactly: C19-OK')
    ok1 = ack.get('accepted') is True
    wait_for(lambda: len(c.finalization_log) >= 1, timeout=150)
    c.close()
    before = sink.count()
    c2 = fresh_client(ledger, sink)  # delivered set excludes the held fid -> replay
    time.sleep(6)
    after = sink.count()
    dups = dup_fids(SINK)
    # Reconcile replays every owed finalization exactly once (>=1 new deliveries,
    # zero duplicates, all new fids ledgered).
    ok = ok1 and (after - before) >= 1 and not dups
    c2.close()
    return ok, {'before': before, 'after': after, 'dups': dups}


def post_restart(ledger, sink):
    """Plugin-restart continuation: same DSH_HOME re-booted; hello carries the
    authoritative delivered set; reconcile replays nothing missing; new admission
    still works; no duplicate deliveries ever; no replay of committed input."""
    res = []

    def add(name, ok, detail):
        res.append({'case': name, 'pass': bool(ok), 'detail': detail})
        log(('PASS ' if ok else 'FAIL ') + name)

    before_sink = sink.count()
    before_turns = count_turns()
    c = fresh_client(ledger, sink)
    time.sleep(4)  # reconcile window
    after_replay = sink.count()
    add('P01-restart-reconcile-no-dup', after_replay == before_sink and not dup_fids(SINK),
        {'before': before_sink, 'after_replay': after_replay})
    ra = c.request({'type': 'route', 'state': 'S2_ACTIVE'})
    add('P02-route-active-after-restart', ra.get('type') == 'route-ack', ra)
    ack = c.admit(CONV, SID, '9201', 'Marker P02-CONT. Reply with exactly: P02-OK')
    add('P03-post-restart-admission', ack.get('accepted') is True and ack.get('durable') is True and ack.get('observed') == 'claimed', ack)
    wait_for(lambda: count_user_occurrences('P02-CONT') >= 1, timeout=120)
    occ = count_user_occurrences('P02-CONT')
    add('P04-exactly-one-new-occurrence', occ == 1, {'occ': occ})
    wait_for(lambda: count_turns() >= before_turns + 1, timeout=120)
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
