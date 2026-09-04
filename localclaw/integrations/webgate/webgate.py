#!/usr/bin/env python3
"""dsh-webgate — minimal DSH web authority projection over CloakBrowser.

Round-2 fixes: TRUE wall-clock budget via per-request worker PROCESS with
SIGKILL watchdog (BLOCKER 1 — sync Playwright cannot cancel, so the whole
browse runs in a forked child that the parent hard-kills at PAGE_BUDGET_S);
per-request DNS cache + bounded route-check count (finding 11); strict fd
adoption with close-on-exec, type masking, env cleanup (finding 7); exact
8 KiB read cap with deterministic violation close (finding 6); strict URL
parser validation of the compiled origin (finding 14); strict adapter-side
framing/params (adapter file); canonical-host plumbing (guard file).

Wire protocol (newline-delimited JSON):
  request : {"op": "fetch", "url": "..."} | {"op": "search", "query": "..."}
  response: {"ok": true, ...} | {"ok": false, "error": "<fixed category>"}
The bearer NEVER appears in any response or log line.
"""
from __future__ import annotations

import json
import multiprocessing
import os
import socket
import socketserver
import sys
import time
from urllib.parse import urlsplit

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import webgate_guard as guard  # noqa: E402
import webgate_pinned_fetch as pin  # noqa: E402

# ---- server-side fixed authority (compiled-in; NOT environment-controlled) --
# Deployment-specific: the CloakBrowser Manager origin this gate is authorized
# to reach (your tailnet Funnel/Serve hostname for the manager).
MANAGER_ORIGIN = "https://REPLACE-WITH-MANAGER-HOST"
PROFILE_ID = "51616565-7faf-477a-ad1e-ca5ae50375f5"         # generic stateless profile

GOTO_TIMEOUT_MS = 45_000
PAGE_BUDGET_S = 90
MAX_BODY_CHARS = 100_000
MAX_HEAD_LINES = 40
MAX_LINKS = 20
MAX_RESULTS = 10
MAX_QUERY_LEN = 400
MAX_REQUEST_LINE = 8192
MAX_ROUTE_CHECKS = 500
SEARCH_ENGINE = "https://lite.duckduckgo.com/lite/?q="  # lite endpoint: serves the gate's anonymous pinned fetch (html. endpoint bot-blocks it, verified live 2026-08-31)


def validate_origin(origin: str) -> None:
    """Strict origin validation at startup (round-2 finding 14): https, exact
    host with no path/query/fragment/userinfo."""
    try:
        parts = urlsplit(origin)
    except ValueError:
        raise RuntimeError("bad manager origin")
    if parts.scheme != "https" or not parts.hostname or parts.username or \
            parts.password or parts.path not in ("", "/") or parts.query or \
            parts.fragment or parts.port is not None:
        raise RuntimeError("bad manager origin")


def read_token() -> str:
    """Bearer enters ONLY via systemd LoadCredential. Never logged."""
    d = os.environ.get("CREDENTIALS_DIRECTORY")
    path = os.path.join(d, "manager-token") if d else ""
    if not path or not os.path.isfile(path):
        raise RuntimeError("credential 'manager-token' not provisioned")
    with open(path, "r", encoding="utf-8") as fh:
        tok = fh.read().strip()
    if not tok:
        raise RuntimeError("empty credential")
    return tok


def document_route(route, deadline: float) -> None:
    """Option B route handler (module-level, testable). The browser NEVER
    connects to the destination: navigation/document requests are fulfilled by
    the gate's OWN pinned fetch (one DNS resolution feeds validation AND
    connection; connected peer re-verified). All subresources and
    non-document requests are aborted — the text-extraction contract needs
    none. Redirects: a 3xx fulfilled document makes Chromium re-request the
    Location URL through THIS handler — re-validated and re-pinned per hop.
    A DNS answer changing after validation cannot redirect the connection."""
    req = route.request
    if req.resource_type != "document" or not req.is_navigation_request():
        route.abort()
        return
    remaining = deadline - time.time()
    if remaining <= 0:
        route.abort()
        return
    try:
        status, headers, body = pin.pinned_fetch(req.url,
                                                 timeout_s=min(20.0, remaining))
    except Exception:
        route.abort()  # fail-closed: validation/pin/transport failure
        return
    fwd = {k: v for k, v in headers.items() if k in pin.FORWARDED_HEADERS}
    route.fulfill(status=status, headers=fwd, body=body)


def _browse_worker(token: str, url: str, extract_search: bool, conn) -> None:
    """Runs in a forked child. On ANY outcome (result, error, or hard kill by
    the parent watchdog) the parent is protected: this process holds the
    Playwright/CDP session only."""
    try:
        from playwright.sync_api import sync_playwright

        deadline = time.time() + PAGE_BUDGET_S
        live_token = token  # cleared after CDP connect (round-3 finding 3)
        dns_cache: dict[str, tuple[bool, str | None]] = {}
        checks = {"n": 0}

        def guarded(url_to_check: str) -> bool:
            """Deadline-aware, cached, count-capped destination check.
            Normalize once, canonical host is the cache key (round-4 finding 3)."""
            checks["n"] += 1
            if checks["n"] > MAX_ROUTE_CHECKS or time.time() > deadline:
                return False
            ok, reason, canonical = guard.check_url(url_to_check)
            if not ok or not canonical:
                return False
            import ipaddress as _ipa
            try:
                _ipa.ip_address(canonical)  # literal IP: already classified
                return True
            except ValueError:
                pass
            if canonical not in dns_cache:
                if len(dns_cache) >= 256:
                    return False
                dns_cache[canonical] = guard.resolve_host(canonical)
            return bool(dns_cache[canonical][0])

        result: dict = {"ok": False}
        remaining_ms = int((deadline - time.time()) * 1000)
        if remaining_ms <= 0:  # strict budget: never operate past deadline (finding 2)
            conn.send({"ok": False, "error": "budget-exceeded"})
            return

        def route_guard(route, _request=None):
            document_route(route, deadline)  # real, testable handler (finding 5)

        with sync_playwright() as p:
            browser = p.chromium.connect_over_cdp(
                f"{MANAGER_ORIGIN}/api/profiles/{PROFILE_ID}/cdp",
                headers={"Authorization": f"Bearer {live_token}"},
                timeout=min(30_000, remaining_ms))
            del live_token  # minimize in-memory credential lifetime (round-3 finding 3)
            try:
                ctx = browser.new_context()  # isolated context owned by us
                ctx.set_default_timeout(15_000)

                ctx.route("**/*", route_guard)
                try:
                    # Syntax-level check only (finding 2): the pinned fetch is
                    # the SOLE authoritative validation/connection operation.
                    ok, reason, canonical = guard.check_url(url)
                    if not ok or not canonical:
                        conn.send({"ok": False, "error": "destination-rejected",
                                   "detail": reason or "rejected"})
                        return
                    page = ctx.new_page()
                    try:
                        remaining_ms = int((deadline - time.time()) * 1000)
                        if remaining_ms <= 0:
                            conn.send({"ok": False, "error": "budget-exceeded"})
                            return
                        page.goto(url, timeout=min(GOTO_TIMEOUT_MS, remaining_ms),
                                  wait_until="domcontentloaded")
                        # Final-URL backstop: syntax/scheme-only check (the
                        # pinned fetch is the authoritative validation and
                        # connection; redirects were re-pinned per hop —
                        # no fresh DNS resolution here, per Hermes round-2).
                        ok_f, reason_f, _c = guard.check_url(page.url)
                        if not ok_f:
                            conn.send({"ok": False,
                                       "error": "final-destination-rejected"})
                            return
                        if extract_search:
                            result.update(_extract_results(page, guarded))
                        else:
                            result.update(_extract_page(page, guarded))
                        result["ok"] = True
                    finally:
                        page.close()
                finally:
                    ctx.close()
            finally:
                browser.close()
        conn.send(result)
    except Exception:
        try:
            conn.send({"ok": False, "error": "browse-failed"})  # fixed category
        except (BrokenPipeError, EOFError, OSError):
            pass  # parent gone; parent maps EOF to browse-failed (finding 4)
    finally:
        conn.close()


def _extract_page(page, guarded) -> dict:
    title = page.title()
    txt = page.evaluate("() => document.body ? document.body.innerText : ''")
    raw = page.evaluate(
        "() => Array.from(document.querySelectorAll('a[href]'))"
        ".slice(0, 200).map(a => ({t: (a.innerText||'').trim().slice(0,120),"
        " h: a.href}))")
    links = [l["h"] for l in raw if l.get("t") and guarded(l["h"])][:MAX_LINKS]
    return {"title": title[:200], "final_url": page.url,
            "body_chars": min(len(txt), MAX_BODY_CHARS),
            "head": [l.strip() for l in txt.splitlines() if l.strip()][:MAX_HEAD_LINES],
            "links": [{"url": u} for u in links]}


def _extract_results(page, guarded) -> dict:
    # DDG lite layout: <a class="result-link"> + adjacent snippet cell
    results = page.evaluate(
        "() => Array.from(document.querySelectorAll('a.result-link')).slice(0, 20)"
        ".map(a => {const row = a.closest('tr');"
        "const sn = row && row.nextElementSibling ?"
        " row.nextElementSibling.querySelector('.result-snippet') : null;"
        "return {title: (a.innerText||'').trim(), url: a.href,"
        " snippet: sn ? sn.innerText.trim().slice(0,300) : ''};})"
        ".filter(r => r.title)")
    urls = []
    for r in results:
        u = r.get("url", "")
        # unwrap DDG's /l/?uddg=<urlencoded> redirect wrapper to the real target
        if "duckduckgo.com/l/" in u and "uddg=" in u:
            from urllib.parse import urlsplit as _us, parse_qs, unquote
            try:
                u2 = unquote(parse_qs(_us(u).query).get("uddg", [""])[0])
                if u2:
                    u = u2
            except ValueError:
                pass
        if u and guarded(u):
            r["url"] = u
            urls.append(u)
    by_url = {r["url"]: r for r in results if r.get("url")}
    return {"results": [by_url[u] for u in urls][:MAX_RESULTS], "final_url": page.url}


class Browser:
    """Serialized browse execution: one worker PROCESS per request, hard-killed
    by the watchdog at PAGE_BUDGET_S (true wall-clock — sync Playwright cannot
    cancel, but a SIGKILLed process stops everything, CDP session included)."""

    def __init__(self, token: str) -> None:
        self.token = token
        self.lock = __import__("threading").Lock()

    def browse(self, url: str, extract_search: bool) -> dict:
        """Wall-clock contract: the child is HARD-KILLED at exactly
        PAGE_BUDGET_S (resolver slow-paths are covered by process-level
        enforcement, not per-call cancellation — documented residual).
        Pipe EOF / start failure / abnormal exit all map to fixed error
        categories (round-3 finding 1)."""
        with self.lock:
            parent_conn, child_conn = multiprocessing.Pipe(duplex=False)
            try:
                proc = multiprocessing.Process(
                    target=_browse_worker,
                    args=(self.token, url, extract_search, child_conn))
                proc.start()
            except Exception:
                child_conn.close()
                parent_conn.close()
                return {"ok": False, "error": "browse-failed"}
            finally:
                child_conn.close()
            try:
                if parent_conn.poll(PAGE_BUDGET_S):
                    try:
                        result = parent_conn.recv()
                    except EOFError:  # child died without sending (finding 1)
                        result = {"ok": False, "error": "browse-failed"}
                    proc.join(1)  # bounded teardown (finding 2)
                    if proc.is_alive():
                        proc.kill()
                        proc.join(5)
                    if not isinstance(result, dict) or "ok" not in result:
                        return {"ok": False, "error": "browse-failed"}
                    return result
                proc.kill()  # watchdog: hard wall-clock at exactly PAGE_BUDGET_S
                proc.join(5)
                return {"ok": False, "error": "budget-exceeded"}
            finally:
                parent_conn.close()
                if proc.is_alive():
                    proc.kill()
                    proc.join(5)


class Handler(socketserver.StreamRequestHandler):
    timeout = 30  # per-connection read timeout

    def _read_line_capped(self) -> bytes | None:
        """Deterministic 8 KiB cap: overlong or unterminated input → close
        (round-2 finding 6). Uses read1() — BufferedReader.read(n) BLOCKS for
        the full n bytes, which stalled every real request to its timeout
        (production-activation failure 2026-09-01)."""
        buf = bytearray()
        while len(buf) <= MAX_REQUEST_LINE:
            chunk = self.rfile.read1(4096)
            if not chunk:
                break
            buf.extend(chunk)
            if b"\n" in buf:
                line, _, rest = bytes(buf).partition(b"\n")
                if rest:  # more than one request on this connection
                    return None
                return line
        return None

    def handle(self) -> None:
        try:
            line = self._read_line_capped()
            if not line:
                resp = {"ok": False, "error": "bad-request"}
            else:
                req = json.loads(line.decode("utf-8"))
                resp = self.dispatch(req)
        except (ValueError, UnicodeDecodeError):
            resp = {"ok": False, "error": "bad-request"}
        except Exception:
            resp = {"ok": False, "error": "internal-error"}  # no exception text
        try:
            self.wfile.write(json.dumps(resp, ensure_ascii=False).encode() + b"\n")
        except OSError:
            pass

    def dispatch(self, req: dict) -> dict:
        if not isinstance(req, dict) or "op" not in req:
            return {"ok": False, "error": "bad-request"}
        op = req["op"]
        if op == "fetch":
            if set(req) != {"op", "url"} or not isinstance(req["url"], str):
                return {"ok": False, "error": "bad-request"}
            return BROWSER.browse(req["url"], extract_search=False)
        if op == "search":
            if set(req) != {"op", "query"} or not isinstance(req["query"], str):
                return {"ok": False, "error": "bad-request"}
            q = req["query"].strip()
            if not q or len(q) > MAX_QUERY_LEN:
                return {"ok": False, "error": "bad-request"}
            return BROWSER.browse(SEARCH_ENGINE + q.replace(" ", "+"),
                                  extract_search=True)
        return {"ok": False, "error": "op-not-allowed"}


class UnixServer(socketserver.ThreadingUnixStreamServer):
    """Socket-activation server: adopts systemd's inherited listening fd.
    v7 fix: TCPServer.__init__ requires (server_address, RequestHandlerClass);
    the previous call `UnixServer(fileno=fd)` passed the socket as the address
    with no handler → TypeError on every activation (the 2026-09-01 01:47
    failure). Construct unbound, discard the auto-created socket, adopt the passed one."""
    daemon_threads = True

    def __init__(self, sock: socket.socket) -> None:
        super().__init__(None, Handler, bind_and_activate=False)
        self.socket.close()                    # discard the auto-created one
        self.socket = sock                     # adopt systemd's listening socket


def adopt_socket_activation() -> socket.socket:
    """Strict systemd socket activation, fail-closed. Validates LISTEN_PID,
    exactly one fd, AF_UNIX listening stream; sets close-on-exec; cleans env."""
    if os.environ.get("LISTEN_PID") != str(os.getpid()):
        raise RuntimeError("not socket-activated (LISTEN_PID mismatch)")
    try:
        n = int(os.environ.get("LISTEN_FDS", ""))
    except ValueError:
        raise RuntimeError("invalid LISTEN_FDS")
    if n != 1:
        raise RuntimeError("expected exactly 1 activated fd")
    s = socket.socket(fileno=3)
    try:
        if (s.family != socket.AF_UNIX or
                (s.type & socket.SOCK_STREAM) != socket.SOCK_STREAM or
                not s.getsockopt(socket.SOL_SOCKET, socket.SO_ACCEPTCONN)):
            raise RuntimeError("activated fd is not a listening AF_UNIX stream")
        import fcntl
        fcntl.fcntl(3, fcntl.F_SETFD, fcntl.FD_CLOEXEC)
    except Exception:
        s.close()
        raise
    finally:
        os.environ.pop("LISTEN_FDS", None)
        os.environ.pop("LISTEN_PID", None)
    return s


def main() -> None:
    global BROWSER
    validate_origin(MANAGER_ORIGIN)
    BROWSER = Browser(read_token())
    server = UnixServer(adopt_socket_activation())
    server.serve_forever()


if __name__ == "__main__":
    main()
