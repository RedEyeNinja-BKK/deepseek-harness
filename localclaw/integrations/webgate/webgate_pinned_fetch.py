#!/usr/bin/env python3
"""webgate_pinned_fetch.py — Option B connection pinning (minimum mechanism).

The DNS-rebinding closure: the GATE (worker process) performs the outbound
document fetch itself — one DNS resolution feeds BOTH the public-destination
validation AND the actual TCP connection; the connected peer is re-verified
against the validated address (getsockname pin). The browser never connects to
the destination: the gate hands it the response via Playwright route.fulfill().
A DNS answer that changes after validation cannot redirect the connection.

Scope (operator-authorized Option B boundary): document-fetch helper for
dsh-webgate ONLY. Not a proxy server, not shared, not a listener.

Single-resolution invariant: getaddrinfo is called EXACTLY ONCE per fetch;
validation, pin, and connection all use that one answer. TLS SNI + certificate
verification use the canonical hostname.
"""
from __future__ import annotations

import socket
import ssl
from urllib.parse import urlsplit

import webgate_guard as guard

MAX_BODY_BYTES = 5_000_000
_MAX_REDIRECTS_NA = None  # redirects are NOT followed here: Chromium re-requests
                          # each hop through the gate route guard (re-validated)
FORWARDED_HEADERS = ("content-type", "content-language", "retry-after")


class PinViolation(Exception):
    """Connected peer did not match the validated address."""


class FetchRejected(Exception):
    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


def _parse_response(buf: bytes) -> tuple[int, dict, bytes]:
    """Minimal HTTP/1.1 response parse (status, headers, body). Connection:
    close is always sent, so the body runs to EOF, capped by the caller."""
    head, sep, body = buf.partition(b"\r\n\r\n")
    if not sep:
        raise FetchRejected("malformed-response")
    lines = head.split(b"\r\n")
    try:
        status = int(lines[0].split()[1])
    except (IndexError, ValueError):
        raise FetchRejected("malformed-status")
    headers: dict[str, str] = {}
    for line in lines[1:]:
        if not line:
            continue
        if b":" not in line:  # malformed header syntax → reject (finding 3)
            raise FetchRejected("malformed-header")
        k, _, v = line.partition(b":")
        key = k.decode("latin-1").strip().lower()
        if key in headers:  # duplicate: first value wins (defined behavior)
            continue
        headers[key] = v.decode("latin-1").strip()
    return status, headers, body


def pinned_fetch(url: str, timeout_s: float) -> tuple[int, dict, bytes]:
    """Fetch a document with the connection pinned to a validated address.

    Returns (status, headers, body). Raises FetchRejected (fail-closed) on any
    validation/connection/pin/size failure.
    """
    # 1. URL-level + classification (literal IPs) / canonical name (DNS names).
    ok, reason, canonical = guard.check_url(url)
    if not ok or not canonical:
        raise FetchRejected(reason or "rejected")

    # 2. Literal-IP fast path: classification is final, NO DNS at all.
    try:
        literal = guard.ipaddress.ip_address(canonical)
    except ValueError:
        literal = None
    if literal is not None:
        if guard.classify_ip(literal) is not None:
            raise FetchRejected("non-public-address")
        addresses = [canonical]
    else:
        # 3. THE one resolution feeding validation AND connection (rebind closure).
        try:
            infos = socket.getaddrinfo(canonical, None, proto=socket.IPPROTO_TCP)
        except (socket.gaierror, OSError):
            raise FetchRejected("dns-resolution-failed")
        addresses = []
        for info in infos:
            addr = info[4][0]
            if "%" in addr:
                raise FetchRejected("scoped-address")
            ip = guard.ipaddress.ip_address(addr)
            if guard.classify_ip(ip) is not None:
                raise FetchRejected("non-public-address")
            if addr not in addresses:
                addresses.append(addr)
        if not addresses:
            raise FetchRejected("dns-empty-answer")


    parts = urlsplit(url)
    port = parts.port or (443 if parts.scheme == "https" else 80)
    host_header = canonical if parts.port is None else f"{canonical}:{parts.port}"
    path = parts.path or "/"
    if parts.query:
        path += "?" + parts.query

    last_err: Exception | None = None
    for addr in addresses:  # happy-eyeballs over the VALIDATED set only
        sock = None
        try:
            sock = socket.create_connection((addr, port), timeout=timeout_s)
            # 4. PIN: the connected peer must be the validated address.
            peer = sock.getpeername()[0]
            if peer != addr:
                raise FetchRejected("pin-violation")
            if parts.scheme == "https":
                ctx = ssl.create_default_context()
                with ctx.wrap_socket(sock, server_hostname=canonical) as tls:
                    return _exchange(tls, host_header, path)
            return _exchange(sock, host_header, path)
        except (FetchRejected, PinViolation):
            if sock is not None:
                sock.close()
            raise  # validation/pin failures are not retried on other addresses
        except OSError as exc:
            last_err = exc
            if sock is not None:
                sock.close()
    raise FetchRejected(f"connect-failed:{type(last_err).__name__ if last_err else 'unknown'}")


def _exchange(sock: socket.socket, host_header: str, path: str) -> tuple[int, dict, bytes]:
    try:
        req = (f"GET {path} HTTP/1.1\r\n"
               f"Host: {host_header}\r\n"
               "User-Agent: dsh-webgate/1.0\r\n"
               "Accept: text/html,text/plain,*/*;q=0.5\r\n"
               "Accept-Encoding: identity\r\n"
               "Connection: close\r\n\r\n")
        sock.sendall(req.encode("latin-1"))
        buf = bytearray()
        while len(buf) <= MAX_BODY_BYTES:
            chunk = sock.recv(65536)
            if not chunk:
                break
            buf.extend(chunk)
        if len(buf) > MAX_BODY_BYTES:
            raise FetchRejected("too-large")
        status, headers, body = _parse_response(bytes(buf))
        # Minimal chunked transfer-coding decode (Connection: close; identity AE).
        if headers.get("transfer-encoding", "").lower() == "chunked":
            out = bytearray()
            rest = body
            while True:
                line, _, rest = rest.partition(b"\r\n")
                try:
                    size = int(line.split(b";")[0].strip() or b"0", 16)
                except ValueError:
                    raise FetchRejected("malformed-chunk")
                if size == 0:
                    break  # terminating chunk (trailer section ignored)
                if len(rest) < size + 2 or rest[size:size + 2] != b"\r\n":
                    raise FetchRejected("malformed-chunk")  # truncated/undelimited
                out.extend(rest[:size])
                rest = rest[size + 2:]
                if len(out) > MAX_BODY_BYTES:
                    raise FetchRejected("too-large")
            body = bytes(out)
        return status, headers, body[:MAX_BODY_BYTES]
    except FetchRejected:
        raise
    except (OSError, ssl.SSLError, ValueError) as exc:
        raise FetchRejected(f"exchange-failed:{type(exc).__name__}")
