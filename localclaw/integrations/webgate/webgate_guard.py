#!/usr/bin/env python3
"""webgate_guard.py — public-destination guard for dsh-webgate.

Pure-stdlib. Fail-closed. Policy = explicit maintained denylist of non-public
address space (IANA special-purpose registry, RFC 6890 families, as reviewed
2026-08; pinned as literal constants below, no dynamic updates) with
ipaddress flags as supplemental checks. Strict canonical hostname handling:
normalize ONCE, then classify/resolve the canonical value (round-2 finding 3);
IDNA encode + decode round-trip comparison (finding 4); 253-octet hostname cap
(finding 5); bracketed IPv6 literals handled before name normalization
(finding 13).
"""
from __future__ import annotations

import ipaddress
import socket
from urllib.parse import urlsplit

ALLOWED_SCHEMES = frozenset({"http", "https"})
MAX_URL_LEN = 2048
MAX_HOST_OCTETS = 253

# Pinned non-public denylist. Sources: IANA IPv4/IPv6 Special-Purpose Address
# Registry (reviewed 2026-08-31) + deployment-internal ranges. ipaddress-module
# flags are supplemental only (completeness comes from this list).
NON_PUBLIC_V4 = [
    ipaddress.ip_network("0.0.0.0/8"),          # "This network"          RFC 791
    ipaddress.ip_network("10.0.0.0/8"),         # private                 RFC 1918
    ipaddress.ip_network("100.64.0.0/10"),      # CGNAT / Tailscale tailnet
    ipaddress.ip_network("127.0.0.0/8"),        # loopback                RFC 1122
    ipaddress.ip_network("169.254.0.0/16"),     # link-local (metadata)   RFC 3927
    ipaddress.ip_network("172.16.0.0/12"),      # private                 RFC 1918
    ipaddress.ip_network("192.0.0.0/24"),       # IETF protocol assignments RFC 7335
    ipaddress.ip_network("192.0.2.0/24"),       # TEST-NET-1              RFC 5737
    ipaddress.ip_network("192.88.99.0/24"),     # 6to4 relay anycast (ret.) RFC 7526
    ipaddress.ip_network("192.168.0.0/16"),     # private                 RFC 1918
    ipaddress.ip_network("198.18.0.0/15"),      # benchmarking            RFC 2544
    ipaddress.ip_network("198.51.100.0/24"),    # TEST-NET-2              RFC 5737
    ipaddress.ip_network("203.0.113.0/24"),     # TEST-NET-3              RFC 5737
    ipaddress.ip_network("224.0.0.0/4"),        # multicast               RFC 1112
    ipaddress.ip_network("240.0.0.0/4"),        # reserved                RFC 1112
]
NON_PUBLIC_V6 = [
    ipaddress.ip_network("::/128"),             # unspecified             RFC 4291
    ipaddress.ip_network("::1/128"),            # loopback                RFC 4291
    ipaddress.ip_network("::ffff:0:0/96"),      # IPv4-mapped             RFC 4291
    ipaddress.ip_network("64:ff9b::/96"),       # NAT64                   RFC 6052
    ipaddress.ip_network("100::/64"),           # discard-only            RFC 6666
    ipaddress.ip_network("2001::/32"),          # Teredo                  RFC 4380
    ipaddress.ip_network("2001:2::/48"),        # benchmarking            RFC 5180
    ipaddress.ip_network("2001:db8::/32"),      # documentation           RFC 3849
    ipaddress.ip_network("2001:10::/28"),       # ORCHID (deprecated)     RFC 4843
    ipaddress.ip_network("fc00::/7"),           # ULA                     RFC 4193
    ipaddress.ip_network("fe80::/10"),          # link-local              RFC 4291
    ipaddress.ip_network("ff00::/8"),           # multicast               RFC 4291
]

_URL_BAD_CHARS = set(" \t\r\n\v\f\\") | {chr(c) for c in range(0x20)} | {chr(0x7f)}
_HOST_BAD_CHARS = _URL_BAD_CHARS | set("/%?#@[]:")


def classify_ip(ip: ipaddress._BaseAddress) -> str | None:
    """Rejection reason for a non-public address, else None. IPv4-mapped IPv6
    is unwrapped and classified as IPv4."""
    if isinstance(ip, ipaddress.IPv6Address) and ip.ipv4_mapped is not None:
        ip = ip.ipv4_mapped
    nets = NON_PUBLIC_V6 if ip.version == 6 else NON_PUBLIC_V4
    for net in nets:
        if ip in net:
            return f"non-public:{str(net)}"
    if ip.is_multicast or ip.is_reserved or ip.is_unspecified or ip.is_loopback \
            or ip.is_link_local or ip.is_private:
        return "non-public:ipaddress-flag"
    return None


def normalize_host(host: str) -> str | None:
    """Canonical hostname: reject control/whitespace/backslash/zone/parser
    chars; strip ONE trailing root dot; IDNA-encode non-ASCII with explicit
    decode round-trip comparison (IDNA2003 codec = documented policy choice);
    validate labels; enforce 253-octet ASCII cap. Returns canonical ASCII
    form or None."""
    if not host or any(c in _HOST_BAD_CHARS for c in host):
        return None
    if host.endswith("."):
        host = host[:-1]
    if not host:
        return None
    canonical = host
    if any(ord(c) > 127 for c in host):
        try:
            canonical = host.encode("idna").decode("ascii")
            # round-trip: re-decoding the ASCII form must reproduce the input
            if canonical.encode("ascii").decode("idna").rstrip(".") != \
                    host.rstrip(".").lower():
                return None
        except (UnicodeError, ValueError):
            return None
    canonical = canonical.lower()
    if len(canonical.encode("ascii", "ignore")) > MAX_HOST_OCTETS:
        return None
    for label in canonical.split("."):
        if not label or len(label) > 63 or label.startswith("-") or label.endswith("-"):
            return None
        if not all(c.isalnum() or c == "-" for c in label):
            return None
    return canonical


def check_url(url: str) -> tuple[bool, str | None, str | None]:
    """Scheme/authority-level validation. Returns (ok, reason, canonical_host).
    canonical_host is the normalized hostname (or the literal-IP string) that
    ALL downstream checks must use (round-2 finding 3)."""
    if not isinstance(url, str) or not url or len(url) > MAX_URL_LEN:
        return False, "invalid-url", None
    if any(c in url for c in _URL_BAD_CHARS):
        return False, "invalid-url", None
    try:
        parts = urlsplit(url)
    except ValueError:
        return False, "invalid-url", None
    if parts.scheme.lower() not in ALLOWED_SCHEMES:
        return False, "scheme-not-allowed", None
    if parts.username is not None or parts.password is not None:
        return False, "userinfo-in-url", None
    host = parts.hostname
    if not host:
        return False, "no-host", None
    if ":" in host:  # bracketed IPv6 literal — classify before name rules (finding 13)
        if "%" in host:
            return False, "scoped-address", None
        try:
            ip = ipaddress.ip_address(host)
        except ValueError:
            return False, "invalid-host", None
        reason = classify_ip(ip)
        return (False, reason, None) if reason else (True, None, str(ip))
    canonical = normalize_host(host)
    if canonical is None:
        return False, "invalid-host", None
    try:
        ip = ipaddress.ip_address(canonical)
    except ValueError:
        return True, None, canonical  # DNS name; resolved and classified later
    reason = classify_ip(ip)
    if reason:
        return False, reason, None
    return True, None, canonical


def resolve_host(canonical_host: str) -> tuple[bool, str | None]:
    """Resolve a CANONICAL hostname; classify EVERY returned address.
    Fail-closed on resolution failure / empty answer / any non-public address /
    any scoped result (zone ids rejected, never stripped)."""
    if not canonical_host or any(c in canonical_host for c in _HOST_BAD_CHARS):
        return False, "invalid-host"
    try:
        infos = socket.getaddrinfo(canonical_host, None, proto=socket.IPPROTO_TCP)
    except (socket.gaierror, OSError, UnicodeError):
        return False, "dns-resolution-failed"
    if not infos:
        return False, "dns-empty-answer"
    seen: set[str] = set()
    for info in infos:
        addr = info[4][0]
        if "%" in addr:
            return False, "scoped-address"
        if addr in seen:
            continue
        seen.add(addr)
        try:
            ip = ipaddress.ip_address(addr)
        except ValueError:
            return False, "unparseable-address"
        reason = classify_ip(ip)
        if reason:
            return False, reason
    return True, None


def validate_destination(url: str) -> tuple[bool, str | None]:
    """Full gate-side destination validation: parse + normalize once, then
    classify literal IPs or resolve the canonical DNS name."""
    ok, reason, canonical = check_url(url)
    if not ok:
        return False, reason
    if canonical is None:
        return False, "no-host"
    if any(c in canonical for c in ":"):  # literal IP, already classified
        return True, None
    return resolve_host(canonical)
