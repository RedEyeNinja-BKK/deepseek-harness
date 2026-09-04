# DSH LINE INTEGRATION — DEPLOYMENT & OPERATOR PACK (2026-09-03)

**Status: STAGED — awaiting Hermes review verdict + ONE operator action pack.**
Architecture mirrors the proven Discord inbound lane (dsh_discord_inbound.py,
deployed + accepted 2026-09-02): platform listener → DSH session RPC → reply.

## Architecture

```
Family LINE user
  → LINE platform (Messaging API webhook, HTTPS)
  → Tailscale Funnel (public https://<funnel-host>/ → 127.0.0.1:3087)
  → dsh-line-inbound.service (signature-verified webhook listener, stdlib-only)
      → dedupe (message.id, persisted)
      → conversation map (user/group → DSH session, persisted)
      → session.create / session.prompt (queue mode, 127.0.0.1:3080)
      → bounded poll session.list (turn end) → session.history (final turn text)
  → LINE Push API (POST /v2/bot/message/push, X-Line-Retry-Key idempotent)
  → family member
```

- DSH (`switchyard-smart-agentic-dsh`) owns conversation/reasoning/tools.
- The adapter is transport-only. No DSH core patch, no event bus, no new agent.
- Turnstone is supervisor (journal + receipt visibility), never inline.

## Staged artifacts (all in /opt/dsh-line/, owner vincent)

| File | Purpose | SHA256 (adapter) |
|---|---|---|
| `dsh_line_inbound.py` | webhook listener + DSH bridge + push reply | `320248a8c7f9a5c5fe0d5211210f2e00f5b650874cfd6d9c01f3b489a15604d7` (v6, Hermes APPROVE run_e7bee50dddac4442872559b51fb35c34 after 6 review rounds: REJECT→REJECT→REJECT→AWF→AWF→APPROVE) |
| `dsh-line-inbound.service` | hardened systemd unit (staged; operator installs) | — |

Local validation already done:
- `--selftest`: 40/40 PASS (signature roundtrip/tamper/missing, chunking
  boundaries, marker extraction incl. ambiguity + splice immunity, trust
  gates, profile paths, pending CAS, claim/finish reservation, stale-claim
  recovery, persistence-failure fail-closed, malformed-state normalization).
- Live smoke: health 200, unsigned 403, signed 200, query rejected, 413
  oversized, duplicate-Content-Length 400, state persistence live.
- SIGTERM: clean bounded drain + durable abandonment record, 3/3 runs,
  0.5s exits. (Found + fixed during smoke: instance-attr shadowing of the
  class-level queue counter — the drain-loop hang root cause.)

## Design decisions (evidence)

1. **Push API, not Reply API.** replyToken validity is seconds; DSH turns take
   10s–minutes. Push (`to` = userId/groupId, ≤5 messages, ≤5000 chars each —
   we chunk at 4500) with `X-Line-Retry-Key` = uuid5(message.id) is
   redelivery-safe and latency-independent (line-openapi messaging-api.yml).
2. **Group chats: mention-gated.** 1:1 user chats always dispatch; group/room
   only when `mentionees[].isSelf` or `type == "all"`.
3. **Media v1 = not implemented.** Non-text messages produce an envelope note
   "(sent a <type> — media handling not enabled yet)" so DSH can acknowledge
   honestly. Real media fetch (`api-data.line.me` content endpoint) is a
   scoped follow-up, not first-deploy scope.
4. **Stdlib only.** No new runtime dependency; signature verification is
   6 lines of hmac/base64 (LINE docs method, fetched + verified 2026-09-03).
5. **Poll design.** `session.history` does not bound its response (measured:
   limit param ignored, 9,278-event stream returned in 0.05s), so we poll the
   cheap `session.list` (running flag + updatedAt) and fetch history once at
   turn end. Bounded at 240s; timeout → evidence logged, NO automatic retry
   (no double-reply risk; reply preserved in session).
6. **Identity reuse.** Runs as `dsh-discord` user (same platform-listener
   trust class as the Discord lane). Dedicated user = optional operator
   hardening, not required.

## OPERATOR PACK (ONE action — all root/Console steps in one sitting)

### A. LINE Developers Console (https://developers.line.biz/)
1. Log in with the family LINE account → create a **Provider** (e.g. "Family").
2. Create a **Messaging API** channel.
3. Basic settings tab: copy the **Channel secret**.
4. Messaging API tab: **issue** a Channel access token (long-lived). Copy it.
5. Messaging API tab settings: **disable** auto-reply, **enable** webhook.
   (Greeting message: your choice.)

### B. Stage credentials (root, values never pass through chat)
```bash
sudo install -d -m 0700 /root/dsh-line
sudo nano /root/dsh-line/channel-secret        # paste Channel secret
sudo nano /root/dsh-line/channel-access-token  # paste Channel access token
sudo chmod 600 /root/dsh-line/channel-secret /root/dsh-line/channel-access-token
sudo chown root:root /root/dsh-line/channel-secret /root/dsh-line/channel-access-token
```

### C. Install + start service
```bash
sudo install -o root -g root -m 0644 /opt/dsh-line/dsh-line-inbound.service /etc/systemd/system/dsh-line-inbound.service
sudo systemctl daemon-reload
sudo systemctl enable --now dsh-line-inbound.service
```

### D. Public webhook path (Tailscale Funnel)
```bash
sudo tailscale funnel --bg 3087
```
If it fails with a Funnel capability/policy error: in the tailnet admin
console, add to the ACL policy: `"nodeAttrs": [{"target": ["*"], "attr": ["funnel"]}]`
then retry.

### E. Register + verify the webhook (token stays in the file, never in chat)
```bash
curl -s -X PATCH https://api.line.me/v2/bot/channel/webhook/endpoint \
  -H "Authorization: Bearer $(sudo cat /root/dsh-line/channel-access-token)" \
  -H "Content-Type: application/json" \
  -d '{"endpoint":"https://<funnel-host>/line/webhook"}'

curl -s -X POST https://api.line.me/v2/bot/channel/webhook/test \
  -H "Authorization: Bearer $(sudo cat /root/dsh-line/channel-access-token)"
```
Expected: `{"success":true...}` for the PATCH; the test POST returns a
delivery result — our listener returns 200 for signature-valid test events.

### F. First message
Add the bot as a friend (QR code in the Console, Messaging API tab) from the
family member's LINE account and send any Thai message.

## Turnstone verification after the pack (mine, no operator needed)
- `ss -tln | grep 3087`, `GET /line/health` → 200.
- `tailscale funnel status` shows the public mapping.
- Funnel reachability probe of /line/health from outside.
- LINE webhook/test result (from step E output).
- Real family message → journal + DSH session.list/history evidence →
  push reply observed by the family member (acceptance).

## Rollback
```bash
sudo systemctl disable --now dsh-line-inbound.service
sudo tailscale funnel --bg reset   # or: tailscale funnel --bg 3087=off
# set webhook endpoint back to a placeholder via the PATCH call if desired
```
State (session map) persists in /var/lib/dsh-line-inbound; nothing in DSH
changes. No shared surface with Discord/webgate/gold lanes.

## Open notes
- Free-tier push quota applies (family volume is tiny); check Console usage
  if replies ever stop.
- Channel access token long-lived; rotation = reissue + overwrite the staged
  file + `systemctl restart dsh-line-inbound.service` (operator step).
