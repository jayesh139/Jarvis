"""
Slack for JARVIS — send / read / list via Web API, plus live notifications
via Socket Mode (no public webhook URL required).

Setup (once):
  1. Create an app at https://api.slack.com/apps → From scratch
  2. OAuth & Permissions → Bot Token Scopes:
       channels:read, channels:history, groups:read, groups:history,
       chat:write, im:read, im:write, im:history,
       users:read, mpim:read, mpim:write, mpim:history
  3. Install to Workspace → copy Bot User OAuth Token (xoxb-…)
  4. Basic Information → App-Level Tokens → Generate Token
       with scope connections:write → copy (xapp-…)
  5. Socket Mode → Enable Socket Mode
  6. Event Subscriptions → Enable Events → Subscribe to bot events:
       message.channels, message.groups, message.im, message.mpim
  7. Reinstall the app if Slack asks
  8. config/api_keys.json:
       "slack_bot_token": "xoxb-…",
       "slack_app_token": "xapp-…"
  9. Invite the bot: /invite @YourBot
 10. Restart JARVIS — new messages are spoken/alerted automatically

No macOS Accessibility needed for API messaging — that is only for click-typing.
"""
from __future__ import annotations

import asyncio
import json
import re
import time
from pathlib import Path
from typing import Any, Callable

import requests

BASE_DIR = Path(__file__).resolve().parent.parent
CONFIG_FILE = BASE_DIR / "config" / "api_keys.json"
DIR_CACHE_FILE = BASE_DIR / "config" / "slack_directory_cache.json"
API = "https://slack.com/api"

# Cache channel / user lookups for the process lifetime (names rarely change).
_channel_cache: dict[str, str] = {}  # lower name -> id
_user_cache: dict[str, str] = {}     # lower display/real/name -> id
_id_to_channel: dict[str, str] = {}  # channel id -> #name
_id_to_user: dict[str, str] = {}     # user id -> display name
_cache_ts = 0.0
_CACHE_TTL = 300.0
_dir_cache_loaded = False
_last_users_error: str = ""

# Live notify: shared queue drained by main.py
_notify_queue: asyncio.Queue | None = None
_bot_user_id: str = ""
_seen_events: set[str] = set()
_socket_connected: bool = False
_rate_limited_until: float = 0.0
_last_api_call: float = 0.0
_MIN_API_GAP = 1.05  # seconds — stay under Slack Tier-ish burst limits
_SKIP_SUBTYPES = {
    "bot_message", "message_changed", "message_deleted", "channel_join",
    "channel_leave", "channel_topic", "channel_purpose", "channel_name",
    "channel_archive", "channel_unarchive", "group_join", "group_leave",
    "message_replied",  # parent thread metadata; replies still arrive as message
}


# ── config ───────────────────────────────────────────────────────────────────

def _load_cfg() -> dict:
    try:
        return json.loads(CONFIG_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _patch_cfg(**fields: Any) -> None:
    data = _load_cfg()
    data.update(fields)
    CONFIG_FILE.parent.mkdir(parents=True, exist_ok=True)
    CONFIG_FILE.write_text(json.dumps(data, indent=4), encoding="utf-8")


def _token() -> str:
    cfg = _load_cfg()
    return (cfg.get("slack_bot_token") or cfg.get("slack_token") or "").strip()


def _app_token() -> str:
    cfg = _load_cfg()
    return (cfg.get("slack_app_token") or "").strip()


def is_configured() -> bool:
    """True when a Slack bot token is present in api_keys.json."""
    tok = _token()
    return bool(tok and (tok.startswith("xoxb-") or tok.startswith("xoxp-")))


def notify_ready() -> bool:
    """Bot + app-level tokens present and notify not explicitly disabled."""
    cfg = _load_cfg()
    if cfg.get("slack_notify") is False:
        return False
    bot = _token()
    app = _app_token()
    return bool(
        bot.startswith("xoxb-") or bot.startswith("xoxp-")
    ) and bool(app.startswith("xapp-"))


def _setup_help() -> str:
    return (
        "Slack is not connected yet. Create a Slack app at api.slack.com/apps, "
        "add bot scopes (channels:read, channels:history, groups:read, "
        "groups:history, chat:write, im:read, im:write, im:history, users:read), "
        "Install to Workspace, then put the Bot token in config/api_keys.json as "
        '"slack_bot_token": "xoxb-…". For live alerts also add slack_app_token '
        '(xapp-… with connections:write), enable Socket Mode, and subscribe to '
        "message.channels / message.groups / message.im / message.mpim. "
        "Invite the bot with /invite @BotName."
    )


def _notify_help() -> str:
    return (
        "Live Slack alerts need an App-Level Token. In api.slack.com/apps → your "
        "app → Basic Information → App-Level Tokens, generate one with scope "
        "connections:write (starts with xapp-). Enable Socket Mode, subscribe to "
        "bot events message.channels, message.groups, message.im, message.mpim, "
        'then add "slack_app_token": "xapp-…" to config/api_keys.json and restart.'
    )


# ── HTTP ─────────────────────────────────────────────────────────────────────

def _respect_rate_limit() -> None:
    """Global client-side spacing so we stop stampeding Slack into HTTP 429."""
    global _last_api_call, _rate_limited_until
    now = time.time()
    if now < _rate_limited_until:
        time.sleep(_rate_limited_until - now)
        now = time.time()
    gap = now - _last_api_call
    if gap < _MIN_API_GAP:
        time.sleep(_MIN_API_GAP - gap)
    _last_api_call = time.time()


def _mark_rate_limited(retry_after: float | None = None) -> None:
    global _rate_limited_until
    wait = 30.0
    if retry_after is not None:
        try:
            wait = max(5.0, float(retry_after))
        except Exception:
            wait = 30.0
    _rate_limited_until = max(_rate_limited_until, time.time() + wait)
    print(f"[Slack] rate-limited — cooling down {wait:.0f}s", flush=True)


def _api(method: str, token: str, **params: Any) -> dict:
    """Call a Slack Web API method. Returns the JSON body (always a dict).

    Never raises on HTTP 429 — returns {"ok": False, "error": "ratelimited"}.
    """
    write = method.startswith("chat.") or method in (
        "conversations.open", "conversations.join",
    )
    headers = {"Authorization": f"Bearer {token}"}
    _respect_rate_limit()
    try:
        if write:
            headers["Content-Type"] = "application/x-www-form-urlencoded"
            r = requests.post(
                f"{API}/{method}",
                headers=headers,
                data={k: v for k, v in params.items() if v is not None},
                timeout=20,
            )
        else:
            r = requests.get(
                f"{API}/{method}",
                headers=headers,
                params={k: v for k, v in params.items() if v is not None},
                timeout=20,
            )
    except requests.RequestException as e:
        return {"ok": False, "error": f"network:{e}"}

    if r.status_code == 429:
        _mark_rate_limited(r.headers.get("Retry-After"))
        return {"ok": False, "error": "ratelimited"}
    if r.status_code >= 400:
        # Prefer Slack's JSON error when present; don't raise and blow the tool.
        try:
            data = r.json()
            if isinstance(data, dict):
                data.setdefault("ok", False)
                data.setdefault("error", f"http_{r.status_code}")
                return data
        except Exception:
            pass
        return {"ok": False, "error": f"http_{r.status_code}"}

    data = r.json()
    if not isinstance(data, dict):
        return {"ok": False, "error": "bad_response"}
    # Slack also rate-limits with 200 + ok:false error:ratelimited
    if data.get("error") in ("ratelimited", "rate_limited"):
        _mark_rate_limited()
    return data


def _auth_test(token: str) -> dict:
    return _api("auth.test", token)


def _friendly_api_error(err: str) -> str:
    if err in ("ratelimited", "rate_limited"):
        left = max(0, int(_rate_limited_until - time.time()))
        return (
            f"Slack is rate-limiting us right now. Wait about {left or 30} seconds "
            f"and try again — too many API calls were made in a short burst."
        )
    return f"Slack error ({err})."


# ── resolve people & channels ────────────────────────────────────────────────

def _load_dir_cache() -> None:
    """Hydrate name maps from disk so alerts work even while Slack is rate-limiting."""
    global _dir_cache_loaded, _cache_ts
    if _dir_cache_loaded:
        return
    _dir_cache_loaded = True
    try:
        data = json.loads(DIR_CACHE_FILE.read_text(encoding="utf-8"))
    except Exception:
        return
    if not isinstance(data, dict):
        return
    _channel_cache.update({str(k).lower(): str(v) for k, v in (data.get("channels") or {}).items()})
    _user_cache.update({str(k).lower(): str(v) for k, v in (data.get("users") or {}).items()})
    _id_to_channel.update({str(k): str(v) for k, v in (data.get("id_to_channel") or {}).items()})
    _id_to_user.update({str(k): str(v) for k, v in (data.get("id_to_user") or {}).items()})
    try:
        _cache_ts = float(data.get("ts") or 0)
    except Exception:
        _cache_ts = 0.0


def _save_dir_cache() -> None:
    try:
        DIR_CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "ts": _cache_ts,
            "channels": _channel_cache,
            "users": _user_cache,
            "id_to_channel": _id_to_channel,
            "id_to_user": _id_to_user,
        }
        DIR_CACHE_FILE.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    except Exception as e:
        print(f"[Slack] could not save directory cache: {e}", flush=True)


def _remember_user(uid: str, name: str) -> None:
    name = (name or "").strip()
    if not uid or not name or name.lower() == "someone":
        return
    _id_to_user[uid] = name
    low = name.lower()
    _user_cache[low] = uid
    _user_cache[f"@{low}"] = uid
    for part in re.split(r"[\s._-]+", low):
        if len(part) >= 3:
            _user_cache.setdefault(part, uid)


def _refresh_caches(token: str, force: bool = False) -> None:
    global _cache_ts
    _load_dir_cache()
    now = time.time()
    if not force and _channel_cache and _user_cache and (now - _cache_ts) < _CACHE_TTL:
        return
    if time.time() < _rate_limited_until and not force:
        return

    # Conversations the bot can see (public + private).
    cursor = None
    channels: dict[str, str] = {}
    id_ch: dict[str, str] = {}
    while True:
        data = _api(
            "conversations.list",
            token,
            types="public_channel,private_channel",
            exclude_archived="true",
            limit=200,
            cursor=cursor or "",
        )
        if not data.get("ok"):
            if data.get("error") in ("ratelimited", "rate_limited"):
                return  # keep prior cache; try later
            break
        for ch in data.get("channels") or []:
            name = (ch.get("name") or "").strip().lower()
            cid = ch.get("id") or ""
            if name and cid:
                channels[name] = cid
                channels[f"#{name}"] = cid
                id_ch[cid] = name
        cursor = (data.get("response_metadata") or {}).get("next_cursor") or ""
        if not cursor:
            break

    # Workspace users — match by display name, real name, or @handle.
    global _last_users_error
    cursor = None
    users: dict[str, str] = {}
    id_u: dict[str, str] = {}
    while True:
        data = _api("users.list", token, limit=200, cursor=cursor or "")
        if not data.get("ok"):
            err = data.get("error") or "unknown"
            if err in ("ratelimited", "rate_limited"):
                return
            if err == "missing_scope":
                needed = data.get("needed") or "users:read"
                _last_users_error = f"missing_scope:{needed}"
            else:
                _last_users_error = err
            break
        _last_users_error = ""
        for u in data.get("members") or []:
            if u.get("deleted") or u.get("is_bot") or u.get("id") == "USLACKBOT":
                continue
            uid = u.get("id") or ""
            if not uid:
                continue
            profile = u.get("profile") or {}
            display = (
                (profile.get("display_name") or "").strip()
                or (profile.get("real_name") or "").strip()
                or (u.get("real_name") or "").strip()
                or (u.get("name") or "").strip()
            )
            names = {
                (u.get("name") or "").strip().lower(),
                (profile.get("display_name") or "").strip().lower(),
                (profile.get("real_name") or "").strip().lower(),
                (u.get("real_name") or "").strip().lower(),
            }
            for n in names:
                if n:
                    users[n] = uid
                    users[f"@{n}"] = uid
            if display:
                id_u[uid] = display
        cursor = (data.get("response_metadata") or {}).get("next_cursor") or ""
        if not cursor:
            break

    if channels:
        _channel_cache.clear()
        _channel_cache.update(channels)
        _id_to_channel.clear()
        _id_to_channel.update(id_ch)
    if users:
        _user_cache.clear()
        _user_cache.update(users)
        _id_to_user.clear()
        _id_to_user.update(id_u)
    if channels or users:
        _cache_ts = now
        _save_dir_cache()


def _looks_like_id(value: str) -> bool:
    return bool(re.fullmatch(r"[CDGUVW][A-Z0-9]{8,}", value.strip().upper()))


def _match_user(query: str) -> tuple[str | None, str | None]:
    """Best-effort match against the in-memory user cache. Returns (uid, label)."""
    q = query.lower().lstrip("@").strip()
    if not q:
        return None, None
    # Exact keys first
    for key in (q, f"@{q}"):
        if key in _user_cache:
            return _user_cache[key], f"@{q}"
    # Exact display/handle equality
    for name, uid in _user_cache.items():
        n = name.lstrip("@")
        if n == q:
            return uid, f"@{n}"
    # First-name / substring: "avani" matches "avani sharma"
    hits: list[tuple[str, str]] = []
    for name, uid in _user_cache.items():
        n = name.lstrip("@")
        parts = n.replace(".", " ").replace("_", " ").split()
        if q in n or any(p == q or p.startswith(q) for p in parts):
            hits.append((uid, n))
    # Dedupe by uid, prefer shorter display names
    uniq: dict[str, str] = {}
    for uid, n in hits:
        if uid not in uniq or len(n) < len(uniq[uid]):
            uniq[uid] = n
    if len(uniq) == 1:
        uid, n = next(iter(uniq.items()))
        return uid, f"@{n}"
    if len(uniq) > 1:
        # Ambiguous — still return None with a hint via label channel
        names = ", ".join(sorted(uniq.values())[:5])
        return None, f"ambiguous:{names}"
    return None, None


def _resolve_target(token: str, raw: str) -> tuple[str | None, str]:
    """Return (channel_id_or_user_id, label) or (None, error)."""
    target = (raw or "").strip()
    if not target:
        return None, "Who should I message on Slack?"

    if _looks_like_id(target):
        return target.upper(), target

    _load_dir_cache()
    # If caches are empty (common right after a rate-limit storm), force a refresh.
    if not _user_cache or not _channel_cache:
        if time.time() < _rate_limited_until:
            return None, (
                f"I can't look up '{target}' yet — Slack is rate-limiting directory "
                f"reads. Wait a minute, then say 'list Slack people' and try again."
            )
        _refresh_caches(token, force=True)
    else:
        _refresh_caches(token)

    key = target.lower().lstrip()
    # Channel forms: #eng, eng, channel eng
    ch_key = key
    if ch_key.startswith("channel "):
        ch_key = ch_key[8:].strip()
    if ch_key in _channel_cache:
        name = ch_key.lstrip("#")
        return _channel_cache[ch_key], f"#{name}"

    bare = ch_key.lstrip("#")
    for name, cid in _channel_cache.items():
        if name.lstrip("#") == bare:
            return cid, f"#{name.lstrip('#')}"
    for name, cid in _channel_cache.items():
        n = name.lstrip("#")
        if bare in n or n in bare:
            return cid, f"#{n}"

    # User forms: @jayesh, jayesh, dm jayesh
    u_key = key
    if u_key.startswith("dm "):
        u_key = u_key[3:].strip()
    if u_key.startswith("to "):
        u_key = u_key[3:].strip()

    uid, label = _match_user(u_key)
    if uid:
        return uid, label or f"@{u_key.lstrip('@')}"
    if label and label.startswith("ambiguous:"):
        names = label.split(":", 1)[1]
        return None, (
            f"Several Slack people match '{target}': {names}. "
            f"Say the fuller name so I can tell them apart."
        )

    # Last chance: force one more directory pull if we still have no users.
    if not _user_cache:
        _refresh_caches(token, force=True)
        uid, label = _match_user(u_key)
        if uid:
            return uid, label or f"@{u_key.lstrip('@')}"
        if time.time() < _rate_limited_until:
            return None, (
                f"I can't look up '{target}' yet — Slack is still rate-limiting "
                f"directory reads. Wait a minute, say 'list Slack people', then try again."
            )

    hint = ""
    if _user_cache:
        sample = sorted({n.lstrip('@') for n in _user_cache if not n.startswith('@')})[:8]
        if sample:
            hint = f" People I can see include: {', '.join(sample)}."
    return None, (
        f"I could not find Slack channel or person '{target}'. "
        f"Try #channel-name or the person's Slack display name, and invite the bot "
        f"to private channels.{hint}"
    )


def _open_dm(token: str, user_id: str) -> tuple[str | None, str]:
    data = _api("conversations.open", token, users=user_id)
    if not data.get("ok"):
        return None, f"Could not open a DM ({data.get('error', 'unknown')})."
    ch = (data.get("channel") or {}).get("id")
    if not ch:
        return None, "Slack did not return a DM channel id."
    return ch, ""


# ── actions ──────────────────────────────────────────────────────────────────

def send_via_slack(receiver: str, message: str) -> str:
    """Used by send_message when platform=slack, and by the slack tool."""
    token = _token()
    if not token:
        return _setup_help()
    if not message.strip():
        return "What should I say on Slack?"

    dest, label_or_err = _resolve_target(token, receiver)
    if not dest:
        return label_or_err

    channel = dest
    label = label_or_err
    # User IDs need conversations.open first.
    if dest.startswith("U") or dest.startswith("W"):
        channel, err = _open_dm(token, dest)
        if err or not channel:
            return err or "Could not open DM."

    data = _api("chat.postMessage", token, channel=channel, text=message)
    if not data.get("ok"):
        err = data.get("error", "unknown")
        if err in ("ratelimited", "rate_limited"):
            return _friendly_api_error(err)
        if err == "not_in_channel":
            # Try joining public channels, then retry once.
            join = _api("conversations.join", token, channel=channel)
            if join.get("ok"):
                data = _api("chat.postMessage", token, channel=channel, text=message)
                if data.get("ok"):
                    return f"Sent on Slack to {label}."
            return (
                f"Bot is not in {label}. Invite it with /invite @Bot in that "
                f"channel, then ask again."
            )
        return f"Slack send failed ({err})."
    return f"Sent on Slack to {label}."


def _list_channels(token: str, limit: int = 25) -> str:
    _load_dir_cache()
    if time.time() >= _rate_limited_until:
        _refresh_caches(token, force=not _channel_cache)
    elif not _channel_cache:
        return _friendly_api_error("ratelimited")
    names = sorted({n.lstrip("#") for n in _channel_cache if not n.startswith("#")})
    if not names:
        if time.time() < _rate_limited_until:
            return _friendly_api_error("ratelimited")
        return "I cannot see any Slack channels yet. Check the bot scopes and reinstall."
    show = names[: max(1, min(limit, 50))]
    extra = len(names) - len(show)
    body = ", ".join(f"#{n}" for n in show)
    if extra > 0:
        body += f" (+{extra} more)"
    cached = " (from cache — Slack is cooling down)" if time.time() < _rate_limited_until else ""
    return f"Slack channels I can see{cached}: {body}."


def _list_people(token: str, limit: int = 25) -> str:
    _load_dir_cache()
    if time.time() >= _rate_limited_until:
        _refresh_caches(token, force=not _user_cache)
    elif not _user_cache:
        return _friendly_api_error("ratelimited")
    # Prefer real display names (with spaces) first, then handles.
    display = sorted({n.lstrip("@") for n in _user_cache if not n.startswith("@") and " " in n})
    handles = sorted({n.lstrip("@") for n in _user_cache if not n.startswith("@") and " " not in n})
    first_names = {d.split()[0].lower() for d in display}
    names = display + [h for h in handles if h.lower() not in first_names]
    if not names:
        names = sorted({n.lstrip("@") for n in _user_cache if not n.startswith("@")})
    if not names:
        if time.time() < _rate_limited_until:
            return _friendly_api_error("ratelimited")
        if _last_users_error.startswith("missing_scope"):
            needed = _last_users_error.split(":", 1)[-1]
            return (
                f"Slack blocked the people list — missing bot scope '{needed}'. "
                f"In api.slack.com/apps → your Jarvis app → OAuth & Permissions → "
                f"Bot Token Scopes, add '{needed}', then click Reinstall to Workspace "
                f"and try 'list Slack people' again."
            )
        if _last_users_error:
            return f"I cannot see workspace members ({_last_users_error})."
        return (
            "I cannot see workspace members. Add the bot scope 'users:read' in "
            "OAuth & Permissions, reinstall the app, then try again."
        )
    show = names[: max(1, min(limit, 50))]
    extra = len(names) - len(show)
    body = ", ".join(show)
    if extra > 0:
        body += f" (+{extra} more)"
    cached = " (from cache — Slack is cooling down)" if time.time() < _rate_limited_until else ""
    return f"Slack people I can message{cached}: {body}."


def _read_history(token: str, target: str, limit: int = 8) -> str:
    dest, label_or_err = _resolve_target(token, target)
    if not dest:
        return label_or_err
    channel = dest
    label = label_or_err
    if dest.startswith("U") or dest.startswith("W"):
        channel, err = _open_dm(token, dest)
        if err or not channel:
            return err or "Could not open DM."

    data = _api(
        "conversations.history",
        token,
        channel=channel,
        limit=max(1, min(int(limit or 8), 20)),
    )
    if not data.get("ok"):
        err = data.get("error", "unknown")
        if err in ("ratelimited", "rate_limited"):
            return _friendly_api_error(err)
        if err in ("not_in_channel", "channel_not_found"):
            return (
                f"I cannot read {label}. Invite the bot into that channel "
                f"(/invite @Bot) and ensure history scopes are installed."
            )
        return f"Could not read Slack history ({err})."

    messages = list(reversed(data.get("messages") or []))
    if not messages:
        return f"No recent messages in {label}."

    _refresh_caches(token)
    id_to_name = {uid: name.lstrip("@") for name, uid in _user_cache.items()}
    lines = []
    for m in messages:
        if m.get("subtype") in ("channel_join", "channel_leave", "bot_add"):
            continue
        text = (m.get("text") or "").strip()
        if not text:
            continue
        uid = m.get("user") or ""
        who = id_to_name.get(uid) or (uid or "someone")
        # Soft-trim very long messages for speech.
        if len(text) > 280:
            text = text[:277] + "…"
        lines.append(f"{who}: {text}")
    if not lines:
        return f"No readable text messages in {label}."
    return f"Recent Slack in {label}:\n" + "\n".join(lines)


def _status(token: str) -> str:
    global _bot_user_id
    data = _auth_test(token)
    if not data.get("ok"):
        err = data.get("error", "unknown")
        if err in ("ratelimited", "rate_limited"):
            return _friendly_api_error(err)
        return f"Slack token looks invalid ({err}). {_setup_help()}"
    team = data.get("team") or "workspace"
    user = data.get("user") or "bot"
    _bot_user_id = data.get("user_id") or _bot_user_id
    cfg = _load_cfg()
    if cfg.get("slack_notify") is False:
        notify = "live alerts paused (say 'enable Slack notifications')"
    elif _socket_connected:
        notify = "live alerts CONNECTED and listening"
    elif notify_ready():
        notify = (
            "live alert tokens present, but the socket is not connected yet "
            "(often Slack rate-limit — wait a minute and restart Jarvis)"
        )
    else:
        notify = "live alerts OFF — add slack_app_token (xapp-…) and enable Socket Mode"
    return f"Slack connected as @{user} in {team}; {notify}."


def _set_notify(enabled: bool) -> str:
    _patch_cfg(slack_notify=bool(enabled))
    if enabled and not _app_token().startswith("xapp-"):
        return (
            f"Slack notifications enabled in config, but {_notify_help()}"
        )
    return (
        "Slack live notifications enabled."
        if enabled else
        "Slack live notifications paused."
    )


# ── Socket Mode live alerts ──────────────────────────────────────────────────

def _ensure_bot_id(token: str) -> str:
    global _bot_user_id
    if _bot_user_id:
        return _bot_user_id
    data = _auth_test(token)
    if data.get("ok"):
        _bot_user_id = data.get("user_id") or ""
    return _bot_user_id


def _channel_label(token: str, channel_id: str) -> str:
    if not channel_id:
        return "Slack"
    if channel_id in _id_to_channel:
        return f"#{_id_to_channel[channel_id]}"
    # DM / MPIM ids often start with D / G
    if channel_id.startswith("D"):
        return "a Slack DM"
    # Avoid API storms while rate-limited — name lookup can wait.
    if time.time() < _rate_limited_until:
        return "Slack"
    try:
        if channel_id in _id_to_channel:
            return f"#{_id_to_channel[channel_id]}"
        info = _api("conversations.info", token, channel=channel_id)
        if info.get("ok"):
            ch = info.get("channel") or {}
            name = (ch.get("name") or "").strip()
            if name:
                _id_to_channel[channel_id] = name
                return f"#{name}"
            if ch.get("is_im"):
                return "a Slack DM"
    except Exception:
        pass
    return "Slack"


def _user_label(token: str, user_id: str, event: dict | None = None) -> str:
    _load_dir_cache()
    if not user_id:
        return "Someone"
    if user_id in _id_to_user:
        return _id_to_user[user_id]
    # Prefer fields already on the event — no extra API call.
    if event:
        profile = event.get("user_profile") or {}
        for key in ("display_name", "real_name", "name"):
            val = (profile.get(key) or "").strip()
            if val:
                _remember_user(user_id, val)
                _save_dir_cache()
                return val
        uname = (event.get("username") or "").strip()
        if uname:
            _remember_user(user_id, uname)
            _save_dir_cache()
            return uname
    if time.time() < _rate_limited_until:
        return "Someone"
    try:
        info = _api("users.info", token, user=user_id)
        if info.get("ok"):
            u = info.get("user") or {}
            profile = u.get("profile") or {}
            name = (
                (profile.get("display_name") or "").strip()
                or (profile.get("real_name") or "").strip()
                or (u.get("name") or "").strip()
                or "Someone"
            )
            _remember_user(user_id, name)
            _save_dir_cache()
            return name
    except Exception:
        pass
    return "Someone"


def _strip_slack_markup(text: str) -> str:
    # <@U123|name> or <@U123> → name / someone
    text = re.sub(r"<@([UW][A-Z0-9]+)(?:\|([^>]+))?>",
                  lambda m: m.group(2) or "someone", text)
    # <#C123|general> → #general
    text = re.sub(r"<#([CG][A-Z0-9]+)(?:\|([^>]+))?>",
                  lambda m: f"#{m.group(2)}" if m.group(2) else "a channel", text)
    # <url|label> or <url>
    text = re.sub(r"<(https?://[^|>]+)(?:\|([^>]+))?>",
                  lambda m: m.group(2) or m.group(1), text)
    text = text.replace("&amp;", "&").replace("&lt;", "<").replace("&gt;", ">")
    return re.sub(r"\s+", " ", text).strip()


def _parse_message_event(token: str, event: dict) -> dict | None:
    """Turn a Slack message event into a notify payload, or None to skip."""
    etype = event.get("type") or ""
    # app_mention duplicates the message event for @bot — ignore it.
    if etype == "app_mention":
        return None
    if etype != "message":
        return None
    subtype = event.get("subtype") or ""
    if subtype in _SKIP_SUBTYPES:
        return None
    # Skip threaded parent echoes / bot echoes of ourselves.
    if event.get("bot_id") and not event.get("user"):
        return None
    uid = event.get("user") or ""
    bot_id = _ensure_bot_id(token)
    if bot_id and uid == bot_id:
        return None
    text = _strip_slack_markup(event.get("text") or "")
    if not text:
        # File shares etc. without text
        files = event.get("files") or []
        if files:
            text = f"shared a file ({files[0].get('name') or 'attachment'})"
        else:
            return None
    if len(text) > 220:
        text = text[:217] + "…"

    event_key = f"{event.get('channel')}:{event.get('ts')}"
    if event_key in _seen_events:
        return None
    _seen_events.add(event_key)
    if len(_seen_events) > 500:
        # Drop oldest-ish by rebuilding from a slice of insertion order (Py3.7+)
        keep = list(_seen_events)[-200:]
        _seen_events.clear()
        _seen_events.update(keep)

    where = _channel_label(token, event.get("channel") or "")
    who = _user_label(token, uid, event=event)
    return {
        "who": who,
        "where": where,
        "text": text,
        "channel": event.get("channel") or "",
        "ts": event.get("ts") or "",
    }


def get_notify_queue() -> asyncio.Queue:
    """Lazy-create the cross-task queue main.py drains."""
    global _notify_queue
    if _notify_queue is None:
        _notify_queue = asyncio.Queue(maxsize=50)
    return _notify_queue


async def _open_socket_url(app_token: str) -> str:
    def _call() -> dict:
        _respect_rate_limit()
        r = requests.post(
            f"{API}/apps.connections.open",
            headers={"Authorization": f"Bearer {app_token}"},
            timeout=20,
        )
        if r.status_code == 429:
            _mark_rate_limited(r.headers.get("Retry-After"))
            return {"ok": False, "error": "ratelimited"}
        try:
            return r.json()
        except Exception:
            return {"ok": False, "error": f"http_{r.status_code}"}

    data = await asyncio.to_thread(_call)
    if not data.get("ok") or not data.get("url"):
        raise RuntimeError(data.get("error") or "apps.connections.open failed")
    return data["url"]


async def run_slack_socket(log: Callable[[str], None] | None = None) -> None:
    """
    Long-running Socket Mode client. Puts notify dicts on get_notify_queue().
    Reconnects with backoff. No-op loop while tokens are missing.
    """
    global _socket_connected
    try:
        import websockets
    except ImportError:
        print("[Slack] websockets package missing — live alerts disabled.", flush=True)
        if log:
            try:
                log("SYS: Slack live alerts need the websockets package.")
            except Exception:
                pass
        return

    def _safe_log(msg: str) -> None:
        print(f"[Slack] {msg}", flush=True)
        if not log:
            return
        try:
            log(msg if msg.startswith("SYS:") else f"SYS: {msg}")
        except Exception:
            pass

    backoff = 2.0
    q = get_notify_queue()
    _safe_log("Slack live-alert watcher starting…")

    while True:
        _socket_connected = False
        if not notify_ready():
            await asyncio.sleep(15)
            continue

        # Honor cool-down after HTTP 429 storms.
        now = time.time()
        if now < _rate_limited_until:
            wait = _rate_limited_until - now
            _safe_log(f"Waiting {wait:.0f}s for Slack rate-limit cool-down…")
            await asyncio.sleep(wait)

        bot = _token()
        app = _app_token()
        try:
            # Resolve bot id once; do NOT crawl the whole workspace on connect —
            # that is what burned the rate limit earlier.
            await asyncio.to_thread(_ensure_bot_id, bot)
            url = await _open_socket_url(app)
            _safe_log("Slack live alerts connected — listening for messages.")
            backoff = 2.0
            _socket_connected = True

            async with websockets.connect(
                url, ping_interval=20, ping_timeout=20, max_size=8_000_000,
            ) as ws:
                async for raw in ws:
                    if not notify_ready():
                        break
                    try:
                        if isinstance(raw, (bytes, bytearray)):
                            raw = raw.decode("utf-8", errors="replace")
                        msg = json.loads(raw)
                    except Exception:
                        continue

                    envelope_id = msg.get("envelope_id")
                    mtype = msg.get("type")

                    # Always ack envelopes Slack expects a reply for.
                    if envelope_id:
                        try:
                            await ws.send(json.dumps({"envelope_id": envelope_id}))
                        except Exception:
                            break

                    if mtype == "hello":
                        print("[Slack] socket hello ok", flush=True)
                        continue

                    if mtype == "disconnect":
                        reason = (msg.get("reason") or "server disconnect")
                        _safe_log(f"Slack socket disconnect ({reason}) — reconnecting.")
                        break

                    if mtype != "events_api":
                        continue

                    payload = msg.get("payload") or {}
                    event = payload.get("event") or {}
                    etype = event.get("type")
                    print(
                        f"[Slack] event {etype}"
                        f"{(':' + event.get('subtype')) if event.get('subtype') else ''}"
                        f" text={(event.get('text') or '')[:60]!r}",
                        flush=True,
                    )
                    parsed = await asyncio.to_thread(_parse_message_event, bot, event)
                    if not parsed:
                        print("[Slack] event skipped by filter", flush=True)
                        continue
                    print(
                        f"[Slack] queued alert: {parsed.get('who')} in "
                        f"{parsed.get('where')}: {(parsed.get('text') or '')[:60]}",
                        flush=True,
                    )
                    try:
                        q.put_nowait(parsed)
                    except asyncio.QueueFull:
                        try:
                            _ = q.get_nowait()
                        except Exception:
                            pass
                        try:
                            q.put_nowait(parsed)
                        except Exception:
                            pass

        except asyncio.CancelledError:
            _socket_connected = False
            raise
        except Exception as e:
            _socket_connected = False
            err = str(e)
            if "ratelimited" in err:
                wait = max(30.0, _rate_limited_until - time.time())
                _safe_log(f"Slack socket rate-limited — retry in {wait:.0f}s.")
                await asyncio.sleep(wait)
                backoff = 5.0
                continue
            _safe_log(f"Slack live alerts error — {e}. Retrying…")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 1.7, 60.0)
            continue

        _socket_connected = False
        await asyncio.sleep(backoff)
        backoff = min(backoff * 1.4, 60.0)


# ── public handler ───────────────────────────────────────────────────────────

def slack_messenger(parameters: dict = None, response=None, player=None,
                    session_memory=None) -> str:
    params = parameters or {}
    action = (params.get("action") or "send").strip().lower()
    target = (params.get("target") or params.get("channel")
              or params.get("receiver") or params.get("user") or "").strip()
    text = (params.get("text") or params.get("message")
            or params.get("message_text") or "").strip()
    limit = params.get("limit") or 8

    if player:
        try:
            player.write_log(f"[Slack] {action} {target}".strip())
        except Exception:
            pass

    token = _token()
    if not token and action not in ("notify_on", "notify_off", "enable_notify",
                                    "disable_notify"):
        return _setup_help()

    try:
        if action in ("status", "connect", "check", "auth"):
            return _status(token) if token else _setup_help()
        if action in ("notify_on", "enable_notify", "unmute_notify"):
            return _set_notify(True)
        if action in ("notify_off", "disable_notify", "mute_notify"):
            return _set_notify(False)
        if action in ("list_channels", "channels", "list"):
            return _list_channels(token)
        if action in ("list_people", "people", "users", "contacts"):
            return _list_people(token)
        if action in ("read", "history", "check_channel", "summary"):
            if not target:
                return "Which Slack channel or person should I read?"
            return _read_history(token, target, limit=limit)
        if action in ("send", "message", "post", "dm"):
            if not target:
                return "Who should I send that Slack message to? Give a #channel or @person."
            if not text:
                return "What should the Slack message say?"
            return send_via_slack(target, text)
        return (
            f"Unknown Slack action '{action}'. "
            f"Use send, read, list_channels, list_people, status, "
            f"notify_on, or notify_off."
        )
    except requests.HTTPError as e:
        if "429" in str(e):
            return _friendly_api_error("ratelimited")
        return f"Slack HTTP error: {e}"
    except Exception as e:
        if "ratelimited" in str(e).lower():
            return _friendly_api_error("ratelimited")
        return f"Slack failed: {e}"


TOOL = {
    "name": "slack_messenger",
    "description": (
        "Talks to the user's Slack workspace via the Slack API (bot token). "
        "Use for: 'send a Slack message to #eng …', 'DM Jayesh on Slack', "
        "'what's the latest in #general', 'list my Slack channels', "
        "'check Slack', 'read my Slack with Priya', "
        "'enable Slack notifications', 'pause Slack alerts'. "
        "Prefer this over send_message / desktop clicking whenever the platform "
        "is Slack. "
        "Live alerts need slack_app_token + Socket Mode (see status). "
        "action=send needs target (#channel or @person) and text; "
        "action=read needs target; "
        "action=list_channels / list_people / status / notify_on / notify_off "
        "need no target."
    ),
    "parameters": {
        "type": "OBJECT",
        "properties": {
            "action": {
                "type": "STRING",
                "description": (
                    "'send' (default), 'read', 'list_channels', 'list_people', "
                    "'status', 'notify_on', or 'notify_off'."
                ),
                "enum": [
                    "send", "read", "list_channels", "list_people", "status",
                    "notify_on", "notify_off",
                ],
            },
            "target": {
                "type": "STRING",
                "description": (
                    "Channel or person: '#general', 'eng', '@jayesh', or a name."
                ),
            },
            "text": {
                "type": "STRING",
                "description": "Message body for action=send.",
            },
            "limit": {
                "type": "NUMBER",
                "description": "For read: how many recent messages (default 8, max 20).",
            },
        },
        "required": ["action"],
    },
    "handler": slack_messenger,
}
