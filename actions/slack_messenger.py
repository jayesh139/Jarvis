"""
Slack for JARVIS — send, read, and list via the Slack Web API (bot token).

Setup (once):
  1. Create an app at https://api.slack.com/apps → Create New App → From scratch
  2. OAuth & Permissions → Bot Token Scopes, add:
       channels:read, channels:history, groups:read, groups:history,
       chat:write, im:read, im:write, im:history,
       users:read, users:read.email, mpim:read, mpim:write, mpim:history
  3. Install to Workspace → copy the Bot User OAuth Token (xoxb-…)
  4. Put it in config/api_keys.json:
       "slack_bot_token": "xoxb-…"
  5. Invite the bot to any private channel it should use: /invite @YourBot
  6. Say "check Slack" or "send Slack message to #general …"

No macOS Accessibility needed for API messaging — that is only for click-typing.
"""
from __future__ import annotations

import json
import re
import time
from pathlib import Path
from typing import Any

import requests

BASE_DIR = Path(__file__).resolve().parent.parent
CONFIG_FILE = BASE_DIR / "config" / "api_keys.json"
API = "https://slack.com/api"

# Cache channel / user lookups for the process lifetime (names rarely change).
_channel_cache: dict[str, str] = {}  # lower name -> id
_user_cache: dict[str, str] = {}     # lower display/real/name -> id
_cache_ts = 0.0
_CACHE_TTL = 300.0


# ── config ───────────────────────────────────────────────────────────────────

def _load_cfg() -> dict:
    try:
        return json.loads(CONFIG_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _token() -> str:
    cfg = _load_cfg()
    return (cfg.get("slack_bot_token") or cfg.get("slack_token") or "").strip()


def is_configured() -> bool:
    """True when a Slack bot token is present in api_keys.json."""
    tok = _token()
    return bool(tok and (tok.startswith("xoxb-") or tok.startswith("xoxp-")))


def _setup_help() -> str:
    return (
        "Slack is not connected yet. Create a Slack app at api.slack.com/apps, "
        "add bot scopes (channels:read, channels:history, groups:read, "
        "groups:history, chat:write, im:read, im:write, im:history, users:read), "
        "Install to Workspace, then put the Bot token in config/api_keys.json as "
        '"slack_bot_token": "xoxb-…". Invite the bot to private channels with '
        "/invite @BotName."
    )


# ── HTTP ─────────────────────────────────────────────────────────────────────

def _api(method: str, token: str, **params: Any) -> dict:
    """Call a Slack Web API method. Returns the JSON body (always a dict)."""
    # Prefer POST form for write methods; GET query for reads — Slack accepts both.
    write = method.startswith("chat.") or method in (
        "conversations.open", "conversations.join",
    )
    headers = {"Authorization": f"Bearer {token}"}
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
    r.raise_for_status()
    data = r.json()
    if not isinstance(data, dict):
        return {"ok": False, "error": "bad_response"}
    return data


def _auth_test(token: str) -> dict:
    return _api("auth.test", token)


# ── resolve people & channels ────────────────────────────────────────────────

def _refresh_caches(token: str, force: bool = False) -> None:
    global _cache_ts
    now = time.time()
    if not force and _channel_cache and _user_cache and (now - _cache_ts) < _CACHE_TTL:
        return

    # Conversations the bot can see (public + private + DMs + MPIMs).
    cursor = None
    channels: dict[str, str] = {}
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
            break
        for ch in data.get("channels") or []:
            name = (ch.get("name") or "").strip().lower()
            cid = ch.get("id") or ""
            if name and cid:
                channels[name] = cid
                channels[f"#{name}"] = cid
        cursor = (data.get("response_metadata") or {}).get("next_cursor") or ""
        if not cursor:
            break

    # Workspace users — match by display name, real name, or @handle.
    cursor = None
    users: dict[str, str] = {}
    while True:
        data = _api("users.list", token, limit=200, cursor=cursor or "")
        if not data.get("ok"):
            break
        for u in data.get("members") or []:
            if u.get("deleted") or u.get("is_bot") or u.get("id") == "USLACKBOT":
                continue
            uid = u.get("id") or ""
            if not uid:
                continue
            profile = u.get("profile") or {}
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
        cursor = (data.get("response_metadata") or {}).get("next_cursor") or ""
        if not cursor:
            break

    _channel_cache.clear()
    _channel_cache.update(channels)
    _user_cache.clear()
    _user_cache.update(users)
    _cache_ts = now


def _looks_like_id(value: str) -> bool:
    return bool(re.fullmatch(r"[CDGUVW][A-Z0-9]{8,}", value.strip().upper()))


def _resolve_target(token: str, raw: str) -> tuple[str | None, str]:
    """Return (channel_id_or_user_id, label) or (None, error)."""
    target = (raw or "").strip()
    if not target:
        return None, "Who should I message on Slack?"

    if _looks_like_id(target):
        return target.upper(), target

    _refresh_caches(token)
    key = target.lower().lstrip()
    # Channel forms: #eng, eng, channel eng
    ch_key = key
    if ch_key.startswith("channel "):
        ch_key = ch_key[8:].strip()
    if ch_key in _channel_cache:
        name = ch_key.lstrip("#")
        return _channel_cache[ch_key], f"#{name}"

    # Fuzzy channel contains
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
    if u_key in _user_cache:
        return _user_cache[u_key], u_key if u_key.startswith("@") else f"@{u_key}"

    for name, uid in _user_cache.items():
        n = name.lstrip("@")
        if bare == n or bare in n or n in bare:
            return uid, f"@{n}"

    return None, (
        f"I could not find Slack channel or person '{target}'. "
        f"Try #channel-name or @username, and invite the bot to private channels."
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
    _refresh_caches(token, force=True)
    names = sorted({n.lstrip("#") for n in _channel_cache if not n.startswith("#")})
    if not names:
        return "I cannot see any Slack channels yet. Check the bot scopes and reinstall."
    show = names[: max(1, min(limit, 50))]
    extra = len(names) - len(show)
    body = ", ".join(f"#{n}" for n in show)
    if extra > 0:
        body += f" (+{extra} more)"
    return f"Slack channels I can see: {body}."


def _list_people(token: str, limit: int = 25) -> str:
    _refresh_caches(token, force=True)
    # Prefer display-ish names without the @ duplicate keys.
    names = sorted({n.lstrip("@") for n in _user_cache if not n.startswith("@")})
    if not names:
        return "I cannot see workspace members. Add users:read and reinstall the app."
    show = names[: max(1, min(limit, 50))]
    extra = len(names) - len(show)
    body = ", ".join(show)
    if extra > 0:
        body += f" (+{extra} more)"
    return f"Slack people I can message: {body}."


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
    data = _auth_test(token)
    if not data.get("ok"):
        return f"Slack token looks invalid ({data.get('error', 'unknown')}). {_setup_help()}"
    team = data.get("team") or "workspace"
    user = data.get("user") or "bot"
    return f"Slack connected as @{user} in {team}."


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
    if not token:
        return _setup_help()

    try:
        if action in ("status", "connect", "check", "auth"):
            return _status(token)
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
            f"Use send, read, list_channels, list_people, or status."
        )
    except requests.HTTPError as e:
        return f"Slack HTTP error: {e}"
    except Exception as e:
        return f"Slack failed: {e}"


TOOL = {
    "name": "slack_messenger",
    "description": (
        "Talks to the user's Slack workspace via the Slack API (bot token). "
        "Use for: 'send a Slack message to #eng …', 'DM Jayesh on Slack', "
        "'what's the latest in #general', 'list my Slack channels', "
        "'check Slack', 'read my Slack with Priya'. "
        "Prefer this over send_message / desktop clicking whenever the platform "
        "is Slack. "
        "action=send needs target (#channel or @person) and text; "
        "action=read needs target; "
        "action=list_channels / list_people / status need no target. "
        "If Slack is not configured, tell the user to add slack_bot_token."
    ),
    "parameters": {
        "type": "OBJECT",
        "properties": {
            "action": {
                "type": "STRING",
                "description": (
                    "'send' (default), 'read', 'list_channels', 'list_people', "
                    "or 'status'."
                ),
                "enum": [
                    "send", "read", "list_channels", "list_people", "status",
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
