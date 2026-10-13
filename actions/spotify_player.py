"""
Spotify playback for JARVIS — voice play / pause / next / previous / volume.

Setup (once):
  1. Create an app at https://developer.spotify.com/dashboard
  2. Add Redirect URI exactly:  http://127.0.0.1:8765/callback
  3. Put client id + secret in config/api_keys.json:
       "spotify_client_id": "...",
       "spotify_client_secret": "..."
  4. Say "connect Spotify" (or just "play …") — browser opens once.
     The refresh token is saved; you should not need to log in again
     unless you revoke the app in Spotify account settings.

Requires Spotify Premium for Web API playback control.
"""
from __future__ import annotations

import base64
import json
import platform
import subprocess
import threading
import time
import webbrowser
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlencode, urlparse

import requests

# ── paths / constants ────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent.parent
CONFIG_FILE = BASE_DIR / "config" / "api_keys.json"

REDIRECT_URI = "http://127.0.0.1:8765/callback"
AUTH_PORT = 8765
TOKEN_URL = "https://accounts.spotify.com/api/token"
AUTH_URL = "https://accounts.spotify.com/authorize"
API = "https://api.spotify.com/v1"

SCOPES = " ".join([
    "user-modify-playback-state",
    "user-read-playback-state",
    "user-read-currently-playing",
    "playlist-read-private",
    "playlist-read-collaborative",
])

# Genre / mood / vibe phrases → prefer a playlist over a single track.
_PLAYLIST_HINTS = (
    "bollywood", "hollywood", "punjabi", "hip hop", "hip-hop", "rap", "rock",
    "jazz", "classical", "lofi", "lo-fi", "chill", "workout", "party",
    "dance", "edm", "metal", "indie", "pop", "romantic", "love songs",
    "focus", "sleep", "rain", "sad", "happy", "desi", "ghazal", "sufi",
    "playlist", "mix", "radio", "hits", "top 50", "charts",
)


# ── config helpers ───────────────────────────────────────────────────────────
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


def _creds() -> tuple[str, str]:
    cfg = _load_cfg()
    return (
        (cfg.get("spotify_client_id") or "").strip(),
        (cfg.get("spotify_client_secret") or "").strip(),
    )


def _basic_auth(client_id: str, client_secret: str) -> str:
    raw = f"{client_id}:{client_secret}".encode()
    return "Basic " + base64.b64encode(raw).decode()


# ── OAuth (one-time) + refresh ───────────────────────────────────────────────
def _exchange_code(code: str, client_id: str, client_secret: str) -> dict:
    r = requests.post(
        TOKEN_URL,
        data={
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": REDIRECT_URI,
        },
        headers={
            "Authorization": _basic_auth(client_id, client_secret),
            "Content-Type": "application/x-www-form-urlencoded",
        },
        timeout=30,
    )
    r.raise_for_status()
    return r.json()


def _refresh_access(refresh_token: str, client_id: str, client_secret: str) -> dict:
    r = requests.post(
        TOKEN_URL,
        data={
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
        },
        headers={
            "Authorization": _basic_auth(client_id, client_secret),
            "Content-Type": "application/x-www-form-urlencoded",
        },
        timeout=30,
    )
    r.raise_for_status()
    return r.json()


def _save_tokens(payload: dict, *, keep_refresh: str | None = None) -> None:
    """Persist tokens. Refresh token is only sent on first auth (and sometimes
    on refresh); we never wipe a stored refresh token unless Spotify gives a new one."""
    refresh = payload.get("refresh_token") or keep_refresh or ""
    fields: dict[str, Any] = {
        "spotify_access_token": payload.get("access_token", ""),
        "spotify_token_expires_at": int(time.time()) + int(payload.get("expires_in", 3600)) - 60,
    }
    if refresh:
        fields["spotify_refresh_token"] = refresh
    _patch_cfg(**fields)


def _run_oauth(client_id: str, client_secret: str) -> str:
    """Open the Spotify consent page once; block until the redirect returns a code."""
    result: dict[str, str] = {"code": "", "error": ""}

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):  # noqa: N802
            qs = parse_qs(urlparse(self.path).query)
            if qs.get("code"):
                result["code"] = qs["code"][0]
                body = b"<html><body style='font-family:sans-serif;background:#111;color:#0f0;padding:2rem'>"
                body += b"<h2>Spotify connected</h2><p>You can close this tab and return to JARVIS.</p></body></html>"
                self.send_response(200)
            else:
                result["error"] = (qs.get("error") or ["unknown"])[0]
                body = b"<html><body style='font-family:sans-serif;padding:2rem'><h2>Authorisation failed</h2></body></html>"
                self.send_response(400)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *_a):  # silence
            return

    try:
        server = HTTPServer(("127.0.0.1", AUTH_PORT), Handler)
    except OSError as e:
        return (f"Could not start the local Spotify login server on port {AUTH_PORT}: {e}. "
                f"Close whatever is using that port and try again.")

    thread = threading.Thread(target=server.handle_request, daemon=True)
    thread.start()

    params = {
        "client_id": client_id,
        "response_type": "code",
        "redirect_uri": REDIRECT_URI,
        "scope": SCOPES,
        "show_dialog": "false",  # skip the consent screen if already approved
    }
    webbrowser.open(f"{AUTH_URL}?{urlencode(params)}")

    thread.join(timeout=180)
    try:
        server.server_close()
    except Exception:
        pass

    if result["error"]:
        return f"Spotify authorisation failed: {result['error']}."
    if not result["code"]:
        return "Spotify login timed out. Say 'connect Spotify' and finish the browser step."

    try:
        tokens = _exchange_code(result["code"], client_id, client_secret)
        if not tokens.get("refresh_token"):
            return ("Spotify did not return a refresh token. In the Spotify dashboard, "
                    "remove JARVIS access under Account → Apps and try connect again.")
        _save_tokens(tokens)
        return "Spotify is connected. You will not need to log in again."
    except Exception as e:
        return f"Spotify token exchange failed: {e}"


def _access_token(*, allow_oauth: bool = True) -> tuple[str | None, str | None]:
    """Return (access_token, error_message). Uses the saved refresh token silently."""
    client_id, client_secret = _creds()
    if not client_id or not client_secret:
        return None, (
            "Spotify is not configured. Add spotify_client_id and spotify_client_secret "
            "to config/api_keys.json, set Redirect URI to http://127.0.0.1:8765/callback "
            "in the Spotify developer dashboard, then say 'connect Spotify'."
        )

    cfg = _load_cfg()
    refresh = (cfg.get("spotify_refresh_token") or "").strip()
    access = (cfg.get("spotify_access_token") or "").strip()
    expires = int(cfg.get("spotify_token_expires_at") or 0)

    if access and time.time() < expires:
        return access, None

    if refresh:
        try:
            tokens = _refresh_access(refresh, client_id, client_secret)
            _save_tokens(tokens, keep_refresh=refresh)
            return tokens["access_token"], None
        except requests.HTTPError as e:
            # Revoked / invalid refresh → need a fresh login once.
            if e.response is not None and e.response.status_code in (400, 401):
                _patch_cfg(spotify_refresh_token="", spotify_access_token="",
                           spotify_token_expires_at=0)
            else:
                return None, f"Spotify token refresh failed: {e}"
        except Exception as e:
            return None, f"Spotify token refresh failed: {e}"

    if not allow_oauth:
        return None, "Spotify is not connected. Say 'connect Spotify' once."

    msg = _run_oauth(client_id, client_secret)
    if "connected" not in msg.lower() and "will not need" not in msg.lower():
        return None, msg
    cfg = _load_cfg()
    access = (cfg.get("spotify_access_token") or "").strip()
    if not access:
        return None, msg
    return access, None


# ── Spotify Web API ──────────────────────────────────────────────────────────
def _api(method: str, path: str, token: str, *,
         json_body: dict | None = None, params: dict | None = None) -> requests.Response:
    return requests.request(
        method,
        f"{API}{path}",
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        json=json_body,
        params=params,
        timeout=30,
    )


def _ensure_spotify_app() -> None:
    system = platform.system()
    try:
        if system == "Darwin":
            subprocess.Popen(["open", "-a", "Spotify"],
                             stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        elif system == "Windows":
            subprocess.Popen(["cmd", "/c", "start", "", "spotify:"],
                             stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                             shell=False)
        else:
            subprocess.Popen(["spotify"],
                             stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    except Exception:
        pass


def _devices(token: str) -> list[dict]:
    r = _api("GET", "/me/player/devices", token)
    if r.status_code != 200:
        return []
    return list((r.json() or {}).get("devices") or [])


def _is_web_player(device: dict) -> bool:
    name = (device.get("name") or "").lower()
    return "web player" in name or name.startswith("web player")


def _device_score(device: dict) -> tuple:
    """Rank devices that actually honour /me/player/play.

    Browser Web Players often accept the track (so JARVIS says the right
    song) but stay paused — phones and the desktop Spotify app do not.
    """
    dtype = (device.get("type") or "").lower()
    web = _is_web_player(device)
    type_rank = {
        "smartphone": 0,
        "tablet": 1,
        "computer": 3 if web else 2,
        "speaker": 1,
        "tv": 2,
    }.get(dtype, 4)
    # Active non-web first; active web last among equals.
    active_rank = 0 if device.get("is_active") and not web else (
        1 if device.get("is_active") else 2)
    restricted = 1 if device.get("is_restricted") else 0
    return (restricted, active_rank, type_rank, web)


def _ranked_devices(token: str) -> list[dict]:
    devices = [d for d in _devices(token) if d.get("id") and not d.get("is_restricted")]
    if not devices:
        _ensure_spotify_app()
        for _ in range(6):
            time.sleep(1.0)
            devices = [d for d in _devices(token)
                       if d.get("id") and not d.get("is_restricted")]
            if devices:
                break
    return sorted(devices, key=_device_score)


def _pick_device(token: str) -> str | None:
    ranked = _ranked_devices(token)
    return ranked[0]["id"] if ranked else None


def _transfer(token: str, device_id: str, *, play: bool = False) -> requests.Response:
    return _api("PUT", "/me/player", token,
                json_body={"device_ids": [device_id], "play": bool(play)})


def _playback_state(token: str) -> dict:
    r = _api("GET", "/me/player", token)
    if r.status_code == 200:
        try:
            return r.json() or {}
        except Exception:
            return {}
    return {}


def _is_playing_on(token: str, device_id: str | None = None) -> bool:
    state = _playback_state(token)
    if not state.get("is_playing"):
        return False
    if device_id and (state.get("device") or {}).get("id") != device_id:
        return False
    return True


def _search(token: str, query: str) -> dict:
    r = _api("GET", "/search", token, params={
        "q": query, "type": "track,playlist,album", "limit": 8,
    })
    r.raise_for_status()
    return r.json()


def _prefer_playlist(query: str) -> bool:
    q = query.lower().strip()
    if " by " in q or "song" in q or "track" in q:
        return False
    return any(h in q for h in _PLAYLIST_HINTS) or len(q.split()) <= 3


def _choose_play_target(query: str, results: dict) -> tuple[dict, str]:
    """Return ({'uris': [...]} or {'context_uri': '...'}, spoken label)."""
    tracks = (results.get("tracks") or {}).get("items") or []
    playlists = (results.get("playlists") or {}).get("items") or []
    albums = (results.get("albums") or {}).get("items") or []
    playlists = [p for p in playlists if p]
    albums = [a for a in albums if a]
    tracks = [t for t in tracks if t]

    if _prefer_playlist(query) and playlists:
        pl = playlists[0]
        name = pl.get("name") or query
        owner = ((pl.get("owner") or {}).get("display_name")) or ""
        label = f"playlist '{name}'" + (f" by {owner}" if owner else "")
        return {"context_uri": pl["uri"]}, label

    if tracks:
        t = tracks[0]
        artists = ", ".join(a.get("name", "") for a in (t.get("artists") or []) if a.get("name"))
        label = f"'{t.get('name')}'" + (f" by {artists}" if artists else "")
        return {"uris": [t["uri"]]}, label

    if playlists:
        pl = playlists[0]
        return {"context_uri": pl["uri"]}, f"playlist '{pl.get('name') or query}'"

    if albums:
        al = albums[0]
        return {"context_uri": al["uri"]}, f"album '{al.get('name') or query}'"

    raise LookupError(f"nothing found for '{query}'")


def _play_body(token: str, body: dict, device_id: str | None) -> requests.Response:
    params = {"device_id": device_id} if device_id else None
    return _api("PUT", "/me/player/play", token, json_body=body, params=params)


def _start_on_device(token: str, body: dict, device: dict) -> bool:
    """Load the track on this device and confirm Spotify is actually playing."""
    device_id = device.get("id")
    if not device_id:
        return False

    # Wake / claim the device with play=true — critical for phones that are
    # listed but idle. Transfer alone with play=false left tracks queued and
    # paused (especially on browser Web Players).
    tr = _transfer(token, device_id, play=True)
    if tr.status_code not in (204, 200, 202) and tr.status_code != 404:
        # 404 = no prior playback to transfer; still try an explicit play.
        pass
    time.sleep(0.5)

    r = _play_body(token, body, device_id)
    if r.status_code == 404:
        time.sleep(1.0)
        r = _play_body(token, body, device_id)
    if r.status_code == 403:
        return False
    if r.status_code not in (204, 200, 202):
        return False

    # API success ≠ audible playback on Web Player. Verify.
    for _ in range(5):
        time.sleep(0.4)
        if _is_playing_on(token, device_id):
            return True
        # Nudge resume on the same device.
        _api("PUT", "/me/player/play", token, params={"device_id": device_id})
    return _is_playing_on(token, device_id)


def _play_query(token: str, query: str) -> str:
    results = _search(token, query)
    body, label = _choose_play_target(query, results)
    ranked = _ranked_devices(token)
    if not ranked:
        return ("I could not find an active Spotify device. Open Spotify on your "
                "phone or install the desktop app, play anything once, then ask again.")

    tried = []
    for device in ranked:
        name = device.get("name") or device.get("type") or "device"
        print(f"[Spotify] trying device: {name} (web={_is_web_player(device)})")
        tried.append(name)
        if _start_on_device(token, body, device):
            return f"Playing {label} on {name}."

    # Last resort: any device that at least accepted the queue.
    last = ranked[0]
    _transfer(token, last["id"], play=True)
    _play_body(token, body, last["id"])
    names = ", ".join(tried)
    return (f"Queued {label}, but Spotify did not start on: {names}. "
            f"Unlock your phone's Spotify app (or open the desktop app) and ask again.")


def _simple_control(token: str, action: str, volume: int | None = None) -> str:
    ranked = _ranked_devices(token)
    # Prefer the device that is actually active/playing for transport controls.
    playing = next((d for d in ranked if d.get("is_active")), None)
    device_id = (playing or (ranked[0] if ranked else {})).get("id")
    params = {"device_id": device_id} if device_id else None

    if action == "pause":
        r = _api("PUT", "/me/player/pause", token, params=params)
        return "Paused Spotify." if r.status_code in (204, 200) else f"Could not pause ({r.status_code})."
    if action in ("resume", "unpause"):
        r = _api("PUT", "/me/player/play", token, params=params)
        return "Resumed Spotify." if r.status_code in (204, 200) else f"Could not resume ({r.status_code})."
    if action in ("next", "skip"):
        r = _api("POST", "/me/player/next", token, params=params)
        return "Skipped to the next track." if r.status_code in (204, 200) else f"Could not skip ({r.status_code})."
    if action in ("previous", "prev", "back"):
        r = _api("POST", "/me/player/previous", token, params=params)
        return "Went back to the previous track." if r.status_code in (204, 200) else f"Could not go back ({r.status_code})."
    if action in ("volume", "vol") and volume is not None:
        vol = max(0, min(100, int(volume)))
        p = {"volume_percent": vol}
        if device_id:
            p["device_id"] = device_id
        r = _api("PUT", "/me/player/volume", token, params=p)
        return f"Spotify volume set to {vol}%." if r.status_code in (204, 200) else f"Could not set volume ({r.status_code})."
    if action in ("status", "what", "now"):
        r = _api("GET", "/me/player/currently-playing", token)
        if r.status_code == 204:
            return "Nothing is playing on Spotify."
        if r.status_code != 200:
            return f"Could not read Spotify status ({r.status_code})."
        data = r.json() or {}
        item = data.get("item") or {}
        name = item.get("name") or "Unknown"
        artists = ", ".join(a.get("name", "") for a in (item.get("artists") or []) if a.get("name"))
        state = "Playing" if data.get("is_playing") else "Paused"
        return f"{state}: {name}" + (f" by {artists}." if artists else ".")
    return f"Unknown Spotify action '{action}'."


# ── mic auto-mute while music plays ─────────────────────────────────────────
_watch_lock = threading.Lock()
_watch_stop = threading.Event()
_watch_thread: threading.Thread | None = None
_watch_player = None


def _set_music_mic(player, playing: bool) -> None:
    """Tell the HUD to mute/unmute around Spotify playback."""
    if player is None or not hasattr(player, "set_spotify_playing"):
        return
    try:
        player.set_spotify_playing(bool(playing))
    except Exception:
        pass


def _watch_playback(player) -> None:
    """Poll Spotify until playback stops so pause-from-phone also reopens the mic."""
    global _watch_thread
    misses = 0
    try:
        while not _watch_stop.is_set():
            time.sleep(4.0)
            if _watch_stop.is_set():
                break
            token, err = _access_token(allow_oauth=False)
            if err or not token:
                misses += 1
                if misses >= 3:
                    break
                continue
            state = _playback_state(token)
            if state.get("is_playing"):
                misses = 0
                _set_music_mic(player, True)
            else:
                misses += 1
                # Two quiet polls ≈ track ended / paused — open the mic again.
                if misses >= 2:
                    _set_music_mic(player, False)
                    break
    finally:
        with _watch_lock:
            _watch_thread = None


def _start_music_watch(player) -> None:
    global _watch_thread, _watch_player
    _set_music_mic(player, True)
    with _watch_lock:
        _watch_player = player
        _watch_stop.clear()
        if _watch_thread is not None and _watch_thread.is_alive():
            return
        _watch_thread = threading.Thread(
            target=_watch_playback, args=(player,),
            daemon=True, name="spotify-mic-watch",
        )
        _watch_thread.start()


def _stop_music_watch(player) -> None:
    _watch_stop.set()
    _set_music_mic(player, False)


# ── public handler ───────────────────────────────────────────────────────────
def spotify_player(parameters: dict = None, response=None, player=None,
                   session_memory=None) -> str:
    params = parameters or {}
    action = (params.get("action") or "play").strip().lower()
    query = (params.get("query") or params.get("source") or "").strip()
    volume = params.get("volume")

    if player:
        try:
            player.write_log(f"[Spotify] {action} {query}".strip())
        except Exception:
            pass

    if action in ("connect", "auth", "login", "authorise", "authorize"):
        client_id, client_secret = _creds()
        if not client_id or not client_secret:
            return (
                "Add spotify_client_id and spotify_client_secret to config/api_keys.json "
                "first, and set the Redirect URI to http://127.0.0.1:8765/callback "
                "in your Spotify developer dashboard."
            )
        # Force a fresh consent if they explicitly ask to connect.
        return _run_oauth(client_id, client_secret)

    token, err = _access_token(allow_oauth=True)
    if err or not token:
        return err or "Spotify is not ready."

    try:
        if action in ("play", "search", "put_on"):
            if not query:
                return "What should I play on Spotify?"
            msg = _play_query(token, query)
            if msg.lower().startswith("playing"):
                _start_music_watch(player)
            return msg
        if action in ("pause",):
            msg = _simple_control(token, action)
            _stop_music_watch(player)
            return msg
        if action in ("resume", "unpause"):
            msg = _simple_control(token, action)
            if "resumed" in msg.lower():
                _start_music_watch(player)
            return msg
        if action in ("next", "skip", "previous", "prev", "back",
                      "status", "what", "now"):
            return _simple_control(token, action)
        if action in ("volume", "vol"):
            if volume is None:
                return "What volume level, from 0 to 100?"
            return _simple_control(token, "volume", volume=int(volume))
        return f"I don't know the Spotify action '{action}'."
    except LookupError as e:
        return f"Spotify: {e}."
    except requests.HTTPError as e:
        return f"Spotify API error: {e}"
    except Exception as e:
        return f"Spotify failed: {e}"


TOOL = {
    "name": "spotify_player",
    "description": (
        "Controls Spotify music for the user (Premium). Use whenever they want to "
        "LISTEN to music: 'play Shape of You', 'play Bollywood', 'play some lo-fi', "
        "'pause the music', 'next song', 'previous track', 'set Spotify volume to 30', "
        "'what's playing', 'connect Spotify'. "
        "Do NOT use video_player or youtube_video for music — those are for watching "
        "video on the HUD / in a browser. "
        "For action='play', pass what they asked for in query (song, artist, album, "
        "genre, or playlist vibe). "
        "First-time only: if Spotify is not connected, this opens a browser login "
        "once and then remembers the account."
    ),
    "parameters": {
        "type": "OBJECT",
        "properties": {
            "action": {
                "type": "STRING",
                "description": (
                    "'play' (default), 'pause', 'resume', 'next', 'previous', "
                    "'volume', 'status', or 'connect' for the one-time login."
                ),
                "enum": [
                    "play", "pause", "resume", "next", "previous",
                    "volume", "status", "connect",
                ],
            },
            "query": {
                "type": "STRING",
                "description": (
                    "For play: song name, artist, album, genre, or vibe "
                    "(e.g. 'Bollywood', 'lofi study', 'Blinding Lights')."
                ),
            },
            "volume": {
                "type": "NUMBER",
                "description": "For action=volume: 0–100.",
            },
        },
        "required": ["action"],
    },
    "handler": spotify_player,
}
