"""session-awareness plugin — cross-session awareness for local Hermes sessions.

Every local session gets a one-shot announcement of the *other* live Hermes sessions on this
Mac (B1), and a heads-up nudge after a delete/move ``terminal`` call while peers are still live
(B3). Peer data comes from a read-only ``SELECT`` on ``cross_session_registry`` in
``$HERMES_HOME/state.db``; nothing is ever written and no hook ever raises.

Return contract (``model_tools._apply_transform_tool_result_hook``): a ``str`` is a FULL
replacement of the tool result (build it as ``result + block``), ``None`` means unchanged, and
the first string across plugins wins — so another plugin's transform may swallow this block.

Kill switch: ``SESSION_AWARENESS_DISABLE=1``. Config (all optional) under
``plugins.session-awareness`` in config.yaml: ``announce_first``, ``overlap_nudges``,
``max_sessions``, ``max_age_min``, ``nudge_cooldown_seconds``.
"""

from __future__ import annotations

import logging
import os
import shlex
import sqlite3
import threading
import time
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

_PLUGIN_ID = "session-awareness"
_ENV_DISABLE = "SESSION_AWARENESS_DISABLE"
_TRUTHY = {"1", "true", "yes", "on"}
_FALSY = {"0", "false", "no", "off"}

# Config defaults (Rules: all keys optional, these values).
DEFAULTS: Dict[str, Any] = {
    "announce_first": True,
    "overlap_nudges": True,
    "max_sessions": 6,
    "max_age_min": 720.0,
    "nudge_cooldown_seconds": 300.0,
}

_TABLE = "cross_session_registry"
_PEER_CACHE_TTL = 5.0  # process-global peer snapshot cache
_CONFIG_CACHE_TTL = 30.0
_STATE_MAX_AGE = 24 * 3600.0  # opportunistic prune horizon for per-session state
_BUSY_TIMEOUT_MS = 50
# Deletion/move verbs for B3 — matched against whole command TOKENS, never as substrings.
_DELETE_MOVE_VERBS = frozenset({"rm", "rmdir", "mv", "truncate"})
# statuses the dispatcher sends for a successful call; anything else is treated as a failure.
_OK_STATUSES = frozenset({"", "ok", "success", "succeeded", "completed"})
_B1_NAME_MAX = 60
_B3_NAME_MAX = 18

# ─── module state (every mutation under _lock) ───────────────────────────────
_lock = threading.Lock()
_peer_cache: Optional[Tuple[float, List[Dict[str, Any]]]] = None
_config_cache: Optional[Tuple[float, Dict[str, Any]]] = None
_announced: Dict[str, float] = {}   # session_id -> time.time() of the B1 block
_nudged: Dict[str, float] = {}      # session_id -> time.time() of the last B3 nudge


def _disabled() -> bool:
    return os.environ.get(_ENV_DISABLE, "").strip().lower() in _TRUTHY


def _as_bool(value: Any, default: bool) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        text = value.strip().lower()
        if text in _TRUTHY:
            return True
        if text in _FALSY:
            return False
    return default


def _load_plugins_config() -> Dict[str, Any]:
    """``config.yaml``'s ``plugins`` mapping. Imported lazily on purpose: the
    ``hermes plugins validate`` capability probe imports this module in a bare interpreter
    where ``hermes_cli`` does not exist, and a module-level import would fail admission."""
    from hermes_cli.config import load_config_readonly  # noqa: PLC0415

    data = load_config_readonly() or {}
    plugins = data.get("plugins") if isinstance(data, dict) else None
    return plugins if isinstance(plugins, dict) else {}


def _settings() -> Dict[str, Any]:
    """Resolved settings: defaults overlaid with ``plugins.session-awareness``, cached briefly."""
    global _config_cache
    now = time.time()
    with _lock:
        if _config_cache is not None and now - _config_cache[0] < _CONFIG_CACHE_TTL:
            return dict(_config_cache[1])
    settings = dict(DEFAULTS)
    try:
        entry = _load_plugins_config().get(_PLUGIN_ID)
        if isinstance(entry, dict):
            for key, default in DEFAULTS.items():
                if key not in entry or entry[key] is None:
                    continue
                raw = entry[key]
                if isinstance(default, bool):
                    settings[key] = _as_bool(raw, bool(default))
                else:
                    try:
                        settings[key] = float(raw)
                    except (TypeError, ValueError):
                        continue
    except Exception as exc:  # missing config / unreadable yaml / no hermes_cli: use defaults
        logger.debug("session-awareness: config unavailable (%s); using defaults", exc)
        settings = dict(DEFAULTS)
    with _lock:
        _config_cache = (now, settings)
    return dict(settings)


# ─── peer discovery ──────────────────────────────────────────────────────────


def _state_db_path() -> str:
    home = os.environ.get("HERMES_HOME") or os.path.join(os.path.expanduser("~"), ".hermes")
    return os.path.join(home, "state.db")


def _read_registry() -> List[Dict[str, Any]]:
    """Read-only snapshot of the peer rows; ANY sqlite/os error -> empty list, never raises."""
    path = _state_db_path()
    # Only '?'/'#' need escaping to survive inside a file: URI; leaving '/' literal keeps the
    # path unambiguous.
    uri = "file:" + path.replace("?", "%3F").replace("#", "%23") + "?mode=ro"
    conn: Optional[sqlite3.Connection] = None
    try:
        conn = sqlite3.connect(uri, uri=True, timeout=_BUSY_TIMEOUT_MS / 1000.0)
        try:
            conn.execute("PRAGMA busy_timeout=%d" % _BUSY_TIMEOUT_MS)
        except sqlite3.Error:
            pass  # the connect() timeout already sets it
        rows = conn.execute(
            "SELECT session_id, name, pid, last_heartbeat FROM %s" % _TABLE
        ).fetchall()
    except Exception as exc:  # missing / corrupt / locked / no such table
        logger.debug("session-awareness: registry read failed (%s)", exc)
        return []
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass
    peers: List[Dict[str, Any]] = []
    for row in rows or []:
        try:
            session_id, name, pid, heartbeat = row[0], row[1], row[2], row[3]
        except Exception:
            continue
        peers.append({
            "session_id": session_id if isinstance(session_id, str) else "",
            "name": name.strip() if isinstance(name, str) and name.strip() else "(unnamed)",
            "pid": pid if isinstance(pid, int) and not isinstance(pid, bool) else None,
            "last_heartbeat": float(heartbeat) if isinstance(heartbeat, (int, float)) else 0.0,
        })
    return peers


def _all_peers() -> List[Dict[str, Any]]:
    """Peer rows, cached 5 s process-wide (one lock, open per read, never a held connection)."""
    global _peer_cache
    now = time.time()
    with _lock:
        if _peer_cache is not None and now - _peer_cache[0] < _PEER_CACHE_TTL:
            return list(_peer_cache[1])
    peers = _read_registry()
    with _lock:
        _peer_cache = (now, peers)
    return list(peers)


def _pid_alive(pid: Optional[int]) -> bool:
    """True unless the pid is provably dead (ESRCH). ``None``/non-positive -> alive: probed pids
    are never 0/negative because ``os.kill(0, 0)``/``os.kill(-1, 0)`` address a whole process
    group, and any other doubt resolves to alive."""
    if not isinstance(pid, int) or isinstance(pid, bool) or pid <= 0:
        return True
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except Exception:
        return True
    return True


def _live_peers(session_id: str, settings: Dict[str, Any], now: float) -> List[Dict[str, Any]]:
    """Other sessions: self excluded, heartbeat newer than ``max_age_min``, pid not provably dead;
    newest heartbeat first, capped at ``max_sessions`` (no '+N more' decoration)."""
    try:
        max_age_min = max(0.0, float(settings.get("max_age_min", DEFAULTS["max_age_min"])))
    except (TypeError, ValueError):
        max_age_min = DEFAULTS["max_age_min"]  # type: ignore[assignment]
    try:
        cap = int(settings.get("max_sessions", DEFAULTS["max_sessions"]))
    except (TypeError, ValueError):
        cap = int(DEFAULTS["max_sessions"])
    cutoff = now - max_age_min * 60.0
    peers = [
        peer for peer in _all_peers()
        if peer["session_id"] != (session_id or "")
        and peer["last_heartbeat"] >= cutoff
        and _pid_alive(peer["pid"])
    ]
    peers.sort(key=lambda peer: peer["last_heartbeat"], reverse=True)
    return peers[: max(1, cap)]


# ─── rendering ───────────────────────────────────────────────────────────────


def _fmt_age(seconds: float) -> str:
    age = max(0, int(seconds))
    if age < 60:
        return "%ds ago" % age
    if age < 3600:
        return "%dm ago" % (age // 60)
    if age < 86400:
        return "%dh ago" % (age // 3600)
    return "%dd ago" % (age // 86400)


def _short(name: str, limit: int) -> str:
    name = (name or "").strip() or "(unnamed)"
    return name if len(name) <= limit else name[: max(1, limit - 1)].rstrip() + "\u2026"


def _b1_block(peers: List[Dict[str, Any]], now: float) -> str:
    lines = ["---", "\U0001F9ED Other live Hermes sessions on this machine (%d):" % len(peers)]
    lines += [
        "- '%s' \u2014 active %s" % (_short(peer["name"], _B1_NAME_MAX), _fmt_age(now - peer["last_heartbeat"]))
        for peer in peers
    ]
    lines.append("Coordinate before changing their trees (list_agents / send_agent_message, if enabled).")
    return "\n\n" + "\n".join(lines)


def _b3_block(peers: List[Dict[str, Any]]) -> str:
    names = ", ".join("'%s'" % _short(peer["name"], _B3_NAME_MAX) for peer in peers)
    verb = "sessions are" if len(peers) != 1 else "session is"
    return (
        "\n\n\u26A0 %d other Hermes %s live on this machine (%s).\n"
        "  This was a delete/move \u2014 if the affected path is theirs, send a heads-up before "
        "further changes." % (len(peers), verb, names)
    )


def _is_delete_or_move(command: Any) -> bool:
    """Token-based match on the command's argv shape (``rm -rf /x``, ``/bin/rm``, ``git mv a b``);
    never a substring match, so ``confirm``/``format`` do not fire."""
    if not isinstance(command, str) or not command.strip():
        return False
    try:
        tokens = shlex.split(command, posix=True)
    except ValueError:
        tokens = command.split()
    for token in tokens:
        token = token.strip().strip("'\"")
        if not token:
            continue
        basename = token.rsplit("/", 1)[-1].rsplit("\\", 1)[-1]
        if basename in _DELETE_MOVE_VERBS:
            return True
    return False


# ─── per-session state ────────────────────────────────────────────────────────


def _prune_locked(now: float) -> None:
    for bucket in (_announced, _nudged):
        for key in [key for key, stamp in bucket.items() if now - stamp > _STATE_MAX_AGE]:
            del bucket[key]


def _claim_announce(session_id: str, now: float) -> bool:
    """Check-and-set under the lock: two concurrent first calls produce exactly ONE B1 block."""
    with _lock:
        _prune_locked(now)
        if session_id in _announced:
            return False
        _announced[session_id] = now
        return True


def _claim_nudge(session_id: str, now: float, cooldown: float) -> bool:
    with _lock:
        _prune_locked(now)
        last = _nudged.get(session_id)
        if last is not None and now - last < max(0.0, cooldown):
            return False
        _nudged[session_id] = now
        return True


def _reset_state() -> None:
    """Drop all cached/in-flight state (used by tests and available to callers)."""
    global _peer_cache, _config_cache
    with _lock:
        _announced.clear()
        _nudged.clear()
        _peer_cache = None
        _config_cache = None


# ─── hooks ───────────────────────────────────────────────────────────────────


def _on_transform_tool_result(tool_name: str = "", args: Any = None, result: Any = None,
                               error_type: Any = None, status: Any = None, session_id: Any = "",
                               **_extra: Any) -> Optional[str]:
    """Append an awareness block to a tool result. ``None`` = leave the result untouched; a ``str``
    is the FULL replacement (``result + block``). Never raises."""
    try:
        if _disabled() or not isinstance(result, str):
            return None
        if error_type:
            return None
        if isinstance(status, str) and status.strip().lower() not in _OK_STATUSES:
            return None
        session_key = session_id if isinstance(session_id, str) else ""
        settings = _settings()
        now = time.time()
        peers: Optional[List[Dict[str, Any]]] = None
        block = ""

        if settings.get("announce_first", True):
            peers = _live_peers(session_key, settings, now)
            if peers and _claim_announce(session_key, now):
                block += _b1_block(peers, now)

        command = args.get("command") if isinstance(args, dict) else None
        if settings.get("overlap_nudges", True) and tool_name == "terminal" and _is_delete_or_move(command):
            if peers is None:
                peers = _live_peers(session_key, settings, now)
            if peers:
                try:
                    cooldown = float(settings.get("nudge_cooldown_seconds", DEFAULTS["nudge_cooldown_seconds"]))
                except (TypeError, ValueError):
                    cooldown = float(DEFAULTS["nudge_cooldown_seconds"])
                if _claim_nudge(session_key, now, cooldown):
                    block += _b3_block(peers)

        return result + block if block else None
    except Exception as exc:  # Rule: any doubt -> return None, never raise into dispatch
        logger.debug("session-awareness: transform_tool_result skipped (%s)", exc)
        return None


def _on_session_end(session_id: Any = "", completed: bool = True, interrupted: bool = False,
                    **_extra: Any) -> None:
    """Evict this session's per-session state (gateway processes host many sessions) and
    opportunistically prune entries older than 24 h. Never raises."""
    try:
        if _disabled():
            return
        session_key = session_id if isinstance(session_id, str) else ""
        now = time.time()
        with _lock:
            _announced.pop(session_key, None)
            _nudged.pop(session_key, None)
            _prune_locked(now)
    except Exception as exc:
        logger.debug("session-awareness: on_session_end skipped (%s)", exc)


def register(ctx: Any) -> None:
    """Register the hooks declared in plugin.yaml."""
    for hook_name, callback in (("transform_tool_result", _on_transform_tool_result),
                                ("on_session_end", _on_session_end)):
        try:
            ctx.register_hook(hook_name, callback)
        except Exception as exc:  # a bad ctx must not take the CLI down
            logger.warning("session-awareness: could not register %s (%s)", hook_name, exc)
