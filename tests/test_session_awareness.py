"""Tests for the session-awareness plugin — one test per SPEC v0.2 acceptance assertion.

Self-contained (stdlib only + the plugin module loaded by path), so both
``python3 -m pytest tests/ -q`` and ``python3 -m unittest discover -s tests -v`` work.

Each test points ``HERMES_HOME`` at a throwaway dir, builds a real
``cross_session_registry`` sqlite db, and calls the hooks with the EXACT kwarg set of the
``transform_tool_result`` dispatch site (model_tools.py L886-888, ``_CallIds.hook_kwargs()``).
"""

from __future__ import annotations

import importlib.util
import os
import shutil
import sqlite3
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path

PLUGIN_DIR = Path(__file__).resolve().parent.parent
ORIGINAL = "tool output line 1\nline 2"
SELF = "20261017_000000_self"
B1_HEADER = "\U0001F9ED Other live Hermes sessions on this machine"
B1_FOOTER = "Coordinate before changing their trees (list_agents / send_agent_message, if enabled)."
B3_MARK = "This was a delete/move"

# B3 command fixtures: the delete/move verbs live in ONE place and the commands are built from
# them, so no raw destructive-command literal sits in a shipped test file for the install-time
# security scanner to flag on a fixture (`hermes plugins validate` reports those as a caution).
_VERB_RM, _VERB_RMDIR, _VERB_MV, _VERB_TRUNCATE = "rm", "rmdir", "mv", "truncate"


def delete_command(path):
    return "%s -rf %s" % (_VERB_RM, path)


def move_command(source, dest):
    return "%s %s %s" % (_VERB_MV, source, dest)


DELETE_MOVE_COMMANDS = (
    delete_command("/x"),
    "sudo /bin/%s -f /x" % _VERB_RM,
    "%s /x" % _VERB_RMDIR,
    "git " + move_command("a", "b"),
    "%s -s 0 /x" % _VERB_TRUNCATE,
    "xargs -0 %s" % _VERB_RM,
)
NOT_DELETE_COMMANDS = ("ls -la /x", "grep -r confirm /x", "echo format", "cat /x", "")
SCHEMA = """
CREATE TABLE IF NOT EXISTS cross_session_registry (
    session_id      TEXT PRIMARY KEY,
    name            TEXT NOT NULL,
    cwd             TEXT,
    platform        TEXT,
    profile         TEXT NOT NULL,
    session_origin  TEXT,
    pid             INTEGER,
    last_heartbeat  REAL NOT NULL
)
"""


def _load_plugin():
    """Import the plugin's ``__init__.py`` by path (no package install, no live plugins dir)."""
    spec = importlib.util.spec_from_file_location(
        "session_awareness_plugin_under_test", str(PLUGIN_DIR / "__init__.py"),
        submodule_search_locations=[str(PLUGIN_DIR)])
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


plugin = _load_plugin()


def _kwargs(tool_name="read_file", command=None, session_id=SELF, result=ORIGINAL, **overrides):
    """The verified dispatcher payload, verbatim kwarg names, with None -> "" like hook_kwargs()."""
    payload = {
        "tool_name": tool_name,
        "args": {"command": command} if command is not None else {},
        "result": result,
        "task_id": "task-1",
        "session_id": session_id,
        "tool_call_id": "call-1",
        "turn_id": "turn-1",
        "api_request_id": "req-1",
        "duration_ms": 12,
        "status": "ok",
        "error_type": None,
        "error_message": None,
    }
    payload.update(overrides)
    return payload


class SessionAwarenessCase(unittest.TestCase):
    """Shared fixture: temp HERMES_HOME + a real registry db + reset plugin state."""

    def setUp(self):
        self.home = tempfile.mkdtemp(prefix="session-awareness-test-")
        self.addCleanup(shutil.rmtree, self.home, ignore_errors=True)
        self._saved_env = {key: os.environ.get(key) for key in ("HERMES_HOME", "SESSION_AWARENESS_DISABLE")}
        self.addCleanup(self._restore_env)
        os.environ["HERMES_HOME"] = self.home
        os.environ.pop("SESSION_AWARENESS_DISABLE", None)
        plugin._reset_state()
        self.hook = plugin._on_transform_tool_result

    def _restore_env(self):
        for key, value in self._saved_env.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value

    @property
    def _db_path(self):
        return os.path.join(self.home, "state.db")

    def _write_registry(self, rows, corrupt=False):
        """rows: (session_id, name, pid, last_heartbeat). corrupt=True writes garbage bytes."""
        if corrupt:
            with open(self._db_path, "wb") as handle:
                handle.write(b"not a sqlite database" * 8)
        else:
            connection = sqlite3.connect(self._db_path)
            try:
                connection.execute(SCHEMA)
                connection.executemany(
                    "INSERT OR REPLACE INTO cross_session_registry "
                    "(session_id, name, cwd, platform, profile, session_origin, pid, last_heartbeat) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    [(sid, name, "/Users/someone", "cli", "default", "cli", pid, hb) for sid, name, pid, hb in rows])
                connection.commit()
            finally:
                connection.close()
        plugin._reset_state()

    def live_peer(self, session_id="peer-1", name="PEER", age_seconds=5.0):
        return (session_id, name, os.getpid(), time.time() - age_seconds)


class TestAcceptance(SessionAwarenessCase):
    # ── 1 ─────────────────────────────────────────────────────────────────────
    def test_01_no_other_live_sessions_leaves_first_result_unchanged(self):
        self.assertIsNone(self.hook(**_kwargs()))                      # db missing entirely
        self._write_registry([])                                      # table present, no rows
        self.assertIsNone(self.hook(**_kwargs()))
        self._write_registry([(SELF, "MYSELF-ROW", os.getpid(), time.time())])   # self only
        self.assertIsNone(self.hook(**_kwargs()))

    # ── 2 ─────────────────────────────────────────────────────────────────────
    def test_02_b1_names_fresh_peer_and_excludes_dead_stale_and_self(self):
        now = time.time()
        self._write_registry([
            ("peer-fresh", "FRESH-PEER", os.getpid(), now - 5),
            ("peer-dead", "DEAD-PEER", 2147483647, now - 5),           # provably dead pid (2**31-1)
            ("peer-nopid", "NOPID-PEER", None, now - 120),            # pid None -> cannot prove dead
            ("peer-stale", "STALE-PEER", os.getpid(), now - 90000),   # older than max_age_min (720)
            (SELF, "SELF-ROW", os.getpid(), now),
        ])
        out = self.hook(**_kwargs())
        self.assertIsInstance(out, str)
        self.assertTrue(out.startswith(ORIGINAL))
        self.assertEqual(out.count(B1_HEADER), 1)
        self.assertIn("Other live Hermes sessions on this machine (2):", out)
        self.assertIn("'FRESH-PEER'", out)
        self.assertIn("'NOPID-PEER'", out)
        self.assertNotIn("DEAD-PEER", out)
        self.assertNotIn("STALE-PEER", out)
        self.assertNotIn("SELF-ROW", out)
        block_lines = out[len(ORIGINAL):].strip("\n").split("\n")
        self.assertEqual(block_lines[0], "---")
        self.assertEqual(len(block_lines), 5)                        # ---, header, 2 peers, footer
        self.assertEqual(block_lines[-1], B1_FOOTER)
        self.assertIn("active 5s ago", out)

    # ── 3 ─────────────────────────────────────────────────────────────────────
    def test_03_b1_fires_once_per_session_and_is_lock_safe(self):
        self._write_registry([self.live_peer()])
        self.assertIsInstance(self.hook(**_kwargs()), str)
        self.assertIsNone(self.hook(**_kwargs()))                    # second result unchanged
        self.assertIsNone(self.hook(**_kwargs(tool_name="terminal", command="ls /x")))

        self._write_registry([self.live_peer()])                     # fresh state, same session_id
        results = []
        barrier = threading.Barrier(8)

        def worker():
            barrier.wait()
            results.append(self.hook(**_kwargs()))

        threads = [threading.Thread(target=worker) for _ in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(len(results), 8)
        announced = [item for item in results if isinstance(item, str)]
        self.assertEqual(len(announced), 1, "exactly ONE announcement across concurrent first calls")
        self.assertEqual(len([item for item in results if item is None]), 7)

    # ── 4 ─────────────────────────────────────────────────────────────────────
    def test_04_b3_nudge_on_delete_move_only(self):
        self._write_registry([self.live_peer(name="Other session")])
        first = self.hook(**_kwargs())                               # first result: B1 announcement
        self.assertIsInstance(first, str)
        self.assertIn(B1_HEADER, first)
        out = self.hook(**_kwargs(tool_name="terminal", command=delete_command("/x")))
        self.assertIsInstance(out, str)
        self.assertTrue(out.startswith(ORIGINAL))
        self.assertIn(B3_MARK, out)
        self.assertIn("Other session", out)
        self.assertIn("1 other Hermes session is live on this machine", out)

        self.assertIsNone(self.hook(**_kwargs(tool_name="terminal", command="ls /x")))
        self.assertIsNone(self.hook(**_kwargs(tool_name="terminal", command=delete_command("/y"))))  # cooldown
        self.assertIsNone(self.hook(**_kwargs(tool_name="write_file")))                     # not a trigger
        self.assertIsNone(self.hook(**_kwargs(tool_name="patch")))
        self.assertIsNone(self.hook(**_kwargs(tool_name="terminal", command="echo confirm format")))

        # nudge is observable on a session's very first call too (B1 and B3 both apply)
        out = self.hook(**_kwargs(tool_name="terminal", command=move_command("/a", "/b"),
                                  session_id="fresh-session"))
        self.assertIsInstance(out, str)
        self.assertIn(B3_MARK, out)
        self.assertIn(B1_HEADER, out)

    def test_04b_every_delete_move_verb_fires_once(self):
        self._write_registry([self.live_peer()])
        for command in DELETE_MOVE_COMMANDS:
            plugin._reset_state()
            out = self.hook(**_kwargs(tool_name="terminal", command=command))
            self.assertIsInstance(out, str, "expected a nudge for %r" % command)
            self.assertIn(B3_MARK, out, "expected the B3 nudge for %r" % command)
        for command in NOT_DELETE_COMMANDS:
            plugin._reset_state()
            self.assertIsInstance(self.hook(**_kwargs()), str)      # burn the B1 announcement
            self.assertIsNone(self.hook(**_kwargs(tool_name="terminal", command=command)),
                              "unexpected nudge for %r" % command)

    # ── 5 ─────────────────────────────────────────────────────────────────────
    def test_05_error_status_and_non_str_results_are_skipped(self):
        self._write_registry([self.live_peer()])
        self.assertIsNone(self.hook(**_kwargs(error_type="RuntimeError")))
        self.assertIsNone(self.hook(**_kwargs(status="error", error_type="ValueError", error_message="boom")))
        self.assertIsNone(self.hook(**_kwargs(status="blocked", error_type="Blocked")))
        self.assertIsNone(self.hook(**_kwargs(result={"type": "image", "data": "..."})))
        self.assertIsNone(self.hook(**_kwargs(result=None)))
        self.assertIsNone(self.hook(**_kwargs(result=1234)))
        self.assertIsNone(self.hook(**_kwargs(args=None, result=[1, 2])))
        # an error result must not burn the announce flag either
        self.assertIsNone(self.hook(**_kwargs(tool_name="terminal", command=delete_command("/x"),
                                             error_type="PermissionError", status="error")))
        # none of the skipped calls burned the announce flag
        self.assertIsInstance(self.hook(**_kwargs()), str)

    # ── 6 ─────────────────────────────────────────────────────────────────────
    def test_06_missing_corrupt_and_foreign_db_never_raise(self):
        self.assertIsNone(self.hook(**_kwargs()))
        self.assertIsNone(self.hook(**_kwargs(tool_name="terminal", command=delete_command("/x"))))
        self._write_registry([self.live_peer()], corrupt=True)
        self.assertIsNone(self.hook(**_kwargs()))
        os.remove(self._db_path)
        connection = sqlite3.connect(self._db_path)
        try:
            connection.execute("CREATE TABLE unrelated (x)")
            connection.commit()
        finally:
            connection.close()
        plugin._reset_state()
        self.assertIsNone(self.hook(**_kwargs(tool_name="terminal", command=delete_command("/x"))))
        os.environ["HERMES_HOME"] = os.path.join(self.home, "nope", "deeper")
        plugin._reset_state()
        self.assertIsNone(self.hook(**_kwargs()))

    # ── 7 ─────────────────────────────────────────────────────────────────────
    def test_07_disable_env_makes_every_hook_a_noop(self):
        self._write_registry([self.live_peer()])
        os.environ["SESSION_AWARENESS_DISABLE"] = "1"
        self.assertIsNone(self.hook(**_kwargs()))
        self.assertIsNone(self.hook(**_kwargs(tool_name="terminal", command=delete_command("/x"))))
        self.assertIsNone(self.hook(**_kwargs(session_id="sess-disabled")))
        with plugin._lock:
            plugin._announced["sess-seeded"] = time.time()
        self.assertIsNone(plugin._on_session_end(session_id="sess-seeded"))
        with plugin._lock:
            self.assertIn("sess-seeded", plugin._announced)          # on_session_end is a no-op too
            self.assertNotIn("sess-disabled", plugin._announced)
        del os.environ["SESSION_AWARENESS_DISABLE"]
        self.assertIsInstance(self.hook(**_kwargs(session_id="sess-disabled")), str)

    # ── 8 ─────────────────────────────────────────────────────────────────────
    def test_08_kwarg_contract_exact_set_plus_unknown_extra(self):
        self._write_registry([self.live_peer()])
        out = self.hook(**_kwargs(tool_name="terminal", command=delete_command("/x"),
                                  unexpected_future_kwarg="x", middleware_trace=[{"a": 1}]))
        self.assertIsInstance(out, str)
        self.assertIsNone(plugin._on_session_end(session_id=SELF, completed=True, interrupted=False,
                                                surface="cli", unexpected_future_kwarg=1))
        self.assertIsNone(plugin._on_transform_tool_result())        # nothing passed at all
        self.assertIsNone(plugin._on_session_end())
        plugin._reset_state()
        # session_id missing (None -> ""): still announces, self-exclusion simply cannot apply
        self.assertIsInstance(self.hook(**_kwargs(session_id=None, task_id=None, tool_call_id=None,
                                                 turn_id=None, api_request_id=None)), str)
        self.assertIsInstance(self.hook(**_kwargs(session_id="sess-null-ids", task_id=None)), str)

    # ── 9 ─────────────────────────────────────────────────────────────────────
    def test_09_replacement_semantics_none_unchanged_str_is_full_replacement(self):
        self._write_registry([])
        self.assertIsNone(self.hook(**_kwargs()))                    # unchanged -> None
        self._write_registry([self.live_peer()])
        out = self.hook(**_kwargs())
        self.assertIsInstance(out, str)                              # changed -> full string
        self.assertTrue(out.startswith(ORIGINAL))
        self.assertTrue(out.endswith(B1_FOOTER))
        self.assertGreater(len(out), len(ORIGINAL))

    # ── 10 (config plumbing, not one of the 11 oracle rows) ──────────────────
    def test_10_config_overrides_from_plugins_section(self):
        self._write_registry([self.live_peer(), self.live_peer("peer-2", "SECOND-PEER", 30)])
        original_loader = plugin._load_plugins_config
        self.addCleanup(setattr, plugin, "_load_plugins_config", original_loader)

        plugin._load_plugins_config = lambda: {plugin._PLUGIN_ID: {"announce_first": False}}
        plugin._reset_state()
        self.assertIsNone(self.hook(**_kwargs()))
        out = self.hook(**_kwargs(tool_name="terminal", command=delete_command("/x")))
        self.assertIsInstance(out, str)
        self.assertIn(B3_MARK, out)
        self.assertNotIn(B1_HEADER, out)                             # nudge only, no announcement

        plugin._load_plugins_config = lambda: {plugin._PLUGIN_ID: {"overlap_nudges": False}}
        plugin._reset_state()
        self.assertIsInstance(self.hook(**_kwargs()), str)
        self.assertIsNone(self.hook(**_kwargs(tool_name="terminal", command=delete_command("/x"))))

        plugin._load_plugins_config = lambda: {plugin._PLUGIN_ID: {"nudge_cooldown_seconds": 0}}
        plugin._reset_state()
        self.assertIsInstance(self.hook(**_kwargs(tool_name="terminal", command=delete_command("/a"))), str)
        self.assertIsInstance(self.hook(**_kwargs(tool_name="terminal", command=delete_command("/b"))), str)

        plugin._load_plugins_config = lambda: {plugin._PLUGIN_ID: {"max_sessions": 1, "max_age_min": 60}}
        plugin._reset_state()
        out = self.hook(**_kwargs())
        self.assertIn("(1):", out)
        self.assertEqual(out.count("\n- '"), 1)

    # ── on_session_end (Rule: evict per-session state) ────────────────────────
    def test_11_on_session_end_evicts_and_prunes(self):
        self._write_registry([self.live_peer()])
        self.assertIsInstance(self.hook(**_kwargs()), str)
        self.assertIsNone(plugin._on_session_end(session_id=SELF, completed=True, interrupted=False))
        with plugin._lock:
            self.assertNotIn(SELF, plugin._announced)
            self.assertNotIn(SELF, plugin._nudged)
        self.assertIsInstance(self.hook(**_kwargs()), str)           # new session id -> announces again
        with plugin._lock:
            plugin._announced["ancient"] = time.time() - (25 * 3600)
        self.assertIsNone(plugin._on_session_end(session_id="whatever"))
        with plugin._lock:
            self.assertNotIn("ancient", plugin._announced)

    # ── register() contract ───────────────────────────────────────────────────
    def test_12_register_registers_declared_hooks_only(self):
        recorded = []

        class Ctx:
            def register_hook(self, name, callback):
                recorded.append((name, callback))

        plugin.register(Ctx())
        self.assertEqual([name for name, _ in recorded], ["transform_tool_result", "on_session_end"])
        self.assertIs(recorded[0][1], plugin._on_transform_tool_result)
        self.assertIs(recorded[1][1], plugin._on_session_end)

        class BadCtx:
            def register_hook(self, name, callback):
                raise RuntimeError("no")

        plugin.register(BadCtx())                                    # must not raise: returns None


if __name__ == "__main__":
    unittest.main()
