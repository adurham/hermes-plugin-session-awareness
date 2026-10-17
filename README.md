# session-awareness

Ambient awareness of the other live Hermes Agent sessions on the same machine: every new
session gets a one-shot list of live peers on its first tool result (B1), and a one-line
nudge after a `terminal` delete/move (`rm`, `rmdir`, `mv`, `truncate` — token match, not
substring) while peers are live (B3).

Peers are read (read-only `SELECT`) from `cross_session_registry` in `$HERMES_HOME/state.db`
(default `~/.hermes/state.db`). The current session, heartbeats older than `max_age_min`,
and provably-dead pids (ESRCH) are excluded. Read-only, never blocks, never raises;
~2 ms cold, well under 1 ms warm.

**B1 is the preventive piece** — it tells a session who else is live *before* it does
anything. B3 is post-hoc by design: it fires *after* a delete/move command has already run;
it is a reminder to coordinate, not a guard (a pre-tool-call gate would be a v2 feature).

Config (optional) under `plugins.session-awareness` in `config.yaml`:
`announce_first: true`, `overlap_nudges: true`, `max_sessions: 6`,
`max_age_min: 720` (minutes), `nudge_cooldown_seconds: 300`.

Kill switch: `SESSION_AWARENESS_DISABLE=1`. Off entirely:
`hermes plugins disable session-awareness`. Loads in new processes only — a running
session keeps its plugins until restart.

Verified against Hermes `v0.22.2+652.g6bb4366`. The `transform_tool_result` kwarg set and
the `cross_session_registry` schema are undocumented internals and may move; the hook
contract is `None` = result unchanged, `str` = full replacement of the tool result.

Caveats:

- The plugin dispatcher keeps the *first* string returned across all plugins, so another
  `transform_tool_result` plugin may swallow this block on a given call.
- B1 announces on the first tool result *after* at least one peer is live (usually the
  literal first result; a session that starts alone still announces if a peer shows up).
- v1 has no per-path matching: in practice a registry `cwd` is often `$HOME`, so
  path-prefix overlap checks could not fire usefully; a path-aware v2 would need
  peer recent-path mining. `write_file`/`patch` are deliberately not B3 triggers.
- `mode=ro` sqlite reads can silently yield zero peers on a WAL database when the `-shm`
  file is not readable — all errors degrade to "no peers". If announcements never appear,
  check `SESSION_AWARENESS_DISABLE`, the plugin enable state, and that `state.db` is
  readable by the process.

## Tests

    python3 -m pytest tests/ -q

13 tests, stdlib-only (a real temp sqlite registry per test; exact dispatcher kwarg set
including unknown-extra-kwarg resilience; a barrier race test asserting exactly one
announcement across 8 concurrent first calls; error/stale/dead-pid/kill-switch cases).
