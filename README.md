# fiber-interop/swoole

Adapter that runs the [`fiber-interop/api`](../api) functions (`async\go`, `await`, `await_all`, `await_any`,
`run`, `sleep`, `readable`, `writable`) on Swoole coroutines, so a library written against the api runs
unchanged for Swoole users.

## Install

```
composer require fiber-interop/swoole
```

Requires `ext-swoole` 6.x (developed against 6.3.0RC1 on PHP 8.5 NTS) or `ext-openswoole`. Composer cannot
express "either extension", so neither is hard-required; `src/register.php` registers the adapter only when
one of them is loaded. Tests must run with the extension loaded, e.g.
`PHP_INI_SCAN_DIR=:/path/to/dir-with-swoole.ini vendor/bin/phpunit` (the conformance runner spawns plain
`php` processes, so `-d extension=` is not enough; use the ini scan dir). Building from PECL source works on
PHP 8.5 (`phpize && ./configure && make`).

## Mapping

| api | Swoole |
|---|---|
| `go($f)` | `Coroutine::create()`; returns a `Task` (result/exception stored, `then()` for other loops). Body runs in the captured `Scope`. |
| `await(Task)` | per-waiter `Channel(1)` fed when the task settles; `pop($timeout)` false -> `async\Timeout`, the task keeps running. Repeatable: the outcome is stored on the Task, never consumed. |
| `await(thenable)` | `then()` + channel, `$done` guard, non-Throwable rejection -> `async\RejectedException`, exceptions rethrown unchanged |
| `sleep()` | `Coroutine::sleep()` (clamped to Swoole's 1 ms minimum) |
| `readable()/writable()` | `System::waitEvent()`; it allows one waiter per fd and mode (a second gets `EEXIST`), so the adapter runs one poller coroutine per fd+mode and fans out to per-waiter channels; the poller is cancelled when the last waiter times out |
| unawaited failure | after one tick without an awaiter the exception is rethrown in the coroutine; Swoole reports it as a fatal error (stderr, non-zero exit) |

## {main} semantics

Swoole needs a scheduler. `go()` works from {main} (the coroutine starts at once). A wait from outside
any coroutine (`await`, `sleep`, `readable`, ...) is wrapped in `Swoole\Coroutine\run()` and the outcome is
carried out as a value (so a `Timeout` does not escape the scheduler). **`Co\run()` returns only when every
coroutine has ended.** `Swoole\Event::exit()` can return earlier but destroys the timers of the remaining
coroutines (they later die with a "deadlock" fatal), so it is not used. Consequently, from {main}:

* a timed-out `await()` does not throw until the leftover tasks have finished, and a task that never ends
  blocks {main}'s wait forever. **Example 04 therefore fails** (output order: "task finished" before the
  timeout message). Inside `run()` / a coroutine it behaves correctly (see `AdapterTest`).
* the CLI's default hook flags include curl hooks; if ext-curl is absent Swoole warns on stdout, so the
  adapter removes the curl flags from `hook_flags` in that case.

## Fibers

Inside a coroutine, a PHP `Fiber` works with Swoole's API (the coroutine suspends). A Fiber started from {main}
sees no coroutine, so the wait wraps `Co\run()` in place (example 15 passes). Fibers are not a unit of
scheduling here; `Fiber::suspend()` is not involved.

## Scope

`Fiber::getCurrent()` is null in every coroutine, so the userland `Scope` polyfill would share one {main} stack
between coroutines. The adapter registers an execution key provider with the polyfill:

```php
\fiber_interop\scope\set_execution_key_provider(fn() => Coroutine::getCid() > 0 ? Coroutine::getContext() : null);
```

so every coroutine gets its own `Scope` stack, keyed on its `Coroutine::getContext()` (the same object for the
coroutine's whole life; the WeakMap entry vanishes when the coroutine ends). This covers raw Swoole switches too
(`Channel`, `Co::sleep` directly), not only the adapter's waits. `go()` propagates the creator's scope into the
task through `$scope->run()`. Examples 13 and 14 pass.

With the `ext-scope` C extension the provider function is a no-op, and the extension cannot see coroutine
switches (they are not fiber switches): use the polyfill with Swoole.

## OpenSwoole

Not tested (not installable here). The adapter uses `Swoole\Coroutine`, `Coroutine\Channel`,
`Coroutine\System::waitEvent`, `Coroutine\run`, `Coroutine::cancel`, `swoole_strerror`; OpenSwoole 22+ provides
these under `OpenSwoole\...` and, with its short-name/alias settings, `Swoole\...`. Without aliases the adapter
would need `class_alias`es; expect differences in `waitEvent` semantics.

## Tests

`vendor/bin/phpunit` (with the extension loaded): `ConformanceTest` runs the api examples (all 17 examples), `AdapterTest` covers repeatable awaits, concurrent awaiters, timeouts, stream multiplexing and
per-coroutine scope.
