# fiber-interop/swoole

Adapter that runs the [`fiber-interop/api`](../api) functions (`async\go`, `await`, `await_all`, `await_any`,
`run`, `sleep`, `readable`, `writable`) on Swoole coroutines, so a library written against the api runs
unchanged for Swoole users.

## Install

```
composer require fiber-interop/swoole
```

Requires `ext-swoole` 6.x (developed against 6.3.0RC1 on PHP 8.5 NTS) or `ext-openswoole` (tested 26.2.0). Composer cannot
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
| `service($f)` | `Coroutine::create()` tracked as a service; cancelled with `Coroutine::cancel()` when the outermost `run()` returns |
| unawaited failure | after one tick without an awaiter the exception is rethrown in the coroutine; Swoole reports it as a fatal error (stderr, non-zero exit) |

## run(), the scheduler and services

Every adapter call happens inside an api `run()`; the outermost `run()` starts with `go()` then `await()` from
{main}, and that first `await()` is where the adapter wraps the single `Swoole\Coroutine\run()` (the outcome is
carried out as a value, so a `Timeout` does not escape the scheduler). `Co\run()` returns only when every
coroutine has ended, so the adapter ends it itself once the work is done. Nested and concurrent `run()`s need no
special handling: the api keeps its frames per coroutine through `\async\Registry::$executionKey`, which the
adapter sets to the same provider it gives the Scope polyfill (see Scope).

**`service($f)` tasks are cancelled when the outermost `run()` returns** (allowed by the spec: runtime-specific,
services are restartable). The adapter counts the running `go()` tasks; when the last one ends inside its
`Co\run()` wrapper it calls `Coroutine::cancel()` on every service coroutine. Cancellation surfaces in the
adapter's waits (`Coroutine::sleep`, `Channel::pop`, which the stream waits and awaits use) as an exception
implementing `async\CancelledException` (`FiberInterop\Swoole\Cancelled`); the service coroutine swallows it and
ends cleanly, so `Co\run()` returns without warnings. A service that catches the exception and keeps going
would keep `Co\run()` open. A service that fails with nobody awaiting it is reported by Swoole as a fatal error
(stderr, non-zero exit); before rethrowing, the adapter closes every channel with a blocked consumer so Swoole
prints no `Channel::~Channel() ... consumers will be discarded` warning on stdout.

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

Tested with OpenSwoole 26.2.0 (built from PECL on PHP 8.5.11 NTS): phpunit (11 tests) and all 20 conformance
examples pass, same as on Swoole 6.3.0RC1. One code path serves both; the differences found:

- OpenSwoole aliases `Swoole\Coroutine`, `Coroutine\Channel`, `Coroutine\System` and the `SWOOLE_*` constants,
  so the class names need no mapping. It has no `Co\run()`/`Swoole\Coroutine\run()` function and no `swoole_strerror()`.
- `Coroutine::sleep()` takes whole seconds only (a float is a `TypeError`). The adapter sleeps with a timed
  `Channel::pop()` instead.
- `Coroutine::run()` refuses to start (and segfaults in 26.2.0) when a coroutine was already created outside
  one, which `go()` before the first `await()` does. On OpenSwoole the adapter therefore starts its loop with
  `Coroutine::create()` + `Event::wait()`, setting `hook_flags` to `SWOOLE_HOOK_ALL` as `run()` would. The
  runner is picked once in the constructor.
- `System::waitEvent` behaves the same (stream examples 11 and 12 pass).

Select the extension with `PHP_INI_SCAN_DIR=:<repo>/build/ini` (Swoole) or `:<repo>/build/ini-openswoole`; never load both.

## Tests

`vendor/bin/phpunit` (with the extension loaded): `ConformanceTest` runs the api examples (all 20 examples), `AdapterTest` covers repeatable awaits, concurrent awaiters, timeouts, stream multiplexing,
service cancellation, nested `run()` in two coroutines and per-coroutine scope.
