<?php

declare(strict_types=1);

namespace FiberInterop\Swoole;

use async\RejectedException;
use async\Timeout;
use Swoole\Coroutine;
use Swoole\Coroutine\Channel;
use Swoole\Coroutine\System;

/**
 * fiber-interop/api on Swoole coroutines. See README.md for the run(), service, Fiber and Scope semantics.
 */
final class Adapter implements \async\Adapter
{
    /** Seconds one System::waitEvent() poll lasts; an unwatched poller exits, and a locally closed stream is noticed, within this time. */
    private const POLL_SLICE = 0.01;
    /** @var array<string, array{waiters: array<int, Channel>}> stream wait groups, key "<resource id>:<r|w>" */
    private array $streams = [];
    private int $nextWaiter = 0;
    /** @var array<int, int> cids of service() coroutines */
    private array $services = [];
    /** go() tasks still running; when it reaches 0 inside our Co\run() wrapper the run is over */
    private int $live = 0;
    private int $wrapDepth = 0;
    /** @var array<int, Channel> channels with a blocked consumer, closed before an unhandled error ends the process */
    private static array $waiting = [];

    /**
     * Swoole: Co\run(). OpenSwoole has no Co\run function, and its Coroutine::run() refuses to start (and
     * segfaults) once a coroutine was created outside one, which go() does before the first await. There the
     * runner is create() + Event::wait(), with the hook flags Coroutine::run() would have set.
     */
    private \Closure $runner;

    public function __construct()
    {
        $this->runner = \function_exists('Swoole\Coroutine\run')
            ? \Closure::fromCallable('Swoole\Coroutine\run')
            : static function (callable $f): void {
                if (!(Coroutine::getOptions()['hook_flags'] ?? 0)) {
                    Coroutine::set(['hook_flags' => \SWOOLE_HOOK_ALL]);
                }
                Coroutine::create($f);
                \OpenSwoole\Event::wait();
            };
        // Co\run() enables SWOOLE_HOOK_ALL. Two curl hooks are masked out of it:
        //  - without ext-curl, every curl hook warns on stdout for each curl function;
        //  - the legacy SWOOLE_HOOK_CURL (in SWOOLE_HOOK_ALL on builds without --enable-swoole-curl) swaps
        //    CurlHandle for Swoole\Curl\Handler: a TypeError for handles made outside a coroutine and a segfault
        //    with curl_multi. Only the native hook (SWOOLE_HOOK_NATIVE_CURL, real libcurl) is kept.
        if (Coroutine::getCid() < 0 && \defined('SWOOLE_HOOK_ALL')) {
            $native = \defined('SWOOLE_HOOK_NATIVE_CURL') ? \SWOOLE_HOOK_NATIVE_CURL : 0;
            $legacy = \defined('SWOOLE_HOOK_CURL') ? \SWOOLE_HOOK_CURL : 0;
            $flags = \SWOOLE_HOOK_ALL & ~$legacy;
            if (!\extension_loaded('curl')) {
                $flags &= ~$native;
            }
            // Always set explicitly: when nothing is masked, Co\run() on Swoole 6.3 leaves the first run
            // unhooked if hook_flags was never set (later runs are hooked), so the first run() would block.
            Coroutine::set(['hook_flags' => $flags]);
        }
        // Scope's polyfill keys its stacks on this object (the coroutine's context dies with the coroutine).
        if (\function_exists('fiber_interop\scope\set_execution_key_provider')) {
            \fiber_interop\scope\set_execution_key_provider(
                static fn(): ?object => Coroutine::getCid() > 0 ? Coroutine::getContext() : null,
            );
        }
        \async\Registry::$executionKey = static fn(): ?object => Coroutine::getCid() > 0 ? Coroutine::getContext() : null;
    }

    /**
     * Pop with a timeout. Returns null on timeout; throws Cancelled when the coroutine was cancelled
     * or the channel closed (a service being stopped, or the process ending on an unhandled error).
     */
    public static function pop(Channel $ch, float $timeout): mixed
    {
        $id = \spl_object_id($ch);
        self::$waiting[$id] = $ch;
        try {
            $r = $ch->pop($timeout === \PHP_FLOAT_MAX ? -1 : \max(0.001, $timeout));
        } finally {
            unset(self::$waiting[$id]);
            self::activate();
        }
        if ($r === false) {
            if ($ch->errCode !== \SWOOLE_CHANNEL_TIMEOUT) {
                throw new Cancelled('Cancelled');
            }
            return null;
        }
        return $r;
    }

    /**
     * Swoole has no userland coroutine-switch hook, so scope superglobals are re-activated at the points where
     * this adapter regains control: coroutine start, after every wait, after the outermost run returns.
     */
    private static function activate(): void
    {
        if (\function_exists('fiber_interop\scope\activate')) {
            \fiber_interop\scope\activate();
        }
    }

    public function go(\Closure $f): object
    {
        $scope = \Scope::getCurrent();
        return $this->spawn(static fn() => $scope->run($f), false);
    }

    /** Services are cancelled when the outermost run() returns; they are restartable by contract. */
    public function service(\Closure $f): object
    {
        return $this->spawn($f, true);
    }

    /**
     * Swoole gives each coroutine its own output buffers, so output from tasks would bypass an ob_start() the
     * caller of the outermost run() opened. While that caller has a buffer open, each coroutine started here
     * collects its output (chunk size 1 keeps the order across coroutines) and run() echoes it on return.
     */
    private ?string $output = null;

    private function captureOutput(): bool
    {
        return $this->output !== null && \ob_start(function (string $buffer): string {
            $this->output .= $buffer;
            return '';
        }, 1);
    }

    private static function releaseOutput(bool $captured): void
    {
        while ($captured && \ob_get_level() > 0) {
            \ob_end_flush();
        }
    }

    private function spawn(\Closure $f, bool $service): Task
    {
        $task = new Task();
        $service || $this->live++;
        if (Coroutine::getCid() < 0 && \ob_get_level() > 0) {
            $this->output ??= ''; // started from {main}: echoed when the wait() that drives it returns
        }
        $cid = Coroutine::create(function () use ($f, $task, $service): void {
            self::activate();
            $captured = $this->captureOutput();
            $cid = Coroutine::getCid();
            $service && $this->services[$cid] = $cid;
            try {
                $task->settle(true, $f());
            } catch (Cancelled $e) {
                $task->settle(false, $e);
            } catch (\Throwable $e) {
                $task->settle(false, $e);
                self::pop(new Channel(1), 0.001); // give an awaiter one tick to claim the failure
                if (!$task->handled) {
                    // Swoole reports it as a fatal error: stderr and non-zero exit. Wake blocked
                    // consumers first so no Channel is destroyed with a waiter (a stdout warning).
                    foreach (self::$waiting as $ch) {
                        $ch->close();
                    }
                    throw $e;
                }
            } finally {
                unset($this->services[$cid]);
                if (!$service && --$this->live === 0 && $this->wrapDepth > 0) {
                    $this->cancelServices();
                }
                self::releaseOutput($captured);
            }
        });
        return $task;
    }

    private function cancelServices(): void
    {
        foreach ($this->services as $cid) {
            Coroutine::cancel($cid);
        }
    }

    public function canAwait(object $o): bool
    {
        return $o instanceof Task || \method_exists($o, 'then');
    }

    public function await(object $o, float $timeout): mixed
    {
        if ($o instanceof Task) {
            $o->handled = true;
            return $this->wait(static fn() => $o->pop($timeout), $timeout);
        }
        return $this->wait(static function () use ($o, $timeout): ?array {
            $ch = new Channel(1);
            $done = false;
            $settle = static function (bool $ok, mixed $v) use (&$done, $ch): void {
                if (!$done) {
                    $done = true;
                    $ch->push([$ok, $v]);
                }
            };
            $o->then(
                static fn($v) => $settle(true, $v),
                static fn($r) => $settle(false, $r instanceof \Throwable ? $r : new RejectedException($r)),
            );
            try {
                return self::pop($ch, $timeout);
            } finally {
                $done = true; // a later result is ignored
            }
        }, $timeout);
    }

    public function sleep(float $seconds): void
    {
        $this->wait(static fn() => null, \PHP_FLOAT_MAX, $seconds);
    }

    public function readable(mixed $resource, float $timeout): void
    {
        $this->waitStream($resource, 'r', $timeout);
    }

    public function writable(mixed $resource, float $timeout): void
    {
        $this->waitStream($resource, 'w', $timeout);
    }

    /**
     * System::waitEvent() allows one waiter per fd and mode, so the first waiter starts a poller
     * coroutine and every waiter (it included) waits on a private channel. The poller is never cancelled:
     * Coroutine::cancel() on a coroutine whose event is becoming ready crashes Swoole 6.3.0RC1 (see README,
     * "Known upstream issues"). It polls in short slices instead and exits by itself at the first slice
     * boundary where nobody waits. It holds $resource, so the resource id cannot be reused meanwhile.
     */
    private function waitStream(mixed $resource, string $mode, float $timeout): void
    {
        $this->wait(function () use ($resource, $mode, $timeout): ?array {
            $key = \get_resource_id($resource) . ':' . $mode;
            $ch = new Channel(1);
            $id = $this->nextWaiter++;
            if (!isset($this->streams[$key])) {
                $this->streams[$key] = ['waiters' => []];
                Coroutine::create(function () use ($resource, $mode, $key): void {
                    $event = $mode === 'r' ? \SWOOLE_EVENT_READ : \SWOOLE_EVENT_WRITE;
                    $watch = $this->dupStream($resource, $mode);
                    try {
                    while (true) {
                        if (!\is_resource($resource)) {
                            // Closed locally: wake everyone normally; their next operation on the resource fails.
                            $waiters = $this->streams[$key]['waiters'];
                            unset($this->streams[$key]);
                            foreach ($waiters as $w) {
                                $w->push([true, null]);
                            }
                            return;
                        }
                        $t = \microtime(true);
                        $r = System::waitEvent($watch, $event, self::POLL_SLICE);
                        if ($r === false && \microtime(true) - $t >= self::POLL_SLICE * 0.5) {
                            if (!$this->streams[$key]['waiters']) {
                                unset($this->streams[$key]); // no yield between the check and the unset
                                return;
                            }
                            continue; // slice elapsed, waiters remain
                        }
                        $waiters = $this->streams[$key]['waiters'];
                        unset($this->streams[$key]);
                        foreach ($waiters as $w) {
                            $w->push($r === false ? [false, new \RuntimeException('waitEvent failed' . (\function_exists('swoole_strerror') ? ': ' . \swoole_strerror(\swoole_last_error()) : ''))] : [true, null]);
                        }
                        return;
                    }
                    } finally {
                        if ($watch !== $resource) {
                            \fclose($watch);
                        }
                    }
                });
            }
            $this->streams[$key]['waiters'][$id] = $ch;
            try {
                return self::pop($ch, $timeout);
            } finally {
                unset($this->streams[$key]['waiters'][$id]); // the poller notices the empty group by itself
            }
        }, $timeout);
    }

    /** @var array<int, int> resource id => fd number found in /proc/self/fd */
    private array $fds = [];

    /**
     * A dup of $resource's fd to hand to waitEvent(): if the caller fclose()s the original while a waitEvent() slice is
     * pending, the reactor would otherwise log "failed to delete events ... Bad file descriptor" when the slice ends.
     * PHP has no stream-to-fd API, so on Linux the fd is found in /proc/self/fd by inode (cached per resource id);
     * elsewhere the resource itself is waited on and that warning can appear.
     */
    private function dupStream(mixed $resource, string $mode): mixed
    {
        $st = \fstat($resource);
        if ($st === false) {
            return $resource; // no stat for this stream (e.g. pg_socket()): wait on the resource itself
        }
        $fd = $this->fds[\get_resource_id($resource)] ?? null;
        if ($fd === null || !\file_exists("/proc/self/fd/$fd") || \stat("/proc/self/fd/$fd")['ino'] !== $st['ino']) {
            $fd = null;
            foreach (\is_dir('/proc/self/fd') ? \scandir('/proc/self/fd') : [] as $n) {
                if (\ctype_digit($n) && \file_exists("/proc/self/fd/$n") && \stat("/proc/self/fd/$n")['ino'] === $st['ino']) {
                    $fd = (int) $n;
                    break;
                }
            }
            if ($fd === null) {
                return $resource;
            }
            $this->fds[\get_resource_id($resource)] = $fd;
        }
        return \fopen("php://fd/$fd", $mode === 'r' ? 'r' : 'w') ?: $resource;
    }

    /**
     * Run $body (blocks on a channel; returns [ok, value], or null on timeout) inside a coroutine.
     * Outside any coroutine (the outermost run()'s first await, or a foreign Fiber) the single Co\run()
     * wrapper is started for it and the outcome is carried out as a value, so a Timeout does not escape
     * the scheduler. Co\run() returns only when every coroutine has ended; services are cancelled once
     * the last go() task has.
     */
    private function wait(\Closure $body, float $timeout, ?float $sleep = null): mixed
    {
        $run = function () use ($body, $timeout, $sleep): array {
            try {
                if ($sleep !== null) {
                    // Not Coroutine::sleep(): OpenSwoole's takes int seconds. A pop that times out is a float sleep.
                    self::pop(new Channel(1), \max(0.001, $sleep));
                    $r = [true, null];
                } else {
                    $r = $body() ?? [false, new Timeout(\sprintf('Timed out after %.3fs', $timeout))];
                }
            } catch (\Throwable $e) {
                $r = [false, $e];
            }
            return $r;
        };
        if (Coroutine::getCid() >= 0) {
            [$ok, $value] = $run();
        } else {
            $out = null;
            $this->wrapDepth++;
            $this->output ??= \ob_get_level() > 0 ? '' : null;
            try {
                ($this->runner)(function () use ($run, &$out): void {
                    $captured = $this->captureOutput();
                    try {
                        $out = $run();
                        $this->live === 0 && $this->cancelServices();
                    } finally {
                        self::releaseOutput($captured);
                    }
                });
            } finally {
                $this->wrapDepth--;
                self::activate();
                if ($this->output !== null) {
                    echo $this->output;
                    $this->output = null;
                }
            }
            [$ok, $value] = $out;
        }
        if (!$ok) {
            throw $value;
        }
        return $value;
    }
}
