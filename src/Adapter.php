<?php

declare(strict_types=1);

namespace FiberInterop\Swoole;

use async\RejectedException;
use async\Timeout;
use Swoole\Coroutine;
use Swoole\Coroutine\Channel;
use Swoole\Coroutine\System;

/**
 * fiber-interop/api on Swoole coroutines. See README.md for the {main}, Fiber and Scope semantics.
 */
final class Adapter implements \async\Adapter
{
    /** @var array<string, array{cid: int, waiters: array<int, Channel>}> stream wait groups, key "<resource id>:<r|w>" */
    private array $streams = [];
    private int $nextWaiter = 0;

    public function __construct()
    {
        // Co\run() enables SWOOLE_HOOK_ALL, which warns on stdout for each curl function when ext-curl is absent.
        if (!\extension_loaded('curl') && Coroutine::getCid() < 0 && \defined('SWOOLE_HOOK_ALL')) {
            Coroutine::set(['hook_flags' => \SWOOLE_HOOK_ALL & ~\SWOOLE_HOOK_CURL & ~(\defined('SWOOLE_HOOK_NATIVE_CURL') ? \SWOOLE_HOOK_NATIVE_CURL : 0)]);
        }
        // Scope's polyfill keys its stacks on this object (the coroutine's context dies with the coroutine).
        if (\function_exists('fiber_interop\scope\set_execution_key_provider')) {
            \fiber_interop\scope\set_execution_key_provider(
                static fn(): ?object => Coroutine::getCid() > 0 ? Coroutine::getContext() : null,
            );
        }
    }

    public function go(\Closure $f): object
    {
        $task = new Task();
        $scope = \Scope::getCurrent();
        Coroutine::create(function () use ($f, $task, $scope): void {
            try {
                $task->settle(true, $scope->run($f));
            } catch (\Throwable $e) {
                $task->settle(false, $e);
                Coroutine::sleep(0.001); // give an awaiter one tick to claim the failure
                if (!$task->handled) {
                    throw $e; // Swoole reports it as a fatal error: stderr and non-zero exit
                }
            }
        });
        return $task;
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
            $r = $ch->pop($timeout === \PHP_FLOAT_MAX ? -1 : \max(0.0001, $timeout));
            $done = true; // a later result is ignored
            return $r === false ? null : $r;
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
     * coroutine and every waiter (it included) waits on a private channel. When the last waiter
     * leaves by timeout the poller is cancelled.
     */
    private function waitStream(mixed $resource, string $mode, float $timeout): void
    {
        $this->wait(function () use ($resource, $mode, $timeout): ?array {
            $key = \get_resource_id($resource) . ':' . $mode;
            $ch = new Channel(1);
            $id = $this->nextWaiter++;
            if (!isset($this->streams[$key])) {
                $this->streams[$key] = ['cid' => 0, 'waiters' => []];
                $this->streams[$key]['cid'] = Coroutine::create(function () use ($resource, $mode, $key): void {
                    $r = System::waitEvent($resource, $mode === 'r' ? \SWOOLE_EVENT_READ : \SWOOLE_EVENT_WRITE, -1);
                    if (($this->streams[$key]['cid'] ?? null) !== Coroutine::getCid()) {
                        return; // cancelled: the last waiter timed out
                    }
                    $waiters = $this->streams[$key]['waiters'];
                    unset($this->streams[$key]);
                    foreach ($waiters as $w) {
                        $w->push($r === false ? [false, new \RuntimeException('waitEvent failed: ' . \swoole_strerror(\swoole_last_error()))] : [true, null]);
                    }
                });
            }
            $this->streams[$key]['waiters'][$id] = $ch;
            $r = $ch->pop($timeout === \PHP_FLOAT_MAX ? -1 : \max(0.0001, $timeout));
            if ($r === false && isset($this->streams[$key])) {
                unset($this->streams[$key]['waiters'][$id]);
                if (!$this->streams[$key]['waiters']) {
                    $cid = $this->streams[$key]['cid'];
                    unset($this->streams[$key]);
                    Coroutine::cancel($cid);
                }
            }
            return $r === false ? null : $r;
        }, $timeout);
    }

    /**
     * Run $body (blocks on a channel; returns [ok, value], or null on timeout) inside a coroutine.
     * From {main} or a Fiber outside any coroutine a Co\run() scheduler is started for it and the
     * outcome is carried out as a value, so a Timeout does not escape the scheduler.
     * Co\run() returns only when every coroutine has ended.
     */
    private function wait(\Closure $body, float $timeout, ?float $sleep = null): mixed
    {
        $run = function () use ($body, $timeout, $sleep): array {
            try {
                if ($sleep !== null) {
                    Coroutine::sleep(\max(0.001, $sleep));
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
            \Swoole\Coroutine\run(static function () use ($run, &$out): void {
                $out = $run();
            });
            [$ok, $value] = $out;
        }
        if (!$ok) {
            throw $value;
        }
        return $value;
    }
}
