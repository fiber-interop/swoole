<?php

declare(strict_types=1);

namespace FiberInterop\Swoole;

use Swoole\Coroutine\Channel;

/**
 * Awaitable returned by go(): the result (or exception) of one coroutine.
 * Awaiters each get their own capacity-1 Channel; then() lets other loops await it.
 */
final class Task
{
    public bool $handled = false;
    private ?array $result = null;
    /** @var list<\Closure> */
    private array $listeners = [];

    public function onSettle(\Closure $cb): void
    {
        $this->result === null ? $this->listeners[] = $cb : $cb(...$this->result);
    }

    public function settle(bool $ok, mixed $value): void
    {
        $this->result = [$ok, $value];
        $listeners = $this->listeners;
        $this->listeners = [];
        foreach ($listeners as $cb) {
            $cb($ok, $value);
        }
    }

    /** Wait for the result from inside a coroutine; null on timeout. @return array{bool, mixed}|null */
    public function pop(float $timeout): ?array
    {
        if ($this->result !== null) {
            return $this->result;
        }
        $ch = new Channel(1);
        $this->onSettle(static fn(bool $ok, mixed $v) => $ch->push([$ok, $v]));
        $r = $ch->pop($timeout === \PHP_FLOAT_MAX ? -1 : \max(0.0001, $timeout));
        return $r === false ? null : $r;
    }

    public function then(?callable $onFulfilled = null, ?callable $onRejected = null): void
    {
        $this->handled = true;
        $this->onSettle(static function (bool $ok, mixed $value) use ($onFulfilled, $onRejected): void {
            $cb = $ok ? $onFulfilled : $onRejected;
            if ($cb !== null) {
                $cb($value);
            }
        });
    }
}
