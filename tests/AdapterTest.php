<?php

declare(strict_types=1);

namespace FiberInterop\Swoole\Tests;

use PHPUnit\Framework\TestCase;

use function async\{go, await, await_all, run, sleep, readable};

final class AdapterTest extends TestCase
{
    public function testAwaitTwiceSequentiallyReturnsSameValue(): void
    {
        $this->assertSame([7, 7], run(function () {
            $t = go(fn() => 7);
            return [await($t), await($t)];
        }));
    }

    public function testAwaitTwiceRethrowsSameException(): void
    {
        $caught = run(function () {
            $t = go(function () { throw new \RuntimeException('boom'); });
            $c = [];
            foreach ([1, 2] as $_) {
                try { await($t); } catch (\RuntimeException $e) { $c[] = $e; }
            }
            return $c;
        });
        $this->assertCount(2, $caught);
        $this->assertSame($caught[0], $caught[1]);
    }

    public function testTwoCoroutinesAwaitOneTask(): void
    {
        $this->assertSame(['a', 'a'], run(function () {
            $t = go(function () { sleep(0.02); return 'a'; });
            return await_all([go(fn() => await($t)), go(fn() => await($t))]);
        }));
    }

    public function testTimeoutLeavesTaskRunning(): void
    {
        $this->assertSame('late', run(function () {
            $t = go(function () { sleep(0.05); return 'late'; });
            try { await($t, 0.01); $this->fail('no timeout'); } catch (\async\Timeout) {}
            return await($t);
        }));
    }

    public function testTimeoutFromMain(): void
    {
        $this->expectException(\async\Timeout::class);
        await(go(fn() => sleep(0.05)), 0.01);
    }

    public function testConcurrentStreamWaitersAndTimeout(): void
    {
        [$r, $w] = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, STREAM_IPPROTO_IP);
        stream_set_blocking($r, false);
        $n = run(function () use ($r, $w) {
            $ts = [go(fn() => readable($r) ? 1 : 1), go(fn() => readable($r) ? 1 : 1)];
            sleep(0.01);
            fwrite($w, 'x');
            return array_sum(await_all($ts));
        });
        $this->assertSame(2, $n);
        fread($r, 10);
        $this->expectException(\async\Timeout::class);
        readable($r, 0.01);
    }

    public function testScopeStackIsPerCoroutine(): void
    {
        $a = new \Scope();
        $b = new \Scope();
        $res = run(function () use ($a, $b) {
            $ta = $a->run(fn() => go(function () use ($a) { sleep(0.02); return \Scope::getCurrent() === $a; }));
            $tb = $b->run(fn() => go(function () use ($b) { sleep(0.01); return \Scope::getCurrent() === $b; }));
            return [await($ta), await($tb)];
        });
        $this->assertSame([true, true], $res);
    }
}
