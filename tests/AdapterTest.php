<?php

declare(strict_types=1);

namespace FiberInterop\Swoole\Tests;

use PHPUnit\Framework\TestCase;

use function fiber\{go, await, await_all, run, sleep, readable};

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
            try { await($t, 0.01); $this->fail('no timeout'); } catch (\fiber\Timeout) {}
            return await($t);
        }));
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
        $this->expectException(\fiber\Timeout::class);
        run(fn() => readable($r, 0.01));
    }

    public function testServicesAreCancelledWhenOutermostRunReturns(): void
    {
        $log = [];
        $t = microtime(true);
        run(function () use (&$log) {
            \fiber\service(function () use (&$log) {
                try {
                    sleep(5);
                } catch (\fiber\CancelledException) {
                    $log[] = 'cancelled';
                }
            });
            sleep(0.01);
            $log[] = 'f done';
        });
        $this->assertSame(['f done', 'cancelled'], $log);
        $this->assertLessThan(1.0, microtime(true) - $t);
    }

    public function testTwoCoroutinesEachDoTheirOwnNestedRun(): void
    {
        $r = run(function () {
            $mk = fn(string $n, float $d) => go(fn() => run(function () use ($n, $d) {
                $inner = go(function () use ($n, $d) { sleep($d); return $n; });
                return await($inner) . '!';
            }));
            return await_all([$mk('a', 0.03), $mk('b', 0.01)]);
        });
        $this->assertSame(['a!', 'b!'], $r);
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

    public function testCoroutineContextIsStableAndReleasedWithTheCoroutine(): void
    {
        $ref = null;
        run(function () use (&$ref) {
            $t = go(function () use (&$ref) {
                $c = \Swoole\Coroutine::getContext();
                sleep(0.005);
                $same = $c === \Swoole\Coroutine::getContext();
                $ref = \WeakReference::create($c);
                return $same;
            });
            $this->assertTrue(await($t));
        });
        $this->assertNotNull($ref);
        $this->assertNull($ref->get(), 'context is released when the coroutine ends');
    }

    public function testScopeStackIsDroppedWhenCoroutineEnds(): void
    {
        $a = new \Scope();
        $inside = run(function () use ($a) {
            return await($a->run(fn() => go(fn() => \Scope::getCurrent() === $a)));
        });
        $this->assertTrue($inside);
        $this->assertSame(\Scope::getRootScope(), \Scope::getCurrent());
    }

    public function testScopeActivatedAtTaskStart(): void
    {
        $this->assertSame('x', run(function () {
            $_GET = ['v' => 'root'];
            $scope = new \Scope();
            $scope->run(function () { $_GET = ['v' => 'x']; });
            // A task started in $scope must see $scope's $_GET from its first statement, though root is active.
            return await($scope->run(fn() => go(fn() => $_GET['v'])));
        }));
    }

    public function testScopeActivatedAfterPop(): void
    {
        $seen = run(function () {
            $out = [];
            $tasks = [];
            foreach (['a', 'b'] as $n) {
                $tasks[] = (new \Scope())->run(function () use ($n, &$out) { return go(function () use ($n, &$out) {
                    $_GET = ['v' => $n];
                    sleep($n === 'a' ? 0.01 : 0.015);
                    $out[$n] = $_GET['v'];
                    await(go(fn() => sleep(0.005)));
                    $out[$n . '2'] = $_GET['v'];
                }); });
            }
            await_all($tasks);
            return $out;
        });
        $this->assertSame(['a' => 'a', 'b' => 'b', 'a2' => 'a', 'b2' => 'b'], $seen + []);
    }

    public function testScopeActivatedAfterReturnToMain(): void
    {
        $root = $_GET;
        run(function () {
            (new \Scope())->run(fn() => await(go(function () { $_GET = ['v' => 'inner']; sleep(0.005); })));
        });
        $this->assertSame($root, $_GET);
    }

    public function testNoSwapWhenScopeUnchanged(): void
    {
        $same = run(function () {
            $_GET = ['v' => 1];
            $active = fn() => (new \ReflectionProperty(\Scope::class, 'active'))->getValue();
            $before = $active();
            sleep(0.005);
            await(go(fn() => 1));
            $_GET['w'] = 2; // in-place edit survives only if no swap reloaded a saved copy
            sleep(0.005);
            return $active() === $before && $_GET === ['v' => 1, 'w' => 2];
        });
        $this->assertTrue($same);
    }

    public function testLocalCloseUnblocksWaitersNormally(): void
    {
        $woke = run(function () {
            [$a, $b] = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, STREAM_IPPROTO_IP);
            $start = microtime(true);
            $wait = function () use ($a, $start) {
                readable($a, 5.0);
                return microtime(true) - $start;
            };
            $tasks = [go($wait), go($wait)];
            sleep(0.02);
            fclose($a);
            $woke = await_all($tasks);
            fclose($b);
            return $woke;
        });
        $this->assertCount(2, $woke);
        $this->assertLessThan(0.1, max($woke));
    }

    /** Services are driven while a run() is active: awaiting what only a service resolves must not end the loop. */
    public function testAwaitResolvedOnlyByService(): void
    {
        $t = \microtime(true);
        $result = \fiber\run(function () {
            $timer = new \fiber\Deferred();
            \fiber\service(function () use ($timer) {
                \fiber\sleep(0.02);
                $timer->settle(true, 'slept');
                \fiber\sleep(30); // a sleeping service must not keep run() alive
            });
            [$a, $b] = \stream_socket_pair(\STREAM_PF_UNIX, \STREAM_SOCK_STREAM, \STREAM_IPPROTO_IP);
            $io = new \fiber\Deferred();
            \fiber\service(function () use ($a, $io) {
                \fiber\readable($a, 5.0);
                $io->settle(true, \fread($a, 10));
            });
            \fiber\service(function () use ($b) {
                \fiber\sleep(0.02);
                \fwrite($b, 'ping');
            });
            return [\fiber\await($timer), \fiber\await($io)];
        });
        $this->assertSame(['slept', 'ping'], $result);
        $this->assertLessThan(5.0, \microtime(true) - $t);
    }

    /** Swoole isolates output buffers per coroutine; task output must still reach the caller's ob_start(). */
    public function testTaskOutputReachesCallersOutputBuffer(): void
    {
        \ob_start();
        run(function () {
            echo 'A';
            await(go(function () { sleep(0.01); echo 'B'; }));
            echo 'C';
        });
        $this->assertSame('ABC', \ob_get_clean());
    }
}
