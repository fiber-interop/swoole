<?php
/**
 * Swoole 6.3.0RC1: Coroutine::cancel() of a coroutine blocked in Coroutine\System::waitEvent() races with
 * the event becoming ready (the fd turns readable in the same loop tick) and segfaults (exit 11).
 *
 * Found with: PHP 8.4, swoole 6.3.0RC1 (built from source). OpenSwoole 26.2.0 does not crash.
 * gdb backtrace (seen through fiber-interop/swoole's stream waiters, ReadinessTest::testReadersContendingOnOneStream):
 *   SIGSEGV swoole::PHPCoroutine::fiber_context_switch_try_notify(from=main_context, to=0x8000000000000000)
 *   <- on_resume <- Coroutine::resume <- CallbackManager::execute <- Reactor::execute_end_callbacks
 * A coroutine is resumed from a deferred (end-of-tick) callback with a garbage context.
 *
 * Shape: a poller coroutine waits on a socket with waitEvent(); readers wait on channels with short timeouts;
 * the last reader to time out cancels the poller while a writer makes the socket readable at about the same time.
 *
 * Usage: php -d extension=swoole.so swoole-cancel-waitevent-segfault.php [seconds=30]
 * Exit: 0 = survived the whole time, 139/11 = crashed (reproduced).
 */
use Swoole\Coroutine;
use Swoole\Coroutine\Channel;
use Swoole\Coroutine\System;

$deadline = \microtime(true) + (float) ($argv[1] ?? 30);
$round = 0;
while (\microtime(true) < $deadline) {
    Coroutine\run(static function () use (&$round): void {
        [$a, $b] = \stream_socket_pair(\STREAM_PF_UNIX, \STREAM_SOCK_STREAM, \STREAM_IPPROTO_IP);
        $waiters = [];
        $poller = Coroutine::create(static function () use ($a, &$waiters): void {
            $r = System::waitEvent($a, \SWOOLE_EVENT_READ, -1);
            if ($r === false) {
                return; // cancelled
            }
            foreach ($waiters as $w) {
                $w->push(true);
            }
        });
        $n = \random_int(1, 8);
        for ($i = 0; $i < $n; $i++) {
            $waiters[$i] = $ch = new Channel(1);
            $timeout = \random_int(1, 5) / 1000;
            Coroutine::create(static function () use ($ch, $timeout, $i, &$waiters, $poller): void {
                $ch->pop($timeout);
                unset($waiters[$i]);
                if (!$waiters) {
                    Coroutine::cancel($poller); // last waiter out stops the poller
                }
            });
        }
        // make the socket readable at about the time the readers time out
        Coroutine::create(static function () use ($b): void {
            (new Channel(1))->pop(\random_int(500, 5500) / 1000000);
            @\fwrite($b, 'x');
        });
    });
    $round++;
}
echo "survived {$round} rounds\n";
