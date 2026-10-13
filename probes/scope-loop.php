<?php
// Runtime-specific: the same scenario with Swoole's own primitives (Coroutine::create + a native sleep), no async\
// API except run() to start the loop. Swoole resumes a coroutine itself after the sleep, where the adapter has no
// hook, so this is EXPECTED to fail: the resumed coroutine sees the scope that was active last.
// See README "Scope state". Prints what it observed and exits 1 on any mismatch.
require __DIR__ . '/_boot.php';

use Swoole\Coroutine;
use Swoole\Coroutine\Channel;

$bad = [];
$root = $_GET;
\async\run(function () use (&$bad) {
    $done = new Channel(2);
    foreach (['a', 'b'] as $name) {
        (new Scope())->run(function () use ($name, $done, &$bad) {
            Coroutine::create(function () use ($name, $done, &$bad) {
                $_GET = ['req' => $name];
                for ($i = 0; $i < 2; $i++) {
                    method_exists(Coroutine::class, 'usleep') ? Coroutine::usleep(10000) : Coroutine::sleep(0.01);
                    if (($_GET['req'] ?? null) !== $name) {
                        $bad[] = "$name after native sleep $i saw " . json_encode($_GET);
                    }
                }
                $done->push(1);
            });
        });
    }
    $done->pop();
    $done->pop();
});
$_GET === $root || $bad[] = 'after run() $_GET is ' . json_encode($_GET);
echo $bad ? implode("\n", $bad) . "\n" : "ok\n";
exit($bad ? 1 : 0);
