<?php
// Portable: two scopes, interleaving tasks through fiber\sleep(); each must keep seeing its own $_GET.
require __DIR__ . '/_boot.php';

use function fiber\{go, await_all, run, sleep};

$bad = [];
$root = $_GET;
run(function () use (&$bad) {
    $task = function (string $name) use (&$bad) {
        return (new Scope())->run(fn() => go(function () use ($name, &$bad) {
            $_GET = ['req' => $name];
            for ($i = 0; $i < 2; $i++) {
                sleep(0.01);
                if (($_GET['req'] ?? null) !== $name) {
                    $bad[] = "$name after sleep $i saw " . json_encode($_GET);
                }
            }
        }));
    };
    await_all([$task('a'), $task('b')]);
});
$_GET === $root || $bad[] = 'after run() $_GET is ' . json_encode($_GET);
echo $bad ? implode("\n", $bad) . "\n" : "ok\n";
exit($bad ? 1 : 0);
