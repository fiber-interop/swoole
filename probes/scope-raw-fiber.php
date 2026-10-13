<?php
// Portable: a plain Fiber inside a scope, suspended with bare Fiber::suspend(), sees the creator's $_GET.
require __DIR__ . '/_boot.php';

use function async\run;

$bad = [];
run(function () use (&$bad) {
    (new Scope())->run(function () use (&$bad) {
        $_GET = ['req' => 'creator'];
        $reads = [];
        $f = new Fiber(function () use (&$reads) {
            $reads[] = $_GET['req'] ?? null;
            Fiber::suspend();
            $reads[] = $_GET['req'] ?? null;
        });
        $f->start();
        $f->resume();
        foreach ($reads as $i => $r) {
            $r === 'creator' || $bad[] = "read $i saw " . json_encode($r);
        }
        count($reads) === 2 || $bad[] = 'fiber did not finish';
    });
});
echo $bad ? implode("\n", $bad) . "\n" : "ok\n";
exit($bad ? 1 : 0);
