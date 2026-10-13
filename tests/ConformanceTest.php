<?php

declare(strict_types=1);

namespace FiberInterop\Swoole\Tests;

use PHPUnit\Framework\TestCase;

final class ConformanceTest extends TestCase
{
    public function testAllExamplesPass(): void
    {
        $this->assertTrue(extension_loaded('swoole') || extension_loaded('openswoole'), 'run with the extension loaded');
        $cmd = [PHP_BINARY, __DIR__ . '/../vendor/fiber-interop/api/bin/fiber-interop-conformance', __DIR__ . '/bootstrap.php'];
        $p = proc_open($cmd, [1 => ['pipe', 'w'], 2 => ['pipe', 'w']], $pipes);
        $out = stream_get_contents($pipes[1]);
        $code = proc_close($p);
        preg_match_all('/^FAIL (\S+)/m', $out, $m);
        $this->assertSame([], $m[1], $out);
        if (!$m[1]) {
            $this->assertSame(0, $code, $out);
        }
    }
}
