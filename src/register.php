<?php

declare(strict_types=1);

if (\extension_loaded('swoole') || \extension_loaded('openswoole')) {
    \async\register(static fn() => new \FiberInterop\Swoole\Adapter(), 'fiber-interop/swoole');
}
