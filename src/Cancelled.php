<?php

declare(strict_types=1);

namespace FiberInterop\Swoole;

/** A wait ended because its coroutine was cancelled (a service stopped at the end of the outermost run()). */
final class Cancelled extends \RuntimeException implements \fiber\CancelledException
{
}
