<?php

declare(strict_types=1);

namespace OperationHardcode\LaravelNsq;

use Illuminate\Support\ServiceProvider;
use Illuminate\Queue\QueueManager;
use Illuminate\Queue\Connectors\ConnectorInterface;
use OperationHardcode\LaravelNsq\Queue\NsqQueueConnector;
use Psr\Log\LoggerInterface;

final class LaravelNsqServiceProvider extends ServiceProvider
{
    public function register(): void
    {
        $this->mergeConfigFrom(
            __DIR__.'/../config/nsq.php',
            'queue.connections.nsq'
        );
    }

    public function boot(): void
    {
        /** @var QueueManager $queue */
        $queue = $this->app['queue'];

        /** @var LoggerInterface $logger */
        $logger = $this->app->get(LoggerInterface::class);

        $queue->addConnector('nsq', fn (): ConnectorInterface => new NsqQueueConnector($logger));
    }
}
