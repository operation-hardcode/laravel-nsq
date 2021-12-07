<?php

declare(strict_types=1);

namespace OperationHardcode\LaravelNsq\Queue;

use Illuminate\Queue\Connectors\ConnectorInterface;
use Illuminate\Contracts\Queue\Queue;
use Nsq\Config\ClientConfig;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

final class NsqQueueConnector implements ConnectorInterface
{
    private LoggerInterface $logger;

    public function __construct(?LoggerInterface $logger = null)
    {
        $this->logger = $logger ?: new NullLogger();
    }

    /**
     * {@inheritdoc}
     */
    public function connect(array $config): Queue
    {
        /** @var array{options: array{host?: string, port?: int, authSecret?: string|null}, queue: array{topic?: string, channel?: string}} $nsqConfig */
        $nsqConfig = $config;
        $nsqConfig['options']['rdyCount'] = 1;

        $address = sprintf('tcp://%s:%s', $nsqConfig['options']['host'] ?? '127.0.0.1', $nsqConfig['options']['port'] ?? 4150);

        $clientConfig = ClientConfig::fromArray($nsqConfig['options']);

        return new NsqQueue(
            new NsqConsumer($address, $nsqConfig['queue'], $clientConfig, $this->logger),
            new NsqProducer($address, $clientConfig, $this->logger),
            $config['topic'] ?? 'default',
        );
    }
}
