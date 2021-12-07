<?php

declare(strict_types=1);

namespace OperationHardcode\LaravelNsq\Queue;

use Amp\Success;
use Nsq\Config\ClientConfig;
use Nsq\Producer;
use Psr\Log\LoggerInterface;
use function Amp\Promise\wait;

final class NsqProducer
{
    /**
     * @var array<string, \WeakReference<Producer>>
     */
    private array $producers = [];

    public function __construct(
        private string $address,
        private ClientConfig $clientConfig,
        private LoggerInterface $logger
    ) {
    }

    public function publish(string $queue, string $body, ?int $delay = null): void
    {
        wait($this->producer($queue)?->publish($queue, $body, $delay) ?: new Success());
    }

    private function producer(string $queue): ?Producer
    {
        if (!isset($this->producers[$queue]) || $this->producers[$queue]->get() === null) {
            $producer = new Producer($this->address, $this->clientConfig, $this->logger);

            wait($producer->connect());

            $this->producers[$queue] = \WeakReference::create($producer);
        }

        return $this->producers[$queue]->get();
    }
}
