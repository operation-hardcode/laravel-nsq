<?php

declare(strict_types=1);

namespace OperationHardcode\LaravelNsq\Queue;

use Nsq\Config\ClientConfig;
use Nsq\Consumer;
use Nsq\Message;
use Psr\Log\LoggerInterface;
use function Amp\delay;
use function Amp\Promise\wait;

final class NsqConsumer
{
    private string $address;

    /**
     * @psalm-var array{channel?: string}
     */
    private array $config;
    private ClientConfig $clientConfig;
    private LoggerInterface $logger;

    /**
     * @var array<string, Message[]>
     */
    private array $messages = [];

    /**
     * @var array<string, \WeakReference<Consumer>>
     */
    private array $consumers = [];

    /**
     * @psalm-param array{channel?: string} $config
     */
    public function __construct(
        string $address,
        array $config,
        ClientConfig $clientConfig,
        LoggerInterface $logger
    ) {
        $this->address = $address;
        $this->config = $config;
        $this->clientConfig = $clientConfig;
        $this->logger = $logger;
    }

    /**
     * @param string $queue
     *
     * @throws \Throwable
     *
     * @return Message|null
     */
    public function consume(string $queue): ?Message
    {
        /** @var Message $message */
        if (null !== ($message = $this->popMessage($queue))) {
            return $message;
        }

        if (!isset($this->consumers[$queue])) {
            $this->consumers[$queue] = $this->createConsumer($queue);
        }

        wait(delay(500));

        return $this->popMessage($queue);
    }

    private function popMessage(string $queue): ?Message
    {
        $message = null;

        if (isset($this->messages[$queue])) {
            $message = array_pop($this->messages[$queue]);
        }

        /** @var Message|null */
        return $message;
    }

    /**
     * @throws \Throwable
     */
    private function createConsumer(string $queue): Consumer
    {
        $consumer = new Consumer(
            $this->address,
            $queue,
            $this->config['channel'] ?? 'default',
            function (Message $message) use ($queue): void {
                $this->messages[$queue][] = $message;
            },
            $this->clientConfig,
            $this->logger
        );

        wait($consumer->connect());

        return $consumer;
    }
}
