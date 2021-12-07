<?php

declare(strict_types=1);

namespace OperationHardcode\LaravelNsq\Queue;

use Illuminate\Container\Container;
use Illuminate\Queue\Jobs\Job;
use Illuminate\Contracts\Queue\Job as JobContract;
use Nsq\Message;
use function Amp\Promise\wait;

final class NsqJob extends Job implements JobContract
{
    private Message $message;

    /**
     * @var array{id?: string, attempts?: int}
     */
    private array $decoded;

    private NsqQueue $nsq;
    private string $queueName;

    public function __construct(Container $container, Message $message, NsqQueue $nsq, string $queueName)
    {
        $this->container = $container;
        $this->message = $message;
        $this->nsq = $nsq;
        $this->queueName = $queueName;

        /** @var array{id?: string, attempts?: int} $payload */
        $payload = $this->payload();
        $this->decoded = $payload;
    }

    /**
     * {@inheritdoc}
     */
    public function getJobId()
    {
        return $this->decoded['id'] ?? null;
    }

    /**
     * {@inheritdoc}
     */
    public function getRawBody(): string
    {
        return $this->message->body;
    }

    /**
     * {@inheritdoc}
     */
    public function attempts(): int
    {
        return $this->decoded['attempts'] ?? 1;
    }

    /**
     * {@inheritdoc}
     *
     * @throws \Throwable
     */
    public function markAsFailed(): void
    {
        parent::markAsFailed();

        if (!$this->message->isProcessed()) {
            wait($this->message->finish());
        }
    }

    /**
     * {@inheritdoc}
     *
     * @throws \Throwable
     */
    public function delete(): void
    {
        parent::delete();

        if (!$this->message->isProcessed()) {
            wait($this->message->finish());
        }
    }

    /**
     * @param int $delay
     *
     * @throws \Throwable
     */
    public function release($delay = 0): void
    {
        parent::release();

        $this->nsq->laterRaw($delay, $this->message->body, $this->queueName, $this->attempts());

        if (!$this->message->isProcessed()) {
            wait($this->message->finish());
        }
    }
}
