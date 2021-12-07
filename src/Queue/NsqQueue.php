<?php

declare(strict_types=1);

namespace OperationHardcode\LaravelNsq\Queue;

use Illuminate\Queue\Queue;
use Illuminate\Contracts\Queue\Queue as QueueContract;
use Illuminate\Support\Str;

final class NsqQueue extends Queue implements QueueContract
{
    public function __construct(
        private NsqConsumer $consumer,
        private NsqProducer $producer,
        private string $topic = 'default'
    ) {
    }

    public function size($queue = null): int
    {
        return 0;
    }

    public function push($job, $data = '', $queue = null)
    {
        return $this->enqueueUsing(
            $job,
            $this->createPayload($job, $this->getQueue($queue), $data),
            $queue,
            null,
            function ($payload, $queue) {
                return $this->pushRaw($payload, $queue);
            }
        );
    }

    /**
     * {@inheritdoc}
     */
    public function pushRaw($payload, $queue = null, array $options = [])
    {
        $this->producer->publish($this->getQueue($queue), $payload);

        return json_decode($payload, true)['id'] ?? null;
    }

    public function later($delay, $job, $data = '', $queue = null)
    {
        return $this->enqueueUsing(
            $job,
            $this->createPayload($job, $this->getQueue($queue), $data),
            $queue,
            $delay,
            function ($payload, $queue, $delay) {
                return $this->laterRaw($delay, $payload, $queue);
            }
        );
    }

    public function laterRaw($delay, $payload, $queue = null, $attempts = 0)
    {
        $delay = $this->secondsUntil($delay) * 1000;

        if ($delay <= 0) {
            return $this->pushRaw($payload, $queue, ['delay' => $delay, 'attempts' => $attempts, 'id' => $this->id()]);
        }

        $this->producer->publish($this->getQueue($queue), $payload, $delay);

        return json_decode($payload, true)['id'] ?? null;
    }

    /**
     * {@inheritdoc}
     */
    public function pop($queue = null)
    {
        $queue ??= $this->topic;

        if (null !== ($message = $this->consumer->consume($queue))) {
            return new NsqJob($this->container, $message, $this, $queue);
        }

        return null;
    }

    private function getQueue(?string $queue = null): string
    {
        return $queue ?? $this->topic;
    }

    /**
     * {@inheritdoc}
     */
    protected function createPayloadArray($job, $queue, $data = ''): array
    {
        return array_merge(parent::createPayloadArray($job, $queue, $data), [
            'id' => $this->id(),
            'attempts' => 0,
        ]);
    }

    /**
     * @return string
     */
    private function id(): string
    {
        return Str::uuid()->toString();
    }
}
