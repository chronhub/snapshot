<?php

declare(strict_types=1);

namespace Chronhub\Snapshot;

use DateTimeImmutable;
use Chronhub\Snapshot\Exception\InvalidArgumentException;
use Chronhub\Chronicler\Support\Contracts\Aggregate\AggregateRootWithSnapshotting;

class Snapshot
{
    public function __construct(private string $aggregateType,
                                private string $aggregateId,
                                private AggregateRootWithSnapshotting $aggregateRoot,
                                private int $lastVersion,
                                private DateTimeImmutable $createdAt)
    {
        if ($lastVersion < 1) {
            throw new InvalidArgumentException("Aggregate version must be greater or equal than 1, current is $lastVersion");
        }
    }

    public function aggregateType(): string
    {
        return $this->aggregateType;
    }

    public function aggregateId(): string
    {
        return $this->aggregateId;
    }

    public function aggregateRoot(): AggregateRootWithSnapshotting
    {
        return $this->aggregateRoot;
    }

    public function lastVersion(): int
    {
        return $this->lastVersion;
    }

    public function createdAt(): DateTimeImmutable
    {
        return $this->createdAt;
    }
}
