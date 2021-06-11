<?php

declare(strict_types=1);

namespace Chronhub\Snapshot\Projection;

use Generator;
use Chronhub\Snapshot\Snapshot;
use Illuminate\Database\Query\Builder;
use Chronhub\Snapshot\Store\SnapshotStore;
use Chronhub\Foundation\Message\DomainEvent;
use Chronhub\Chronicler\Exception\StreamNotFound;
use Chronhub\Foundation\Support\Contracts\Clock\Clock;
use Chronhub\Foundation\Support\Contracts\Message\Header;
use Chronhub\Chronicler\Support\Contracts\Query\QueryFilter;
use Chronhub\Foundation\Support\Contracts\Aggregate\AggregateId;
use Chronhub\Chronicler\Support\Contracts\Aggregate\AggregateRootWithSnapshotting;
use Chronhub\Chronicler\Support\Contracts\Aggregate\AggregateRepositoryWithSnapshotting;

class SnapshotRepository
{
    public function __construct(private SnapshotStore $snapshotStore,
                                private AggregateRepositoryWithSnapshotting $aggregateRepository,
                                private Clock $clock,
                                private int $persisEveryEvents = 1000)
    {
    }

    public function store(DomainEvent $event): bool
    {
        $version = (int) $event->header(Header::AGGREGATE_VERSION);
        $aggregateType = $event->header(Header::AGGREGATE_TYPE);
        $aggregateId = determineAggregateId($event);

        $aggregateRoot = null;

        if (1 === $version) {
            $aggregateRoot = $this->reconstituteFirstVersion($aggregateId, $aggregateType);
        } elseif (0 === $version % $this->persisEveryEvents) {
            $aggregateRoot = $this->reconstituteFromSnapshot($aggregateId, $aggregateType, $version);
        }

        if ($aggregateRoot instanceof AggregateRootWithSnapshotting) {
            $snapshot = new Snapshot(
                $aggregateType,
                $aggregateId->toString(),
                $aggregateRoot,
                $aggregateRoot->version(),
                $this->clock->fromNow()->dateTime(),
            );

            $this->snapshotStore->save($snapshot);

            return true;
        }

        return false;
    }

    public function deleteAll(string $aggregateType): void
    {
        $this->snapshotStore->deleteAll($aggregateType);
    }

    protected function reconstituteFromSnapshot($aggregateId, $aggregateType, int $toVersion): ?AggregateRootWithSnapshotting
    {
        $lastSnapshot = $this->snapshotStore->get($aggregateType, $aggregateId->toString());

        if (null === $lastSnapshot) {
            return null;
        }

        try {
            $aggregateRoot = $lastSnapshot->aggregateRoot();

            $events = $this->fromHistory(
                $aggregateRoot->aggregateId(),
                $lastSnapshot->lastVersion() + 1,
                $toVersion + 1
            );

            return $aggregateRoot->reconstituteFromSnapshotEvents($events);
        } catch (StreamNotFound) {
            return null;
        }
    }

    protected function reconstituteFirstVersion(AggregateId $aggregateId, string $aggregateType): ?AggregateRootWithSnapshotting
    {
        try {
            $events = $this->fromHistory($aggregateId, 1, $this->persisEveryEvents);

            /* @var AggregateRootWithSnapshotting $aggregateType */
            return $aggregateType::reconstituteFromEvents($aggregateId, $events);
        } catch (StreamNotFound) {
            return null;
        }
    }

    protected function fromHistory(AggregateId $aggregateId, int $fromVersion, int $toVersion): Generator
    {
        $filter = new class($aggregateId, $fromVersion, $toVersion) implements QueryFilter {
            public function __construct(private AggregateId $aggregateId,
                                        private int $fromVersion,
                                        private int $toVersion)
            {
            }

            public function filterQuery(): callable
            {
                return function (Builder $query): void {
                    $query
                        ->whereJsonContains('headers->__aggregate_id', $this->aggregateId->toString())
                        ->whereRaw('CAST(headers->>\'__aggregate_version\' AS INT) >= ' . $this->fromVersion)
                        ->whereRaw('CAST(headers->>\'__aggregate_version\' AS INT) <= ' . $this->toVersion)
                        ->orderBy('no');
                };
            }
        };

        $streamName = $this->aggregateRepository->streamProducer()->determineStreamName($aggregateId->toString());

        return $this->aggregateRepository->chronicler()->retrieveFiltered($streamName, $filter);
    }
}
