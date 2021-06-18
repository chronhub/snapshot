<?php

declare(strict_types=1);

namespace Chronhub\Snapshot\Projection;

use Generator;
use Chronhub\Snapshot\Snapshot;
use Illuminate\Database\Query\Builder;
use Chronhub\Snapshot\Store\SnapshotStore;
use Chronhub\Foundation\Message\DomainEvent;
use Chronhub\Chronicler\Exception\StreamNotFound;
use Chronhub\Snapshot\Exception\RuntimeException;
use Chronhub\Foundation\Support\Contracts\Clock\Clock;
use Chronhub\Foundation\Support\Contracts\Message\Header;
use Chronhub\Chronicler\Support\Contracts\Query\QueryFilter;
use Chronhub\Foundation\Support\Contracts\Aggregate\AggregateId;
use Chronhub\Chronicler\Support\Contracts\Aggregate\AggregateRootWithSnapshotting;
use Chronhub\Chronicler\Support\Contracts\Aggregate\AggregateRepositoryWithSnapshotting;

class SnapshotRepository
{
    private array $map = [];

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

        $aggregateRoot = $this->reconstituteAggregate($aggregateId, $aggregateType, $version);

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

        $this->map = [];
    }

    /**
     * Cover scenarios.
     *
     *  1) starting snapshot projection along others projections
     *     snapshot (at least) first version and increment to persist x events
     *
     *  2) stopping snapshot projection
     *     restart again and it will increment normally
     *
     *  3) deleting/resetting snapshots projections
     *     it will start the first version of snapshot at the last event version
     *
     *  for 1) and 3)
     *     it will adjust snapshot version when persist x events match the current event version
     *     e.g  with a snapshot every 1000 events,
     *          starting a 3/4016, will take a snapshot at 1000/5000
     *          and it will increment normally
     *
     *  it also cover scenarios when snapshots projections
     *  and/or snapshots streams positions does/not exists
     */
    protected function reconstituteAggregate(AggregateId $aggregateId,
                                             string $aggregateType,
                                             int $version): ?AggregateRootWithSnapshotting
    {
        if (1 === $version) {
            return $this->reconstituteFromFirstVersion($aggregateId, $aggregateType);
        }

        if (isset($this->map[$aggregateId->toString()]) && $version < $this->map[$aggregateId->toString()]) {
            return null;
        }

        if (0 === ($version % $this->persisEveryEvents)) {
            return $this->reconstituteFromSnapshot($aggregateId, $aggregateType, $version);
        }

        return null;
    }

    protected function reconstituteFromFirstVersion(AggregateId $aggregateId, string $aggregateType): ?AggregateRootWithSnapshotting
    {
        // if snapshots streams positions have been deleted
        // it will replace current snapshot

        try {
            $events = $this->fromHistory($aggregateId, 1, PHP_INT_MAX);

            /* @var AggregateRootWithSnapshotting $aggregateType */
            $aggregateRoot = $aggregateType::reconstituteFromEvents($aggregateId, $events);

            $this->map[$aggregateId->toString()] = $aggregateRoot->version();

            return $aggregateRoot;
        } catch (StreamNotFound) {
            throw new RuntimeException('Unable to take first snapshot for aggregate id ' . $aggregateId);
        }
    }

    protected function reconstituteFromSnapshot($aggregateId, $aggregateType, int $toVersion): ?AggregateRootWithSnapshotting
    {
        $lastSnapshot = $this->snapshotStore->get($aggregateType, $aggregateId->toString());

        if (null === $lastSnapshot) {
            // happens if snapshots have been deleted, but snapshot stream positions still exists
            // but it will only be hit when the current event version will match the persist x events
            return $this->reconstituteFromFirstVersion($aggregateId, $aggregateType);
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
                        ->where('aggregate_id', $this->aggregateId->toString())
                        ->whereBetween('aggregate_version', [$this->fromVersion, $this->toVersion])
                        ->orderBy('aggregate_version');
                };
            }
        };

        $streamName = $this->aggregateRepository->streamProducer()->determineStreamName($aggregateId->toString());

        return $this->aggregateRepository->chronicler()->retrieveFiltered($streamName, $filter);
    }
}
