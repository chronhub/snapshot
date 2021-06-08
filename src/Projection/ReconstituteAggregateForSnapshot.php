<?php

declare(strict_types=1);

namespace Chronhub\Snapshot\Projection;

use Generator;
use Chronhub\Snapshot\Snapshot;
use Illuminate\Database\Query\Builder;
use Chronhub\Chronicler\Exception\StreamNotFound;
use Chronhub\Foundation\Aggregate\AggregateChanged;
use Chronhub\Chronicler\Support\Contracts\Query\QueryFilter;
use Chronhub\Foundation\Support\Contracts\Aggregate\AggregateId;
use Chronhub\Chronicler\Support\Contracts\Aggregate\AggregateRootWithSnapshotting;
use Chronhub\Chronicler\Support\Contracts\Aggregate\AggregateRepositoryWithSnapshotting;

final class ReconstituteAggregateForSnapshot
{
    public function __construct(private AggregateRepositoryWithSnapshotting $aggregateRepository)
    {
    }

    public function reconstituteFromSnapshot(Snapshot $snapshot,
                                             AggregateId $aggregateId,
                                             int $toVersion): ?AggregateRootWithSnapshotting
    {
        try {
            $aggregateRoot = $snapshot->aggregateRoot();

            return $aggregateRoot->reconstituteFromSnapshotEvents(
                $this->fromHistory($aggregateId, $snapshot->lastVersion() + 1, $toVersion + 1)
            );
        } catch (StreamNotFound) {
            return null;
        }
    }

    public function reconstituteFromFirstVersion(AggregateId $aggregateId,
                                                 string $aggregateType,
                                                 AggregateChanged $event): ?AggregateRootWithSnapshotting
    {
        try {
            $events = function () use ($event): Generator {
                yield $event;

                return 1;
            };

            /* @var AggregateRootWithSnapshotting $aggregateType */
            return $aggregateType::reconstituteFromEvents($aggregateId, $events());
        } catch (StreamNotFound) {
            return null;
        }
    }

    private function fromHistory(AggregateId $aggregateId, int $fromVersion, int $toVersion): Generator
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
                        ->orderByRaw('CAST(headers->>\'__aggregate_version\' AS INT) ASC');
                };
            }
        };

        $streamName = $this->aggregateRepository->streamProducer()->determineStreamName($aggregateId->toString());

        return $this->aggregateRepository->chronicler()->retrieveFiltered($streamName, $filter);
    }
}
