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
                $this->fromHistory($aggregateId, $snapshot->lastVersion() + 1, $toVersion)
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

    private function fromHistory(AggregateId $aggregateId, int $from, int $to): Generator
    {
        $filter = new class($aggregateId, $from, $to) implements QueryFilter {
            public function __construct(private AggregateId $aggregateId, private int $from, private int $to)
            {
            }

            public function filterQuery(): callable
            {
                return function (Builder $query): void {
                    $query
                        ->whereJsonContains('headers->__aggregate_id', $this->aggregateId->toString())
                        ->whereRaw('CAST(headers->>\'__aggregate_version\' AS INT) >= ' . $this->from)
                        ->whereRaw('CAST(headers->>\'__aggregate_version\' AS INT) <= ' . $this->to)
                        ->orderByRaw('CAST(headers->>\'__aggregate_version\' AS INT) ASC');
                };
            }
        };

        $streamName = $this->aggregateRepository->streamProducer()->determineStreamName($aggregateId->toString());

        return $this->aggregateRepository->chronicler()->retrieveFiltered($streamName, $filter);
    }
}
