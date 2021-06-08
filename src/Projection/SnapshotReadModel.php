<?php

declare(strict_types=1);

namespace Chronhub\Snapshot\Projection;

use Chronhub\Snapshot\Snapshot;
use Illuminate\Support\Collection;
use Illuminate\Database\Query\Builder;
use Chronhub\Snapshot\Store\SnapshotStore;
use Chronhub\Foundation\Aggregate\AggregateChanged;
use Chronhub\Foundation\Support\Contracts\Clock\Clock;
use Chronhub\Foundation\Support\Contracts\Message\Header;
use Chronhub\Projector\Support\Contracts\Support\ReadModel;
use Chronhub\Chronicler\Support\Contracts\Query\QueryFilter;
use Chronhub\Foundation\Support\Contracts\Aggregate\AggregateId;
use Chronhub\Foundation\Support\Contracts\Aggregate\AggregateRoot;
use Chronhub\Chronicler\Support\Contracts\Aggregate\AggregateRootWithSnapshotting;
use Chronhub\Chronicler\Support\Contracts\Aggregate\AggregateRepositoryWithSnapshotting;

final class SnapshotReadModel implements ReadModel
{
    private Collection $aggregateCache;

    public function __construct(private AggregateRepositoryWithSnapshotting $aggregateRepository,
                                private SnapshotStore $snapshotStore,
                                private Clock $clock,
                                private array $aggregateTypes,
                                private int $persisEveryEvents = 1000)
    {
        $this->aggregateCache = new Collection();
    }

    /**
     * @param AggregateChanged[] $events
     */
    public function stack(string $operation, ...$events): void
    {
        $event = $events[0];

        $this->aggregateCache->put($event->aggregateId(), $event);
    }

    public function persist(): void
    {
        $this->aggregateCache->each(function (AggregateChanged $event): void {
            $version = (int) $event->header(Header::AGGREGATE_VERSION);

            if (1 === $version || 0 === $version % $this->persisEveryEvents) {
                $aggregateId = $this->determineAggregateId($event);

                $from = $version === $this->persisEveryEvents ? 1 : $version - $this->persisEveryEvents;

                /** @var AggregateRootWithSnapshotting $aggregateRoot */
                $aggregateRoot = $this->retrieveAggregateFromVersion($aggregateId, $from, $version);

                if ($aggregateRoot instanceof AggregateRootWithSnapshotting) {
                    $snapshot = new Snapshot(
                        $event->header(Header::AGGREGATE_TYPE),
                        $aggregateId->toString(),
                        $aggregateRoot,
                        $aggregateRoot->version(),
                        $this->clock->fromNow()->dateTime(),
                    );

                    $this->snapshotStore->save($snapshot);
                }

                $this->aggregateRepository->flushCache();
            }
        });

        $this->aggregateCache = new Collection();
    }

    public function reset(): void
    {
        foreach ($this->aggregateTypes as $aggregateType) {
            $this->snapshotStore->deleteAll($aggregateType);
        }
    }

    public function down(): void
    {
        foreach ($this->aggregateTypes as $aggregateType) {
            $this->snapshotStore->deleteAll($aggregateType);
        }
    }

    public function isInitialized(): bool
    {
        return true;
    }

    public function initialize(): void
    {
    }

    private function determineAggregateId(AggregateChanged $event): AggregateId
    {
        $aggregateId = $event->header(Header::AGGREGATE_ID);

        if ($aggregateId instanceof AggregateId) {
            return $aggregateId;
        }

        $aggregateIdType = $event->header(Header::AGGREGATE_ID_TYPE);

        /* @var AggregateId $aggregateIdType */
        return $aggregateIdType::fromString($aggregateId);
    }

    private function retrieveAggregateFromVersion(AggregateId $aggregateId, int $from, int $to): ?AggregateRoot
    {
        $filter = new class($aggregateId, $from, $to) implements QueryFilter {
            public function __construct(private AggregateId $aggregateId, private $from, private int $to)
            {
            }

            public function filterQuery(): callable
            {
                return function (Builder $query): void {
                    $query
                        ->whereRaw('headers->>\'__aggregate_id\' = ' . $this->aggregateId->toString())
                        ->whereRaw('CAST(headers->>\'__aggregate_version\' AS INT) >= ' . $this->from)
                        ->whereRaw('CAST(headers->>\'__aggregate_version\' AS INT) <= ' . $this->to)
                        ->orderByRaw('CAST(headers->>\'__aggregate_version\' AS INT) ASC');
                };
            }
        };

        return $this->aggregateRepository->retrievePartially($aggregateId, $filter);
    }
}
