<?php

declare(strict_types=1);

namespace Chronhub\Snapshot\Store;

use stdClass;
use Chronhub\Snapshot\Snapshot;
use Illuminate\Database\Connection;
use Illuminate\Database\Query\Builder;
use Illuminate\Database\QueryException;
use Illuminate\Database\ConnectionInterface;
use Chronhub\Chronicler\Exception\QueryFailure;
use Chronhub\Snapshot\Serializer\SnapshotSerializer;
use Chronhub\Foundation\Support\Contracts\Clock\Clock;
use Chronhub\Foundation\Support\Contracts\Clock\PointInTime;
use Chronhub\Projector\Support\Contracts\ProjectionQueryScope;
use Chronhub\Chronicler\Support\Traits\HasConnectionTransaction;
use function is_resource;
use function stream_get_contents;

final class ConnectionSnapshotStore implements SnapshotStore
{
    use HasConnectionTransaction;

    public function __construct(protected ConnectionInterface|Connection $connection,
                                private SnapshotSerializer $serializer,
                                private ProjectionQueryScope $queryScope,
                                private Clock $clock,
                                private string $tableName = 'snapshots',
                                private array $mappingTableNames = [])
    {
    }

    public function get(string $aggregateType, string $aggregateId): ?Snapshot
    {
        try {
            $result = $this->queryBuilder($aggregateType)
                ->where('aggregate_id', $aggregateId)
                ->orderBy('last_version', 'DESC')
                ->first();
        } catch (QueryException $queryException) {
            throw QueryFailure::fromQueryException($queryException);
        }

        if ( ! $result instanceof stdClass) {
            return null;
        }

        return new Snapshot(
            $aggregateType,
            $aggregateId,
            $this->unserializeAggregateRoot($result->aggregate_root),
            (int) $result->last_version,
            $this->createDatetimeFrom($result->created_at)->dateTime()
        );
    }

    public function save(Snapshot ...$snapshots): void
    {
        if (empty($snapshots)) {
            return;
        }

        $this->beginTransaction();

        try {
            foreach ($snapshots as $snapshot) {
                $this->queryBuilder($snapshot->aggregateType())
                    ->where('aggregate_id', $snapshot->aggregateId())
                    ->delete();
            }

            foreach ($snapshots as $snapshot) {
                $this->queryBuilder($snapshot->aggregateType())
                    ->insert([
                        'aggregate_id' => $snapshot->aggregateId(),
                        'aggregate_type' => $snapshot->aggregateType(),
                        'last_version' => $snapshot->lastVersion(),
                        'created_at' => $this->clock->fromDateTime($snapshot->createdAt())->toString(),
                        'aggregate_root' => $this->serializer->serialize($snapshot->aggregateRoot()),
                    ]);
            }
        } catch (QueryException $queryException) {
            $this->rollbackTransaction();

            throw QueryFailure::fromQueryException($queryException);
        }

        $this->commitTransaction();
    }

    public function deleteAll(string $aggregateType): void
    {
        $this->beginTransaction();

        try {
            $result = $this->queryBuilder($aggregateType)
                ->where('aggregate_type', $aggregateType)
                ->delete();
        } catch (QueryException $exception) {
            $this->rollbackTransaction();

            throw QueryFailure::fromQueryException($exception);
        }

        if (0 === $result) {
            throw new QueryFailure("Unable to delete snapshots from $aggregateType");
        }

        $this->commitTransaction();
    }

    public function queryScope(): ProjectionQueryScope
    {
        return $this->queryScope;
    }

    private function queryBuilder(string $aggregateType): Builder
    {
        return $this->connection->table($this->determineTableName($aggregateType));
    }

    private function determineTableName(string $aggregateType): string
    {
        if ($tableName = $this->mappingTableNames[$aggregateType] ?? null) {
            return $tableName;
        }

        return $this->tableName;
    }

    private function unserializeAggregateRoot($serialized)
    {
        if (is_resource($serialized)) {
            $serialized = stream_get_contents($serialized);
        }

        return $this->serializer->unserialize($serialized);
    }

    private function createDatetimeFrom(string $datetime): PointInTime
    {
        return $this->clock->fromString($datetime);
    }
}
