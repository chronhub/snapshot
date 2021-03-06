<?php

declare(strict_types=1);

namespace Chronhub\Snapshot;

use Illuminate\Support\Arr;
use Illuminate\Contracts\Config\Repository;
use Illuminate\Contracts\Foundation\Application;
use Chronhub\Snapshot\Exception\RuntimeException;
use Chronhub\Foundation\Aggregate\AggregateChanged;
use Chronhub\Projector\Context\ContextualReadModel;
use Chronhub\Snapshot\Projection\SnapshotReadModel;
use Chronhub\Snapshot\Projection\SnapshotRepository;
use Chronhub\Foundation\Support\Contracts\Clock\Clock;
use Chronhub\Projector\Support\Contracts\ServiceManager;
use Chronhub\Projector\Support\Contracts\ProjectorFactory;
use Chronhub\Projector\Support\Contracts\Support\ReadModel;
use Chronhub\Chronicler\Support\Contracts\Factory\RepositoryManager;
use Chronhub\Chronicler\Support\Contracts\Aggregate\AggregateRepositoryWithSnapshotting;
use function count;
use function is_string;

class SnapshotProjectionManager
{
    public const PROJECTION_SUFFIX = '_snapshot';

    protected array $config;

    public function __construct(private RepositoryManager $repositoryManager,
                                private ServiceManager $projectorServiceManager,
                                private Application $app)
    {
        $this->config = $app->get(Repository::class)->get('chronicler');
    }

    public function create(string $streamName): ProjectorFactory
    {
        $streamConfig = $this->fromChronicler("repositories.$streamName");

        $streamName = $streamConfig['stream_name'] ?? $streamName;

        $aggregateTypes = $this->determineAggregateTypes($streamConfig);

        $snapshotConfig = $streamConfig['snapshot'] ?? [];

        if (0 === count($snapshotConfig)) {
            throw new RuntimeException("Invalid snapshot configuration for stream $streamName");
        }

        $readModel = $this->createSnapshotReadModel($streamName, $snapshotConfig, $aggregateTypes);

        return $this->resolveSnapshotProjection($readModel, $streamName, $snapshotConfig);
    }

    protected function resolveSnapshotProjection(ReadModel $readModel,
                                                 string $streamName,
                                                 array $config): ProjectorFactory
    {
        $projector = $config['projector'];

        $projectorManager = $this->projectorServiceManager->create($projector['name']);
        $options = $projector['options'] ?? [];

        if (is_string($options)) {
            $options = $this->app[Repository::class]->get("projector.options.$options", []);
        }

        $snapshotStreamName = $config['stream_name'] ?? $streamName . self::PROJECTION_SUFFIX;

        return $projectorManager
            ->createReadModelProjection($snapshotStreamName, $readModel, $options)
            ->withQueryFilter($projectorManager->queryScope()->fromIncludedPosition())
            ->whenAny(function (AggregateChanged $event): void {
                /* @var ContextualReadModel $this */
                $this->readModel()->stack('snapshot ...', $event);
            });
    }

    protected function createSnapshotReadModel(string $streamName, array $config, array $aggregateTypes): ReadModel
    {
        $snapshotServiceId = $config['store'] ?? null;

        if ( ! is_string($snapshotServiceId) || ! $this->app->bound($snapshotServiceId)) {
            throw new RuntimeException("Invalid snapshot store service id for stream $streamName");
        }

        $repository = $this->repositoryManager->create($streamName);

        if ( ! $repository instanceof AggregateRepositoryWithSnapshotting) {
            throw new RuntimeException("Aggregate repository for $streamName must implement contract " . AggregateRepositoryWithSnapshotting::class);
        }

        $snapshotRepository = new SnapshotRepository(
            $this->app->get($snapshotServiceId),
            $repository,
            $this->app->get(Clock::class),
            $config['persist_every_x_events'] ?? 1000,
        );

        return new SnapshotReadModel($snapshotRepository, $aggregateTypes, );
    }

    protected function determineAggregateTypes(array $config): array
    {
        $aggregateTypes = $config['aggregate_type'];

        if (is_string($aggregateTypes)) {
            return [$aggregateTypes];
        }

        return [$aggregateTypes['root']];
    }

    protected function fromChronicler(string $key, $default = null)
    {
        return Arr::get($this->config, $key, $default);
    }
}
