<?php

declare(strict_types=1);

namespace Chronhub\Snapshot;

use Illuminate\Support\ServiceProvider;
use Illuminate\Contracts\Foundation\Application;
use function is_string;

class SnapshotServiceProvider extends ServiceProvider
{
    /**
     * @var Application
     */
    public $app;

    public function boot(): void
    {
        if ($this->app->runningInConsole()) {
            $this->publishes(
                [$this->getConfigPath() => config_path('snapshot.php')],
                'config'
            );

            $loadMigrationFrom = config('snapshot.load_migrations_from');

            if (is_string($loadMigrationFrom)) {
                $this->loadMigrationsFrom(__DIR__ . '/../database/migrations/' . $loadMigrationFrom);
            }
        }
    }

    public function register(): void
    {
        $this->mergeConfigFrom($this->getConfigPath(), 'snapshot');

        $this->app->singleton(SnapshotManager::class, SnapshotManager::class);
        $this->app->singleton(SnapshotProjectionManager::class);
    }

    private function getConfigPath(): string
    {
        return __DIR__ . '/../config/snapshot.php';
    }
}
