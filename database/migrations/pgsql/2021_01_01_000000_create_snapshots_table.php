<?php

declare(strict_types=1);

use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Schema;
use Illuminate\Database\Migrations\Migration;

final class CreateSnapshotsTable extends Migration
{
    public function up(): void
    {
        DB::statement(
            'CREATE TABLE snapshots (
  aggregate_id VARCHAR(150) NOT NULL,
  aggregate_type VARCHAR(150) NOT NULL,
  last_version INT NOT NULL,
  created_at CHAR(26) NOT NULL,
  aggregate_root BYTEA,
  PRIMARY KEY (aggregate_id)
        )');
    }

    public function down(): void
    {
        Schema::dropIfExists('snapshots');
    }
}
