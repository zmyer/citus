-- Test creation of cluster-distributed tables and metadata syncing

CREATE TABLE clustered_table (
    key text primary key,
    value jsonb
);
CREATE INDEX ON clustered_table USING GIN (value);
SELECT cluster_create_distributed_table('clustered_table', 'key', 'hash');
SELECT cluster_create_shards('clustered_table', 4);

-- Verify that we've logged commit records
SELECT count(*) FROM pg_dist_transaction;

-- Confirm that the metadata transactions have been committed
SELECT recover_prepared_transactions();

-- Verify that the commit records have been removed
SELECT count(*) FROM pg_dist_transaction;

\c - - - :worker_1_port

\d clustered_table

SELECT isowner, iscluster FROM pg_dist_partition
WHERE logicalrelid = 'clustered_table'::regclass;

SELECT count(*) FROM pg_dist_shard JOIN pg_dist_shard_placement USING (shardid)
WHERE logicalrelid = 'clustered_table'::regclass;

\c - - - :worker_2_port

\d clustered_table

SELECT isowner, iscluster FROM pg_dist_partition
WHERE logicalrelid = 'clustered_table'::regclass;

SELECT count(*) FROM pg_dist_shard JOIN pg_dist_shard_placement USING (shardid)
WHERE logicalrelid = 'clustered_table'::regclass;


