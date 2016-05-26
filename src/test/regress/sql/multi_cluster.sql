-- Test creation of cluster-distributed tables and metadata syncing

CREATE TABLE clustered_table (
    key text primary key,
    value jsonb
);
CREATE INDEX ON clustered_table USING GIN (value);
SELECT cluster_create_distributed_table('clustered_table', 'key', 'hash');
SELECT cluster_create_shards('clustered_table', 4);

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


