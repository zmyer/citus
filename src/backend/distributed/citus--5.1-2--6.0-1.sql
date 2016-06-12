/* citus--5.1-3--6.0-1.sql */

SET search_path = 'pg_catalog';

CREATE TABLE citus.pg_dist_transaction (
    groupid int NOT NULL,
    gid text NOT NULL
);
CREATE INDEX pg_dist_transaction_group_index
ON citus.pg_dist_transaction using btree(groupid);
ALTER TABLE citus.pg_dist_transaction SET SCHEMA pg_catalog;
ALTER TABLE pg_catalog.pg_dist_transaction
ADD CONSTRAINT pg_dist_transaction_unique_constraint UNIQUE (groupid, gid);
GRANT SELECT ON pg_catalog.pg_dist_transaction TO public;

CREATE FUNCTION column_to_column_name(table_name regclass, column_var text)
    RETURNS text
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$column_to_column_name$$;
COMMENT ON FUNCTION column_to_column_name(table_name regclass, column_var text)
    IS 'convert a textual Var representation of a column to a column name';

CREATE FUNCTION column_name_to_column(table_name regclass, column_name text)
    RETURNS text
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$column_name_to_column$$;
COMMENT ON FUNCTION column_name_to_column(table_name regclass, column_name text)
    IS 'convert a column name to its textual Var representation';

CREATE FUNCTION cluster_create_distributed_table(table_name regclass,
                                                 distribution_column text,
                                                 distribution_method citus.distribution_type)
    RETURNS void
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$cluster_create_distributed_table$$;
COMMENT ON FUNCTION cluster_create_distributed_table(table_name regclass,
                                                     distribution_column text,
                                                     distribution_method citus.distribution_type)
    IS 'turn a table into a cluster-distributed table';

CREATE FUNCTION cluster_create_shards(table_name regclass, shard_count integer)
    RETURNS void
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$cluster_create_shards$$;
COMMENT ON FUNCTION cluster_create_shards(table_name regclass, shard_count integer)
    IS 'create shards for a cluster-distributed table';

CREATE FUNCTION recover_prepared_transactions()
    RETURNS void
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$recover_prepared_transactions$$;
COMMENT ON FUNCTION recover_prepared_transactions()
    IS 'recover prepared transactions started by this node';

ALTER TABLE pg_dist_partition ADD COLUMN isowner bool DEFAULT true NOT NULL;
ALTER TABLE pg_dist_partition ADD COLUMN iscluster bool DEFAULT false NOT NULL;

RESET search_path;
