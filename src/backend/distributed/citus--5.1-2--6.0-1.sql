/* citus--5.1-1--6.0-1.sql */

SET search_path = 'pg_catalog';

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

ALTER TABLE pg_dist_partition ADD COLUMN isowner bool DEFAULT true NOT NULL;
ALTER TABLE pg_dist_partition ADD COLUMN iscluster bool DEFAULT false NOT NULL;

CREATE TABLE citus.pg_dist_node(
    nodeid int NOT NULL,
    nodename text NOT NULL,
    nodeport int NOT NULL,
    noderole "char" NOT NULL,
    nodeactive bool NOT NULL,
    groupid bigint NOT NULL
);

CREATE SEQUENCE citus.pg_dist_groupid_seq
    MINVALUE 1
    MAXVALUE 4294967296;

CREATE SEQUENCE citus.pg_dist_node_nodeid_seq
    MINVALUE 1
    MAXVALUE 4294967296;

ALTER TABLE citus.pg_dist_node SET SCHEMA pg_catalog;
ALTER SEQUENCE citus.pg_dist_groupid_seq SET SCHEMA pg_catalog;
ALTER SEQUENCE citus.pg_dist_node_nodeid_seq SET SCHEMA pg_catalog;

CREATE FUNCTION master_dist_node_cache_invalidate()
    RETURNS trigger
    LANGUAGE C
    AS 'MODULE_PATHNAME', $$master_dist_node_cache_invalidate$$;
COMMENT ON FUNCTION master_dist_node_cache_invalidate()
    IS 'register node cache invalidation for changed rows';

CREATE FUNCTION cluster_add_node(nodename text,
                                 nodeport integer,
                                 groupid integer DEFAULT 0)
    RETURNS CSTRING
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$cluster_add_node$$;
COMMENT ON FUNCTION cluster_add_node(nodename text,
                                     nodeport integer,
                                     groupid integer)
    IS 'add nodes to the cluster';

CREATE FUNCTION cluster_deactivate_node(nodename text,
                                        nodeport integer)
    RETURNS BOOL
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$cluster_deactivate_node$$;
COMMENT ON FUNCTION cluster_deactivate_node(nodename text,
                                            nodeport integer)
    IS 'deactivates a node in the cluster';

CREATE FUNCTION cluster_activate_node(nodename text,
                                        nodeport integer)
    RETURNS BOOL
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$cluster_activate_node$$;
COMMENT ON FUNCTION cluster_activate_node(nodename text,
                                            nodeport integer)
    IS 'activates a node in the cluster';

CREATE FUNCTION cluster_read_worker_file(filepath text DEFAULT 'pg_worker_list.conf')
    RETURNS BOOL
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$cluster_read_worker_file$$;
COMMENT ON FUNCTION cluster_read_worker_file(nodename text)
    IS 'reads nodes from a file in the cluster';

/* create invalidation triiger for pg_dist_node change */
CREATE TRIGGER dist_node_cache_invalidate
    AFTER INSERT OR UPDATE OR DELETE
    ON pg_catalog.pg_dist_node
    FOR EACH ROW EXECUTE PROCEDURE master_dist_node_cache_invalidate();

RESET search_path;
