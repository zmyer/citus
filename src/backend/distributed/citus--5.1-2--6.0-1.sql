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

-- TODO: Is there a better place to define the followings?
CREATE TABLE citus.pg_dist_node(
    nodeid int NOT NULL,
    nodename text NOT NULL,
    nodeport int NOT NULL,
    noderole "char" NOT NULL,
    groupid bigint NOT NULL
);

CREATE SEQUENCE citus.pg_dist_groupid_seq
    MINVALUE 1
    MAXVALUE 4294967296;

CREATE SEQUENCE citus.pg_dist_node_nodeid_seq
    MINVALUE 1
    MAXVALUE 4294967296;

ALTER TABLE citus.pg_dist_node SET SCHEMA pg_catalog;
ALTER SEQUENCE  citus.pg_dist_groupid_seq SET SCHEMA pg_catalog;
ALTER SEQUENCE  citus.pg_dist_node_nodeid_seq SET SCHEMA pg_catalog;

CREATE FUNCTION cluster_add_node(nodename text,
                                 nodeport integer,
                                 groupid integer)
    RETURNS bool
    LANGUAGE C STRICT
    AS 'MODULE_PATHNAME', $$cluster_add_node$$;
COMMENT ON FUNCTION cluster_add_node(nodename text,
                                     nodeport integer,
                                     groupid integer)
    IS 'add nodes to the cluster';

RESET search_path;
