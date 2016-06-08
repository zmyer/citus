/*
 * node_metadata.c
 *	  Functions that operate on pg_dist_node
 *
 * Copyright (c) 2012-2016, Citus Data, Inc.
 */
#include "postgres.h"
#include "miscadmin.h"


#include "access/genam.h"
#include "access/heapam.h"
#include "access/htup.h"
#include "access/htup_details.h"
#include "access/skey.h"
#include "access/stratnum.h"
#include "access/tupmacs.h"
#include "access/xact.h"
#include "catalog/indexing.h"
#include "commands/sequence.h"
#include "distributed/master_protocol.h"
#include "distributed/metadata_cache.h"
#include "distributed/pg_dist_node.h"
#include "lib/stringinfo.h"
#include "storage/lock.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/rel.h"
#include "utils/relcache.h"


#define GROUPID_SEQUENCE_NAME "pg_dist_groupid_seq"
#define NODEID_SEQUENCE_NAME "pg_dist_node_nodeid_seq"


/* */
int GroupSize = 1;

/* local function forward declarations */
void InsertNodedRow(char *nodename, int32 nodeport, char noderole, uint64 groupId);
uint64 GetNodeCountInGroup(uint64 groupId);
char NodeRole(uint64 groupId);
uint32 NextGroupId(void);
char *
InsertNodeCommand(char *nodename, int nodeport, char noderole, uint64 groupId);
int GetMaxGroupId(void);

/* declarations for dynamic loading */
PG_FUNCTION_INFO_V1(cluster_add_node);
PG_FUNCTION_INFO_V1(master_get_next_groupid);
PG_FUNCTION_INFO_V1(master_get_new_nodeid);


/*
 * cluster_add_node
 *
 */
Datum
cluster_add_node(PG_FUNCTION_ARGS)
{
	text *nodeName = PG_GETARG_TEXT_P(0);
	int32 nodePort = PG_GETARG_INT32(1);
	int32 groupId = PG_GETARG_INT32(2);

	char *nodeNameString = text_to_cstring(nodeName);
	char nodeRole = '\0';
	char *insertNodeCommand = NULL;

	/* acquire a lock so that no one can do this concurrently ??*/

	/* check that this already started */

	/* user lets Citus to decide on the group that the newly added node should be in */
	if (groupId == 0)
	{
		groupId = NextGroupId();
	}

	nodeRole = NodeRole(groupId);

	InsertNodedRow(nodeNameString, nodePort, nodeRole, groupId);

	insertNodeCommand = InsertNodeCommand(nodeNameString, nodePort, nodeRole, groupId);

	// insert the node to the worker


	PG_RETURN_BOOL(true);
}

/*
 * InsertShardRow opens the shard system catalog, and inserts a new row with the
 * given values into that system catalog. Note that we allow the user to pass in
 * null min/max values in case they are creating an empty shard.
 *
 * TODO: update the comment!!
 */
void
InsertNodedRow(char *nodename, int32 nodeport, char noderole, uint64 groupId)
{
	Relation pgDistNode = NULL;
	TupleDesc tupleDescriptor = NULL;
	HeapTuple heapTuple = NULL;
	Datum nextNodeId = master_get_new_nodeid(NULL);
	Datum values[Natts_pg_dist_node];
	bool isNulls[Natts_pg_dist_node];

	/* form new shard tuple */
	memset(values, 0, sizeof(values));
	memset(isNulls, false, sizeof(isNulls));

	values[Anum_pg_dist_node_nodeid - 1] = Int32GetDatum(nextNodeId);
	values[Anum_pg_dist_node_nodename - 1] = CStringGetTextDatum(nodename);
	values[Anum_pg_dist_node_nodeport - 1] = Int32GetDatum(nodeport);
	values[Anum_pg_dist_node_noderole - 1] = CharGetDatum(noderole);
	values[Anum_pg_dist_node_groupid - 1] = Int64GetDatum(groupId);


	/* open shard relation and insert new tuple */
	pgDistNode = heap_open(DistNodeRelationId(), RowExclusiveLock);

	tupleDescriptor = RelationGetDescr(pgDistNode);
	heapTuple = heap_form_tuple(tupleDescriptor, values, isNulls);

	simple_heap_insert(pgDistNode, heapTuple);
	CatalogUpdateIndexes(pgDistNode, heapTuple);

	CommandCounterIncrement();

	/* close relation and invalidate previous cache entry */
	heap_close(pgDistNode, RowExclusiveLock);
}



/*
 * DistributionCreateCommands generates a commands that can be
 * executed to replicate the metadata for a distributed table.
 */
char *
InsertNodeCommand(char *nodename, int nodeport, char noderole, uint64 groupId)
{
	StringInfo insertNodeCommand = makeStringInfo();

	appendStringInfo(insertNodeCommand,
					 "INSERT INTO pg_dist_node "
					 "(nodename, nodeport, noderole, groupid) "
					 "VALUES "
					 "(%s, %d, %c, %ld)",
					 nodename,
					 nodeport,
					 noderole,
					 groupId);

	return insertNodeCommand->data;
}


/*
 * check the group size
 */
uint32 NextGroupId()
{
	uint64 nextGroupId = 0;
	uint64 maxGroupIdInt = GetMaxGroupId();
	uint64 nodeCountInMaxGroupId = GetNodeCountInGroup(maxGroupIdInt);

	if (nodeCountInMaxGroupId == 0 || nodeCountInMaxGroupId == GroupSize)
	{
		Datum nextGroupIdDatum = master_get_next_groupid(NULL);

		nextGroupId = DatumGetInt64(nextGroupIdDatum);
	}
	else
	{
		nextGroupId = maxGroupIdInt;
	}

	return nextGroupId;
}

char NodeRole(uint64 groupId)
{
	uint64 nodeCountInMaxGroupId = GetNodeCountInGroup(groupId);
	char nodeRole = '\0';

	if (nodeCountInMaxGroupId == 0)
	{
		nodeRole = NODE_ROLE_PRIMARY;
	}
	else
	{
		nodeRole = NODE_ROLE_SECONDARY;
	}

	return nodeRole;
}


// SELECT count(*) FROM pg_dist_node WHERE groupid = groupId;

uint64 GetNodeCountInGroup(uint64 groupId)
{
	SysScanDesc scanDescriptor = NULL;
	ScanKeyData scanKey[1];
	int scanKeyCount = 1;
	HeapTuple heapTuple = NULL;
	uint64 elementCountInGroup = 0;

	Relation pgDistNode = heap_open(DistNodeRelationId(), AccessShareLock);

	ScanKeyInit(&scanKey[0], Anum_pg_dist_node_groupid,
				BTEqualStrategyNumber, F_INT8EQ, Int64GetDatum(groupId));

	scanDescriptor = systable_beginscan(pgDistNode,
										InvalidOid, false,
										NULL, scanKeyCount, scanKey);

	heapTuple = systable_getnext(scanDescriptor);
	while (HeapTupleIsValid(heapTuple))
	{
		++elementCountInGroup;

		heapTuple = systable_getnext(scanDescriptor);
	}

	systable_endscan(scanDescriptor);
	heap_close(pgDistNode, AccessShareLock);

	return elementCountInGroup;
}


/*
 * master_get_new_groupid allocates and returns a unique groupId for the group
 * to be created. This allocation occurs both in shared memory and in write
 * ahead logs; writing to logs avoids the risk of having groupId collisions.
 *
 * Please note that the caller is still responsible for finalizing node data
 * and the groupId with the master node. Further note that this function relies
 * on an internal sequence created in initdb to generate unique identifiers.
 *
 * NB: This can be called by any user; for now we have decided that that's
 * ok. We might want to restrict this to users part of a specific role or such
 * at some later point.
 */
Datum
master_get_next_groupid(PG_FUNCTION_ARGS)
{
	text *sequenceName = cstring_to_text(GROUPID_SEQUENCE_NAME);
	Oid sequenceId = ResolveRelationId(sequenceName);
	Datum sequenceIdDatum = ObjectIdGetDatum(sequenceId);
	Oid savedUserId = InvalidOid;
	int savedSecurityContext = 0;
	Datum groupIdDatum = 0;

	GetUserIdAndSecContext(&savedUserId, &savedSecurityContext);
	SetUserIdAndSecContext(CitusExtensionOwner(), SECURITY_LOCAL_USERID_CHANGE);

	/* generate new and unique shardId from sequence */
	groupIdDatum = DirectFunctionCall1(nextval_oid, sequenceIdDatum);

	SetUserIdAndSecContext(savedUserId, savedSecurityContext);

	PG_RETURN_DATUM(groupIdDatum);
}


/*
 * TODO: add comment
 */
int
GetMaxGroupId()
{
	SysScanDesc scanDescriptor = NULL;
	int scanKeyCount = 0;
	HeapTuple heapTuple = NULL;
	ScanKeyData scanKey[1];
	uint64 maxGroupId = 0;
	bool isNull = false;
	TupleDesc tupleDescriptor = NULL;
	Relation pgDistNode = heap_open(DistNodeRelationId(), AccessShareLock);

	scanDescriptor = systable_beginscan(pgDistNode,
										InvalidOid, false,
										NULL, scanKeyCount, scanKey);

	tupleDescriptor = RelationGetDescr(pgDistNode);

	heapTuple = systable_getnext(scanDescriptor);
	while (HeapTupleIsValid(heapTuple))
	{
		Datum currentGroupId = heap_getattr(heapTuple, Anum_pg_dist_node_groupid,
											tupleDescriptor, &isNull);
		uint64 currentGroupIdInt = DatumGetInt64(currentGroupIdInt);

		if (currentGroupId > maxGroupId)
		{
			maxGroupId = currentGroupId;
		}

		heapTuple = systable_getnext(scanDescriptor);
	}

	systable_endscan(scanDescriptor);
	heap_close(pgDistNode, AccessShareLock);

	return maxGroupId;
}


/*
 * master_get_new_shardid allocates and returns a unique shardId for the shard
 * to be created. This allocation occurs both in shared memory and in write
 * ahead logs; writing to logs avoids the risk of having shardId collisions.
 *
 * Please note that the caller is still responsible for finalizing shard data
 * and the shardId with the master node. Further note that this function relies
 * on an internal sequence created in initdb to generate unique identifiers.
 *
 * NB: This can be called by any user; for now we have decided that that's
 * ok. We might want to restrict this to users part of a specific role or such
 * at some later point.
 */
Datum
master_get_new_nodeid(PG_FUNCTION_ARGS)
{
	text *sequenceName = cstring_to_text(NODEID_SEQUENCE_NAME);
	Oid sequenceId = ResolveRelationId(sequenceName);
	Datum sequenceIdDatum = ObjectIdGetDatum(sequenceId);
	Oid savedUserId = InvalidOid;
	int savedSecurityContext = 0;
	Datum shardIdDatum = 0;

	GetUserIdAndSecContext(&savedUserId, &savedSecurityContext);
	SetUserIdAndSecContext(CitusExtensionOwner(), SECURITY_LOCAL_USERID_CHANGE);

	/* generate new and unique shardId from sequence */
	shardIdDatum = DirectFunctionCall1(nextval_oid, sequenceIdDatum);

	SetUserIdAndSecContext(savedUserId, savedSecurityContext);

	PG_RETURN_DATUM(shardIdDatum);
}
