/*-------------------------------------------------------------------------
 *
 * metadata_sync.c
 *
 * Routines for synchronizing metadata to all workers.
 *
 * Copyright (c) 2013-2016, Citus Data, Inc.
 *
 * $Id$
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "miscadmin.h"

#include <sys/stat.h>
#include <unistd.h>

#include "utils/builtins.h"
#include "distributed/citus_ruleutils.h"
#include "distributed/master_metadata_utility.h"
#include "distributed/master_protocol.h"
#include "distributed/metadata_sync.h"
#include "distributed/multi_join_order.h"
#include "distributed/multi_transaction.h"
#include "distributed/worker_manager.h"
#include "distributed/worker_transaction.h"
#include "nodes/pg_list.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"


/*
 * DistributionCreateCommands generates a commands that can be
 * executed to replicate the metadata for a distributed table.
 */
char *
DistributionCreateCommand(Oid distributedRelationId,
						   char distributionMethod,
						   char *distributionColumnName)
{
	char *distributedRelationName = NULL;
	StringInfo insertDistributionCommand = makeStringInfo();

	distributedRelationName = generate_relation_name(distributedRelationId, NIL);

	appendStringInfo(insertDistributionCommand,
					 "INSERT INTO pg_dist_partition "
					 "(logicalrelid, partmethod, partkey) "
					 "VALUES "
					 "(%s::regclass, '%c', column_name_to_column(%s,%s))",
					 quote_literal_cstr(distributedRelationName),
					 distributionMethod,
					 quote_literal_cstr(distributedRelationName),
					 quote_literal_cstr(distributionColumnName));

	return insertDistributionCommand->data;
}


/*
 * DistributionDeleteCommands generate a command that can be
 * executed to delete the metadata for a distributed table.
 */
char *
DistributionDeleteCommand(Oid distributedRelationId)
{
	char *distributedRelationName = NULL;
	StringInfo deleteDistributionCommand = makeStringInfo();

	distributedRelationName = generate_relation_name(distributedRelationId, NIL);

	appendStringInfo(deleteDistributionCommand,
					 "DELETE FROM pg_dist_partition "
					 "WHERE logicalrelid = %s::regclass",
					 quote_literal_cstr(distributedRelationName));

	return deleteDistributionCommand->data;
}


/*
 * ShardCreateCommands generates a list of commands that can be
 * executed to replicate shard and shard placement metadata for the
 * given shard intervals.
 */
List *
ShardCreateCommands(List *shardIntervalList)
{
	List *commandList = NIL;
	ListCell *shardIntervalCell = NULL;

	foreach(shardIntervalCell, shardIntervalList)
	{
		ShardInterval *shardInterval = (ShardInterval *) lfirst(shardIntervalCell);
		uint64 shardId = shardInterval->shardId;
		Oid distributedRelationId = shardInterval->relationId;
		char *distributedRelationName = NULL;
		StringInfo insertPlacementCommand = makeStringInfo();
		StringInfo insertShardCommand = makeStringInfo();

		/* TODO: consider other types */
		int minHashToken = DatumGetInt32(shardInterval->minValue);
		int maxHashToken = DatumGetInt32(shardInterval->maxValue);

		List *shardPlacementList = FinalizedShardPlacementList(shardId);
		ShardPlacement *placement = (ShardPlacement *) linitial(shardPlacementList);

		appendStringInfo(insertPlacementCommand,
						 "INSERT INTO pg_dist_shard_placement "
						 "(shardid, shardstate, shardlength,"
						 " nodename, nodeport) "
						 "VALUES "
						 "(%lu, 1, %lu, %s, %d)",
						 shardId,
						 placement->shardLength,
						 quote_literal_cstr(placement->nodeName),
						 placement->nodePort);

		commandList = lappend(commandList, insertPlacementCommand->data);

		distributedRelationName = generate_relation_name(distributedRelationId, NIL);

		appendStringInfo(insertShardCommand,
						 "INSERT INTO pg_dist_shard "
						 "(logicalrelid, shardid, shardstorage,"
						 " shardminvalue, shardmaxvalue) "
						 "VALUES "
						 "(%s::regclass, %lu, '%c', '%d', '%d')",
						 quote_literal_cstr(distributedRelationName),
						 shardId,
						 shardInterval->storageType,
						 minHashToken,
						 maxHashToken);

		commandList = lappend(commandList, insertShardCommand->data);
	}

	return commandList;
}


/*
 * ShardDeleteCommands generates a list of commands that can be
 * executed to delete the shard and shard placement metadta for
 * the given shard intervals.
 */
List *
ShardDeleteCommands(List *shardIntervalList)
{
	List *commandList = NIL;
	ListCell *shardIntervalCell = NULL;

	foreach(shardIntervalCell, shardIntervalList)
	{
		ShardInterval *shardInterval = (ShardInterval *) lfirst(shardIntervalCell);
		uint64 shardId = shardInterval->shardId;
		StringInfo deletePlacementCommand = makeStringInfo();
		StringInfo deleteShardCommand = makeStringInfo();

		appendStringInfo(deletePlacementCommand,
						 "DELETE FROM pg_dist_shard_placement "
						 "WHERE shardid = %lu",
						 shardId);

		commandList = lappend(commandList, deletePlacementCommand->data);

		appendStringInfo(deletePlacementCommand,
						 "DELETE FROM pg_dist_shard "
						 "WHERE shardid = %lu",
						 shardId);

		commandList = lappend(commandList, deleteShardCommand->data);
	}

	return commandList;
}

