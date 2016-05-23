/*-------------------------------------------------------------------------
 *
 * metadata_sync.h
 *	  Type and function declarations used to sync metadata across all
 *	  workers.
 *
 * Copyright (c) 2016, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef METADATA_SYNC_H
#define METADATA_SYNC_H


#include "nodes/pg_list.h"


/* Functions declarations for metadata syncing */
extern char * DistributionCreateCommand(Oid distributedRelationId,
										char distributionMethod,
										char *distributionColumnName);
extern char * DistributionDeleteCommand(Oid distributedRelationId);
extern List * ShardCreateCommands(List *shardIntervalList);
extern List * ShardDeleteCommands(List *shardIntervalList);


#endif /* METADATA_SYNC_H */
