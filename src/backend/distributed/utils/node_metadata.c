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
#if (PG_VERSION_NUM >= 90500 && PG_VERSION_NUM < 90600)
#include "access/stratnum.h"
#else
#include "access/skey.h"
#endif
#include "access/tupmacs.h"
#include "access/xact.h"
#include "catalog/indexing.h"
#include "commands/sequence.h"
#include "distributed/master_protocol.h"
#include "distributed/metadata_cache.h"
#include "distributed/pg_dist_node.h"
#include "distributed/worker_manager.h"
#include "distributed/worker_transaction.h"
#include "lib/stringinfo.h"
#include "storage/lock.h"
#include "storage/fd.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/rel.h"
#include "utils/relcache.h"


/* default group size */
int GroupSize = 1;


/* local function forward declarations */
static StringInfo GenerateNodeInfoSummary(WorkerNode *workerNode);
static WorkerNode * FindWorkerNode(char *nodeName, int32 nodePort);
static uint32 NextGroupId(void);
static uint32 GetMaxGroupId(void);
static char NodeRole(uint32 groupId);
static uint64 GetNodeCountInGroup(uint32 groupId);
static char * InsertNodeCommand(uint32 nodeid, char *nodename, int nodeport,
								char noderole, bool nodeActive, uint32 groupId);
static List * ParseWorkerNodeFile(const char *workerNodeFilename);

/* declarations for dynamic loading */
PG_FUNCTION_INFO_V1(cluster_add_node);
PG_FUNCTION_INFO_V1(cluster_deactivate_node);
PG_FUNCTION_INFO_V1(cluster_activate_node);
PG_FUNCTION_INFO_V1(cluster_read_worker_file);
PG_FUNCTION_INFO_V1(master_get_new_nodeid);
PG_FUNCTION_INFO_V1(master_get_next_groupid);


/*
 * cluster_add_node function adds a new node to the cluster. If the node already
 * exists, the function returns with the information about the node. If not, the
 * following prodecure is followed while adding a node.
 * If the groupId is not explicitly given by the user, the function picks the
 * group that the new node should be in with respect to GroupSize. Then, the
 * new node is inserted into the local pg_dist_node.
 *
 * TODO: The following will be added in the near future.
 * Lastly, the new node is inserted to all other nodes' pg_dist_node table.
 */
Datum
cluster_add_node(PG_FUNCTION_ARGS)
{
	text *nodeName = PG_GETARG_TEXT_P(0);
	int32 nodePort = PG_GETARG_INT32(1);
	int32 groupId = PG_GETARG_INT32(2);
	char *nodeNameString = text_to_cstring(nodeName);

	char nodeRole = '\0';
	Relation pgDistNode = NULL;
	Datum nextNodeId = 0;
	int nextNodeIdInt = 0;
	char *insertCommand = NULL;
	StringInfo returnData = makeStringInfo();
	WorkerNode *workerNode = NULL;
	bool nodeActive = true;

	/* acquire a lock so that no one can do this concurrently */
	pgDistNode = heap_open(DistNodeRelationId(), ExclusiveLock);

	/* check if the node already exists in the cluster */
	workerNode = FindWorkerNode(nodeNameString, nodePort);
	if (workerNode != NULL)
	{
		/* fill return data and return */
		returnData = GenerateNodeInfoSummary(workerNode);

		/* close the heap */
		heap_close(pgDistNode, ExclusiveLock);

		PG_RETURN_CSTRING(returnData->data);
	}

	/* user lets Citus to decide on the group that the newly added node should be in */
	if (groupId == 0)
	{
		groupId = NextGroupId();
	}
	else
	{
		uint maxGroupId = GetMaxGroupId();

		if (groupId > maxGroupId)
		{
			ereport(ERROR, (errmsg("you cannot add a node to a non-existing group")));
		}
	}

	/* generate the new node id from the sequence */
	nextNodeId = master_get_new_nodeid(NULL);
	nextNodeIdInt = DatumGetInt32(nextNodeId);

	nodeRole = NodeRole(groupId);

	InsertNodedRow(nextNodeIdInt, nodeNameString, nodePort, nodeRole, nodeActive, groupId);

	insertCommand = InsertNodeCommand(nextNodeIdInt, nodeNameString, nodePort, nodeRole,
									  nodeActive, groupId);

	/* TODO: enable this once we have fully metadata sync */
	/* SendCommandToWorkersInParallel(insertCommand); */

	heap_close(pgDistNode, ExclusiveLock);

	/* fetch the worker node, and generate the output */
	workerNode = FindWorkerNode(nodeNameString, nodePort);
	returnData = GenerateNodeInfoSummary(workerNode);

	PG_RETURN_CSTRING(returnData->data);
}


/*
 * cluster_deactivate_node marks the node's nodeactive column to false. It
 * errors out if the node does not exists or already marked as inactive.
 */
Datum
cluster_deactivate_node(PG_FUNCTION_ARGS)
{
	text *nodeName = PG_GETARG_TEXT_P(0);
	int32 nodePort = PG_GETARG_INT32(1);

	char *nodeNameString = text_to_cstring(nodeName);
	WorkerNode *workerNode = FindWorkerNode(nodeNameString, nodePort);
	bool nodeActive = false;

	if (workerNode == NULL)
	{
		ereport(ERROR, (errmsg("you cannot deactivate a non-existent node")));
	}

	if (!workerNode->workerActive)
	{
		ereport(ERROR, (errmsg("node is already deactivated")));
	}

	UpdateNodeActiveColumn(workerNode, nodeActive);

	PG_RETURN_BOOL(true);
}


/*
 * cluster_activate_node marks the node's nodeactive column to true. It
 * errors out if the node does not exists or already marked as active.
 */
Datum
cluster_activate_node(PG_FUNCTION_ARGS)
{
	text *nodeName = PG_GETARG_TEXT_P(0);
	int32 nodePort = PG_GETARG_INT32(1);

	char *nodeNameString = text_to_cstring(nodeName);
	WorkerNode *workerNode = FindWorkerNode(nodeNameString, nodePort);
	bool nodeActive = true;

	if (workerNode == NULL)
	{
		ereport(ERROR, (errmsg("you cannot activate a non-existent node")));
	}

	if (workerNode->workerActive)
	{
		ereport(ERROR, (errmsg("node is already activated")));
	}

	UpdateNodeActiveColumn(workerNode, nodeActive);

	PG_RETURN_BOOL(true);
}


/*
 * cluster_activate_node marks the node's nodeactive column to true. It
 * errors out if the node does not exists or already marked as active.
 */
Datum
cluster_read_worker_file(PG_FUNCTION_ARGS)
{
	text *filePath = PG_GETARG_TEXT_P(0);
	char *filePathCStr = text_to_cstring(filePath);

	ListCell *workerNodeCell = NULL;
	List *workerNodes = ParseWorkerNodeFile(filePathCStr);

	foreach(workerNodeCell, workerNodes)
	{
		WorkerNode *workerNode = (WorkerNode *) lfirst(workerNodeCell);
		Datum workerNameDatum = PointerGetDatum(cstring_to_text(workerNode->workerName));

		DirectFunctionCall3(cluster_add_node, workerNameDatum,
							UInt32GetDatum(workerNode->workerPort),
							PointerGetDatum(NULL));
	}

	PG_RETURN_BOOL(true);
}


/*
 * GenerateNodeInfoSummary returns a StringInfo that is JSON formatted and
 * includes the information about the given workerNode.
 */
static StringInfo
GenerateNodeInfoSummary(WorkerNode *workerNode)
{
	StringInfo returnData = makeStringInfo();

	appendStringInfo(returnData, "{ \"host\": \"%s\", \"port\": %d, \"group\": %d, "
								 "\"role\": \"%c\", \"active\": \"%s\"}",
					 workerNode->workerName,
					 workerNode->workerPort, workerNode->groupId, workerNode->workerRole,
					 workerNode->workerActive ? "true" : "false");

	return returnData;
}


/*
 * FindWorkerNode iterates of the worker nodes and returns the workerNode
 * if it already exists. Else, the function returns NULL.
 */
static WorkerNode *
FindWorkerNode(char *nodeName, int32 nodePort)
{
	WorkerNode *workerNode = NULL;
	HTAB *workerNodeHash = GetWorkerNodeHash();
	HASH_SEQ_STATUS status;

	hash_seq_init(&status, workerNodeHash);

	while ((workerNode = hash_seq_search(&status)) != NULL)
	{
		if (strncasecmp(nodeName, workerNode->workerName, WORKER_LENGTH) == 0 &&
			nodePort == workerNode->workerPort)
		{
			/* we need to terminate the scan since we break */
			hash_seq_term(&status);
			break;
		}
	}

	return workerNode;
}


/*
 * NextGroupId returns the next group that that can be assigned to a node. If the
 * group is full (i.e., it has equal or more elements than GroupSize), a new group id
 * is generated and returned. Else, the current maximum group id is returned.
 */
static uint32
NextGroupId()
{
	uint32 nextGroupId = 0;
	uint32 maxGroupIdInt = GetMaxGroupId();
	uint64 nodeCountInMaxGroupId = GetNodeCountInGroup(maxGroupIdInt);

	if (nodeCountInMaxGroupId == 0 || nodeCountInMaxGroupId >= GroupSize)
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


/*
 * GetMaxGroupId iterates over the rows of pg_dist_node table, and returns the maximum
 * group id from the table.
 */
static uint32
GetMaxGroupId()
{
	SysScanDesc scanDescriptor = NULL;
	int scanKeyCount = 0;
	HeapTuple heapTuple = NULL;
	ScanKeyData scanKey[1];
	uint32 maxGroupId = 0;
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
		uint32 currentGroupIdInt = DatumGetInt32(currentGroupId);

		if (currentGroupIdInt > maxGroupId)
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
 * NodeRole function returns the role of the node in the given groupId. First node in
 * the group is assigned to be primary, all others are assigned to be secondaries.
 */
static char
NodeRole(uint32 groupId)
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


/*
 * GetNodeCountInGroup iterates over the rows of pg_dist_node table, and returns the
 * element count with the given groupId.
 */
static uint64
GetNodeCountInGroup(uint32 groupId)
{
	SysScanDesc scanDescriptor = NULL;
	ScanKeyData scanKey[1];
	int scanKeyCount = 1;
	HeapTuple heapTuple = NULL;
	uint64 elementCountInGroup = 0;

	Relation pgDistNode = heap_open(DistNodeRelationId(), AccessShareLock);

	ScanKeyInit(&scanKey[0], Anum_pg_dist_node_groupid,
				BTEqualStrategyNumber, F_INT4EQ, Int64GetDatum(groupId));

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
 * DistributionCreateCommands generates a commands that can be
 * executed to replicate the metadata for a distributed table.
 */
static char *
InsertNodeCommand(uint32 nodeid, char *nodename, int nodeport, char noderole,
				  bool nodeActive, uint32 groupId)
{
	StringInfo insertNodeCommand = makeStringInfo();

	appendStringInfo(insertNodeCommand,
					 "INSERT INTO pg_dist_node " /*TODO: add a ON CONFLICT clause */
					 "(nodeid, nodename, nodeport, noderole, nodeactive, groupid) "
					 "VALUES "
					 "(%d, '%s', %d, '%c', %s , %d);",
					 nodeid,
					 nodename,
					 nodeport,
					 noderole,
					 nodeActive ? "true" : "false",
					 groupId);

	return insertNodeCommand->data;
}


/*
 * ParseWorkerNodeFile opens and parses the node name and node port from the
 * specified configuration file.
 * Note that this function is deprecated. Do not use this function for any new
 * features.
 */
static List *
ParseWorkerNodeFile(const char *workerNodeFilename)
{
	FILE *workerFileStream = NULL;
	List *workerNodeList = NIL;
	char workerNodeLine[MAXPGPATH];
	char *workerFilePath = make_absolute_path(workerNodeFilename);
	char *workerPatternTemplate = "%%%u[^# \t]%%*[ \t]%%%u[^# \t]%%*[ \t]%%%u[^# \t]";
	char workerLinePattern[1024];
	const int workerNameIndex = 0;
	const int workerPortIndex = 1;

	memset(workerLinePattern, '\0', sizeof(workerLinePattern));

	workerFileStream = AllocateFile(workerFilePath, PG_BINARY_R);
	if (workerFileStream == NULL)
	{
		if (errno == ENOENT)
		{
			ereport(DEBUG1, (errmsg("worker list file located at \"%s\" is not present",
									workerFilePath)));
		}
		else
		{
			ereport(ERROR, (errcode_for_file_access(),
							errmsg("could not open worker list file \"%s\": %m",
								   workerFilePath)));
		}
		return NIL;
	}

	/* build pattern to contain node name length limit */
	snprintf(workerLinePattern, sizeof(workerLinePattern), workerPatternTemplate,
			 WORKER_LENGTH, MAX_PORT_LENGTH, WORKER_LENGTH);

	while (fgets(workerNodeLine, sizeof(workerNodeLine), workerFileStream) != NULL)
	{
		const int workerLineLength = strnlen(workerNodeLine, MAXPGPATH);
		WorkerNode *workerNode = NULL;
		char *linePointer = NULL;
		int32 nodePort = 5432; /* default port number */
		int fieldCount = 0;
		bool lineIsInvalid = false;
		char nodeName[WORKER_LENGTH + 1];
		char nodeRack[WORKER_LENGTH + 1];
		char nodePortString[MAX_PORT_LENGTH + 1];

		memset(nodeName, '\0', sizeof(nodeName));
		//strlcpy(nodeRack, WORKER_DEFAULT_RACK, sizeof(nodeRack));
		memset(nodePortString, '\0', sizeof(nodePortString));

		if (workerLineLength == MAXPGPATH - 1)
		{
			ereport(ERROR, (errcode(ERRCODE_CONFIG_FILE_ERROR),
							errmsg("worker node list file line exceeds the maximum "
								   "length of %d", MAXPGPATH)));
		}

		/* trim trailing newlines preserved by fgets, if any */
		linePointer = workerNodeLine + workerLineLength - 1;
		while (linePointer >= workerNodeLine &&
			   (*linePointer == '\n' || *linePointer == '\r'))
		{
			*linePointer-- = '\0';
		}

		/* skip leading whitespace */
		for (linePointer = workerNodeLine; *linePointer; linePointer++)
		{
			if (!isspace((unsigned char) *linePointer))
			{
				break;
			}
		}

		/* if the entire line is whitespace or a comment, skip it */
		if (*linePointer == '\0' || *linePointer == '#')
		{
			continue;
		}

		/* parse line; node name is required, but port and rack are optional */
		fieldCount = sscanf(linePointer, workerLinePattern,
							nodeName, nodePortString, nodeRack);

		/* adjust field count for zero based indexes */
		fieldCount--;

		/* raise error if no fields were assigned */
		if (fieldCount < workerNameIndex)
		{
			lineIsInvalid = true;
		}

		/* no special treatment for nodeName: already parsed by sscanf */

		/* if a second token was specified, convert to integer port */
		if (fieldCount >= workerPortIndex)
		{
			char *nodePortEnd = NULL;

			errno = 0;
			nodePort = strtol(nodePortString, &nodePortEnd, 10);

			if (errno != 0 || (*nodePortEnd) != '\0' || nodePort <= 0)
			{
				lineIsInvalid = true;
			}
		}

		if (lineIsInvalid)
		{
			ereport(ERROR, (errcode(ERRCODE_CONFIG_FILE_ERROR),
							errmsg("could not parse worker node line: %s",
								   workerNodeLine),
							errhint("Lines in the worker node file must contain a valid "
									"node name and, optionally, a positive port number. "
									"Comments begin with a '#' character and extend to "
									"the end of their line.")));
		}

		/* allocate worker node structure and set fields */
		workerNode = (WorkerNode *) palloc0(sizeof(WorkerNode));

		strlcpy(workerNode->workerName, nodeName, WORKER_LENGTH);
		//strlcpy(workerNode->workerRack, nodeRack, WORKER_LENGTH);
		workerNode->workerPort = nodePort;
		workerNode->workerActive = true;

		workerNodeList = lappend(workerNodeList, workerNode);
	}

	FreeFile(workerFileStream);
	free(workerFilePath);

	return workerNodeList;
}


/*
 * master_get_new_nodeid allocates and returns a unique nodeId for the node
 * to be added. This allocation occurs both in shared memory and in write
 * ahead logs; writing to logs avoids the risk of having nodeId collisions.
 *
 * Please note that the caller is still responsible for finalizing node data
 * and the nodeId with the master node. Further note that this function relies
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


/*
 * master_get_next_groupid allocates and returns a unique groupId for the group
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
