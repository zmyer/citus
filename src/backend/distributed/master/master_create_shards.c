/*-------------------------------------------------------------------------
 *
 * master_create_shards.c
 *
 * This file contains functions to distribute a table by creating shards for it
 * across a set of worker nodes.
 *
 * Copyright (c) 2014-2016, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "c.h"
#include "fmgr.h"
#include "libpq-fe.h"
#include "miscadmin.h"
#include "port.h"

#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <sys/errno.h>
#include <sys/socket.h>
#include <unistd.h>

#include "access/htup.h"
#include "catalog/namespace.h"
#include "catalog/pg_class.h"
#include "catalog/pg_type.h"
#include "distributed/connection_cache.h"
#include "distributed/listutils.h"
#include "distributed/master_metadata_utility.h"
#include "distributed/master_protocol.h"
#include "distributed/multi_join_order.h"
#include "distributed/pg_dist_partition.h"
#include "distributed/pg_dist_shard.h"
#include "distributed/resource_lock.h"
#include "distributed/worker_manager.h"
#include "executor/spi.h"
#include "lib/stringinfo.h"
#include "libpq/ip.h"
#include "nodes/pg_list.h"
#include "nodes/primnodes.h"
#include "postmaster/postmaster.h"
#include "storage/fd.h"
#include "storage/lock.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/errcodes.h"
#include "utils/lsyscache.h"
#include "utils/palloc.h"
#include "utils/syscache.h"

/* TODO: use existing definitions */
#define BEGIN_COMMAND "BEGIN"
#define COMMIT_COMMAND "COMMIT"
#define ROLLBACK_COMMAND "ROLLBACK"


/* local function forward declarations */
static void CheckHashPartitionedTable(Oid distributedTableId);
static text * IntegerToText(int32 value);
static bool ExecuteRemoteCommandPgShard(PGconn *connection, const char *sqlCommand);


static void CheckHostnameCallback(struct sockaddr *address, struct sockaddr *netmask,
								  void *callbackData);
static char * LogGlobalTransactionStart(void);
static bool UpdateGlobalTransactionStatus(char *transactionId,
										  TransactionStatus finalStatus);
static char * GenerateFunctionCallTemplate(Oid functionId);
static int DecodeFunctionArgValuesAndTypes(Oid functionId, ArrayType *argArray,
										   char ***argValuesOut, Oid **argTypesOut);
static bool PrepareRemoteTransaction(char *nodeName, uint32 nodePort, char *transactionId,
									 char *functionCallTemplate, char **argValues,
									 Oid *argTypes, int argCount);
static TransactionStatus FinishGlobalTransaction(char *transactionId);
static TransactionStatus GetGlobalTransactionStatus(char *transactionId);
static void DeleteGlobalTransactionRecord(char *transactionId);

/* declarations for dynamic loading */
PG_FUNCTION_INFO_V1(master_create_worker_shards);
PG_FUNCTION_INFO_V1(perform_2pc);
PG_FUNCTION_INFO_V1(get_own_machine_id);
PG_FUNCTION_INFO_V1(get_worker_list);
PG_FUNCTION_INFO_V1(get_host_name);


static char *
GenerateFunctionCallTemplate(Oid functionId)
{
	Oid namespaceId = InvalidOid;
	StringInfo callTemplate = makeStringInfo();
	FmgrInfo fmgrInfo;
	char *functionName = NULL;
	int argCount = 0;
	char *namespaceName = NULL;

	fmgr_info(functionId, &fmgrInfo);
	functionName = get_func_name(functionId);
	namespaceId = get_func_namespace(functionId);
	namespaceName = get_namespace_name(namespaceId);
	argCount = get_func_nargs(functionId);


	appendStringInfo(callTemplate, "SELECT %s.%s(", quote_identifier(namespaceName),
					 quote_identifier(functionName));

	for (int argNum = 0; argNum < argCount; argNum++)
	{
		if (argNum != 0)
		{
			appendStringInfoChar(callTemplate, ',');
		}

		appendStringInfo(callTemplate, "$%d", argNum + 1);
	}
	appendStringInfoChar(callTemplate, ')');

	return callTemplate->data;
}


Datum
perform_2pc(PG_FUNCTION_ARGS)
{
	Oid remoteFunctionId = PG_GETARG_OID(0);
	ArrayType *remoteFunctionArgs = PG_GETARG_ARRAYTYPE_P(1);
	TransactionStatus transactionStatus = XACT_STATUS_COMMIT;
	char *xid = LogGlobalTransactionStart();
	List *workerNodeList = NIL;
	char *functionCallTemplate = GenerateFunctionCallTemplate(remoteFunctionId);
	char **argValues = NULL;
	Oid *argTypes = NULL;
	ListCell *workerNodeCell = NULL;
	int argCount = DecodeFunctionArgValuesAndTypes(remoteFunctionId, remoteFunctionArgs,
												   &argValues, &argTypes);

	workerNodeList = ParseWorkerNodeFile(WORKER_LIST_FILENAME);
	workerNodeList = SortList(workerNodeList, CompareWorkerNodes);

	foreach(workerNodeCell, workerNodeList)
	{
		WorkerNode *workerNode = (WorkerNode *) lfirst(workerNodeCell);
		bool transactionPrepared = true;

		transactionPrepared = PrepareRemoteTransaction(workerNode->workerName,
													   workerNode->workerPort, xid,
													   functionCallTemplate, argValues,
													   argTypes, argCount);

		if (!transactionPrepared)
		{
			transactionStatus = XACT_STATUS_ABORT;
			break;
		}
	}

	UpdateGlobalTransactionStatus(xid, transactionStatus);

	FinishGlobalTransaction(xid);

	PG_RETURN_BOOL(transactionStatus == XACT_STATUS_COMMIT);
}


static int
DecodeFunctionArgValuesAndTypes(Oid functionId, ArrayType *argArray,
								char ***argValuesOut, Oid **argTypesOut)
{
	Datum *argDatums = NULL;
	char **argStrings = NULL;
	int argCount = -1;
	int paramCount = -1;

	Assert(ARR_NDIM(argArray) == 0 || ARR_NDIM(argArray) == 1);
	Assert(!ARR_HASNULL(argArray));
	Assert(ARR_ELEMTYPE(argArray) == TEXTOID);

	deconstruct_array(argArray, TEXTOID, -1, false, 'i', &argDatums, NULL, &argCount);

	argStrings = palloc(argCount * sizeof(char *));
	for (int argNum = 0; argNum < argCount; argNum++)
	{
		argStrings[argNum] = TextDatumGetCString(argDatums[argNum]);
	}

	*argValuesOut = argStrings;

	get_func_signature(functionId, argTypesOut, &paramCount);

	Assert(argCount == paramCount);

	return argCount;
}


Datum
get_own_machine_id(PG_FUNCTION_ARGS)
{
	List *workerNodeList = NIL;
	void *callbackData[2];
	int32 machineId = -1;

	workerNodeList = ParseWorkerNodeFile(WORKER_LIST_FILENAME);
	workerNodeList = SortList(workerNodeList, CompareWorkerNodes);

	callbackData[0] = (void *) workerNodeList;
	callbackData[1] = &machineId;

	pg_foreach_ifaddr(CheckHostnameCallback, (void *) callbackData);

	PG_RETURN_INT32(machineId);
}


static void
CheckHostnameCallback(struct sockaddr *address, struct sockaddr *netmask,
					  void *callbackData)
{
	void **callbackArray = (void **) callbackData;
	List *workerNodeList = (List *) callbackArray[0];
	int32 *machineId = (int *) callbackArray[1];
	ListCell *workerNodeCell = NULL;
	int32 currentId = 0;

	char hostname[NI_MAXHOST];
	int status = -1;

	if (*machineId != -1)
	{
		return;
	}

#ifdef HAVE_STRUCT_SOCKADDR_SA_LEN

	status = pg_getnameinfo_all((const struct sockaddr_storage *) address,
								address->sa_len, hostname, sizeof(hostname), NULL, 0,
								NI_NAMEREQD);
#else
	status = pg_getnameinfo_all((const struct sockaddr_storage *) address,
								0, hostname, sizeof(hostname), NULL, 0,
								NI_NAMEREQD);
#endif


	if (status != 0)
	{
		return;
	}

	foreach(workerNodeCell, workerNodeList)
	{
		WorkerNode *workerNode = (WorkerNode *) lfirst(workerNodeCell);

		bool hostMatch = pg_strncasecmp(hostname, workerNode->workerName, NI_MAXHOST) == 0;
		bool portMatch = PostPortNumber == (int) workerNode->workerPort;

		if (hostMatch && portMatch)
		{
			*machineId = currentId;
			return;
		}

		currentId++;
	}
}


static bool
PrepareRemoteTransaction(char *nodeName, uint32 nodePort, char *transactionId,
						 char *functionCallTemplate, char **argValues, Oid *argTypes,
						 int argCount)
{
	bool remoteTransactionPrepared = true;
	bool beginIssued = false;
	PGresult *result = NULL;

	PGconn *connection = GetOrEstablishConnection(nodeName, nodePort);
	if (connection == NULL)
	{
		return false;
	}

	/* begin a transaction before we start executing commands */
	beginIssued = ExecuteRemoteCommandPgShard(connection, BEGIN_COMMAND);
	if (!beginIssued)
	{
		return false;
	}

	result = PQexecParams(connection, functionCallTemplate, argCount, argTypes,
						  (const char *const *) argValues, NULL, NULL, 0);
	if (PQresultStatus(result) == PGRES_COMMAND_OK ||
		PQresultStatus(result) == PGRES_TUPLES_OK)
	{
		StringInfo prepareCommand = makeStringInfo();
		bool prepareIssued = true;

		appendStringInfo(prepareCommand, "PREPARE TRANSACTION '%s';", transactionId);

		prepareIssued = ExecuteRemoteCommandPgShard(connection, prepareCommand->data);
		if (!prepareIssued)
		{
			remoteTransactionPrepared = false;
		}
	}
	else
	{
		ReportRemoteError(connection, result);

		ExecuteRemoteCommandPgShard(connection, ROLLBACK_COMMAND);
		remoteTransactionPrepared = false;
	}

	PQclear(result);
	return remoteTransactionPrepared;
}


static char *
LogGlobalTransactionStart()
{
	char *transactionId = NULL;
	PGconn *loopback = GetOrEstablishConnection("localhost", PostPortNumber);
	PGresult *result = NULL;

	if (loopback == NULL)
	{
		return NULL;
	}

	result = PQexec(loopback, "INSERT INTO citus."
							  "global_transactions DEFAULT VALUES RETURNING id");

	if (PQresultStatus(result) != PGRES_COMMAND_OK &&
		PQresultStatus(result) != PGRES_TUPLES_OK)
	{
		ReportRemoteError(loopback, result);
	}
	else
	{
		Assert(PQntuples(result) == 1);
		Assert(PQnfields(result) == 1);
		Assert(PQfformat(result, 0) == 0);
		Assert(PQgetisnull(result, 0, 0) == false);

		transactionId = PQgetvalue(result, 0, 0);
		transactionId = pstrdup(transactionId);
	}

	PQclear(result);
	return transactionId;
}


static bool
UpdateGlobalTransactionStatus(char *transactionId, TransactionStatus finalStatus)
{
	bool updateSuccessful = false;
	PGconn *loopback = GetOrEstablishConnection("localhost", PostPortNumber);
	PGresult *result = NULL;
	char *statusString = (finalStatus == XACT_STATUS_ABORT) ? "ABORT" : "COMMIT";

	Oid paramTypes[] = { InvalidOid, TEXTOID };
	char *paramValues[] = { statusString, transactionId };
	const int paramCount = sizeof(paramValues) / sizeof(paramValues[0]);
	Oid metadataSchemaOid = get_namespace_oid("pg_catalog", false);
	Oid transactionStatusOid = GetSysCacheOid2(TYPENAMENSP,
											   PointerGetDatum("xact_status"),
											   ObjectIdGetDatum(metadataSchemaOid));
	paramTypes[0] = transactionStatusOid;

	if (loopback == NULL)
	{
		return false;
	}

	Assert(finalStatus == XACT_STATUS_ABORT || finalStatus == XACT_STATUS_COMMIT);

	result = PQexecParams(loopback, "UPDATE citus."
									"global_transactions SET status = $1 WHERE id = $2",
						  paramCount, paramTypes, (const char *const *) paramValues,
						  NULL, NULL, 0);

	if (PQresultStatus(result) != PGRES_COMMAND_OK &&
		PQresultStatus(result) != PGRES_TUPLES_OK)
	{
		ReportRemoteError(loopback, result);
	}
	else
	{
		char *affectedTupleString = PQcmdTuples(result);
		int32 affectedTupleCount = pg_atoi(affectedTupleString, sizeof(int32), 0);

		updateSuccessful = (affectedTupleCount == 1);
	}

	PQclear(result);
	return updateSuccessful;
}


static TransactionStatus
FinishGlobalTransaction(char *transactionId)
{
	TransactionStatus transactionStatus = GetGlobalTransactionStatus(transactionId);
	StringInfo transactionCommand = makeStringInfo();
	List *workerNodeList = NIL;
	ListCell *workerNodeCell = NULL;
	appendStringInfo(transactionCommand, ((transactionStatus == XACT_STATUS_ABORT) ?
										  "ROLLBACK PREPARED '%s'" :
										  "COMMIT PREPARED '%s'"), transactionId);

	workerNodeList = ParseWorkerNodeFile(WORKER_LIST_FILENAME);
	workerNodeList = SortList(workerNodeList, CompareWorkerNodes);

	foreach(workerNodeCell, workerNodeList)
	{
		WorkerNode *workerNode = (WorkerNode *) lfirst(workerNodeCell);
		PGresult *result = NULL;

		PGconn *connection = GetOrEstablishConnection(workerNode->workerName, workerNode->workerPort);
		if (connection == NULL)
		{
			ereport(ERROR, (errmsg("could not connect to host")));
		}

		result = PQexec(connection, transactionCommand->data);
		if (PQresultStatus(result) != PGRES_COMMAND_OK)
		{
			char *sqlStateString = PQresultErrorField(result, PG_DIAG_SQLSTATE);
			int sqlState = ERRCODE_CONNECTION_FAILURE;

			if (sqlStateString != NULL)
			{
				sqlState = MAKE_SQLSTATE(sqlStateString[0], sqlStateString[1],
										 sqlStateString[2], sqlStateString[3],
										 sqlStateString[4]);
			}

			if (sqlState != ERRCODE_UNDEFINED_OBJECT)
			{
				ReportRemoteError(connection, result);
				ereport(ERROR, (errmsg("unexpected error")));
			}
		}

		PQclear(result);
	}

	DeleteGlobalTransactionRecord(transactionId);

	return transactionStatus;
}


static TransactionStatus
GetGlobalTransactionStatus(char *transactionId)
{
	TransactionStatus transactionStatus = XACT_STATUS_INVALID_FIRST;
	Oid argTypes[] = { TEXTOID };
	Datum argValues[] = { CStringGetTextDatum(transactionId) };
	const int argCount = sizeof(argValues) / sizeof(argValues[0]);
	int spiStatus PG_USED_FOR_ASSERTS_ONLY = 0;
	bool isNull = false;
	Datum statusDatum = 0;
	char *statusString = NULL;

	SPI_connect();

	spiStatus = SPI_execute_with_args("SELECT status::text FROM "
									  "citus.global_transactions "
									  "WHERE id = $1", argCount, argTypes,
									  argValues, NULL, false, 1);
	Assert(spiStatus == SPI_OK_SELECT);

	if (SPI_processed != 1)
	{
		ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
						errmsg("no transaction in progress with id \"%s\"",
							   transactionId)));
	}

	statusDatum = SPI_getbinval(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1, &isNull);
	statusString = text_to_cstring(DatumGetTextP(statusDatum));

	if (strncmp(statusString, "ABORT", NAMEDATALEN) == 0)
	{
		transactionStatus = XACT_STATUS_ABORT;
	}
	else if (strncmp(statusString, "COMMIT", NAMEDATALEN) == 0)
	{
		transactionStatus = XACT_STATUS_COMMIT;
	}

	Assert(transactionStatus != XACT_STATUS_INVALID_FIRST);

	SPI_finish();

	return transactionStatus;
}


static void
DeleteGlobalTransactionRecord(char *transactionId)
{
	Oid argTypes[] = { TEXTOID };
	Datum argValues[] = { CStringGetTextDatum(transactionId) };
	const int argCount = sizeof(argValues) / sizeof(argValues[0]);
	int spiStatus PG_USED_FOR_ASSERTS_ONLY = 0;

	SPI_connect();

	spiStatus = SPI_execute_with_args("DELETE FROM "
									  "citus.global_transactions "
									  "WHERE id = $1", argCount, argTypes, argValues,
									  NULL, false, 0);
	Assert(spiStatus == SPI_OK_DELETE);

	if (SPI_processed != 1)
	{
		ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
						errmsg("global transaction with id %s does not exist",
							   transactionId)));
	}

	SPI_finish();
}



/*
 * master_create_worker_shards creates empty shards for the given table based
 * on the specified number of initial shards. The function first gets a list of
 * candidate nodes and issues DDL commands on the nodes to create empty shard
 * placements on those nodes. The function then updates metadata on the master
 * node to make this shard (and its placements) visible. Note that the function
 * assumes the table is hash partitioned and calculates the min/max hash token
 * ranges for each shard, giving them an equal split of the hash space.
 */
Datum
master_create_worker_shards(PG_FUNCTION_ARGS)
{
	text *tableNameText = PG_GETARG_TEXT_P(0);
	int32 shardCount = PG_GETARG_INT32(1);
	int32 replicationFactor = PG_GETARG_INT32(2);

	Oid distributedTableId = ResolveRelationId(tableNameText);
	char relationKind = get_rel_relkind(distributedTableId);
	char *tableName = text_to_cstring(tableNameText);
	char shardStorageType = '\0';
	List *workerNodeList = NIL;
	List *ddlCommandList = NIL;
	int32 workerNodeCount = 0;
	uint32 placementAttemptCount = 0;
	uint64 hashTokenIncrement = 0;
	List *existingShardList = NIL;
	int64 shardIndex = 0;

	/* make sure table is hash partitioned */
	CheckHashPartitionedTable(distributedTableId);

	/* we plan to add shards: get an exclusive metadata lock */
	LockRelationDistributionMetadata(distributedTableId, ExclusiveLock);

	/* validate that shards haven't already been created for this table */
	existingShardList = LoadShardList(distributedTableId);
	if (existingShardList != NIL)
	{
		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("table \"%s\" has already had shards created for it",
							   tableName)));
	}

	/* make sure that at least one shard is specified */
	if (shardCount <= 0)
	{
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("shard_count must be positive")));
	}

	/* make sure that at least one replica is specified */
	if (replicationFactor <= 0)
	{
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("replication_factor must be positive")));
	}

	/* calculate the split of the hash space */
	hashTokenIncrement = HASH_TOKEN_COUNT / shardCount;

	/* load and sort the worker node list for deterministic placement */
	workerNodeList = WorkerNodeList();
	workerNodeList = SortList(workerNodeList, CompareWorkerNodes);

	/* make sure we don't process cancel signals until all shards are created */
	HOLD_INTERRUPTS();

	/* retrieve the DDL commands for the table */
	ddlCommandList = GetTableDDLEvents(distributedTableId);

	workerNodeCount = list_length(workerNodeList);
	if (replicationFactor > workerNodeCount)
	{
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("replication_factor (%d) exceeds number of worker nodes "
							   "(%d)", replicationFactor, workerNodeCount),
						errhint("Add more worker nodes or try again with a lower "
								"replication factor.")));
	}

	/* if we have enough nodes, add an extra placement attempt for backup */
	placementAttemptCount = (uint32) replicationFactor;
	if (workerNodeCount > replicationFactor)
	{
		placementAttemptCount++;
	}

	/* set shard storage type according to relation type */
	if (relationKind == RELKIND_FOREIGN_TABLE)
	{
		shardStorageType = SHARD_STORAGE_FOREIGN;
	}
	else
	{
		shardStorageType = SHARD_STORAGE_TABLE;
	}

	for (shardIndex = 0; shardIndex < shardCount; shardIndex++)
	{
		uint32 roundRobinNodeIndex = shardIndex % workerNodeCount;

		/* initialize the hash token space for this shard */
		text *minHashTokenText = NULL;
		text *maxHashTokenText = NULL;
		int32 shardMinHashToken = INT32_MIN + (shardIndex * hashTokenIncrement);
		int32 shardMaxHashToken = shardMinHashToken + (hashTokenIncrement - 1);
		Datum shardIdDatum = master_get_new_shardid(NULL);
		int64 shardId = DatumGetInt64(shardIdDatum);

		/* if we are at the last shard, make sure the max token value is INT_MAX */
		if (shardIndex == (shardCount - 1))
		{
			shardMaxHashToken = INT32_MAX;
		}

		/* insert the shard metadata row along with its min/max values */
		minHashTokenText = IntegerToText(shardMinHashToken);
		maxHashTokenText = IntegerToText(shardMaxHashToken);

		/*
		 * Grabbing the shard metadata lock isn't technically necessary since
		 * we already hold an exclusive lock on the partition table, but we'll
		 * acquire it for the sake of completeness. As we're adding new active
		 * placements, the mode must be exclusive.
		 */
		LockShardDistributionMetadata(shardId, ExclusiveLock);

		CreateShardPlacements(shardId, ddlCommandList, workerNodeList,
							  roundRobinNodeIndex, replicationFactor);

		InsertShardRow(distributedTableId, shardId, shardStorageType,
					   minHashTokenText, maxHashTokenText);
	}

	if (QueryCancelPending)
	{
		ereport(WARNING, (errmsg("cancel requests are ignored during shard creation")));
		QueryCancelPending = false;
	}

	RESUME_INTERRUPTS();

	PG_RETURN_VOID();
}


/*
 * CheckHashPartitionedTable looks up the partition information for the given
 * tableId and checks if the table is hash partitioned. If not, the function
 * throws an error.
 */
static void
CheckHashPartitionedTable(Oid distributedTableId)
{
	char partitionType = PartitionMethod(distributedTableId);
	if (partitionType != DISTRIBUTE_BY_HASH)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("unsupported table partition type: %c", partitionType)));
	}
}


/* Helper function to convert an integer value to a text type */
static text *
IntegerToText(int32 value)
{
	text *valueText = NULL;
	StringInfo valueString = makeStringInfo();
	appendStringInfo(valueString, "%d", value);

	valueText = cstring_to_text(valueString->data);

	return valueText;
}


/*
 * get_worker_list introduces a format to represent pg_worker_list config file.
 * The function returns the file as text in the following format:
 * hostname_1:port_1|hostname_2:port_2|...|hostname_n:port_n
 * */
Datum
get_worker_list(PG_FUNCTION_ARGS)
{
	List *workerNodeList = NIL;
	ListCell *workerNodeListCell = NULL;
	StringInfo workerListText = makeStringInfo();
	int workerListCont = 0;

	workerNodeList = ParseWorkerNodeFile(WORKER_LIST_FILENAME);
	workerListCont = list_length(workerNodeList);

	foreach(workerNodeListCell, workerNodeList)
	{
		WorkerNode *workerNode = (WorkerNode *) lfirst(workerNodeListCell);
		StringInfo workerNodeRepresentation = makeStringInfo();

		appendStringInfoString(workerNodeRepresentation, workerNode->workerName);
		appendStringInfoString(workerNodeRepresentation, ":");
		appendStringInfoString(workerNodeRepresentation, text_to_cstring(IntegerToText(
																			 workerNode->
																			 workerPort)));

		appendStringInfoString(workerListText, workerNodeRepresentation->data);

		if (list_nth(workerNodeList, workerListCont - 1) != workerNode)
		{
			appendStringInfoString(workerListText, "|");
		}
	}

	PG_RETURN_TEXT_P(cstring_to_text(workerListText->data));
}


/*
 * get_host_name function returns the host name provided by gethostname()
 * on the node the function is executed.
 */
Datum
get_host_name(PG_FUNCTION_ARGS)
{
	text *hostNameText = NULL;
	size_t hostNameMaxLength = 200;
	int getHostNameReturnValue = 0;
	char hostNameChar[hostNameMaxLength];

	getHostNameReturnValue = gethostname(hostNameChar, hostNameMaxLength);

	if (getHostNameReturnValue != 0)
	{
		ereport(ERROR, (errcode(ERRCODE_CONFIG_FILE_ERROR), /* not a good error code */
						errmsg("get_host_name failed")));

	}

	hostNameText = cstring_to_text(hostNameChar);

	PG_RETURN_TEXT_P(hostNameText);
}


/*
 * ExecuteRemoteCommand executes the given sql command on the remote node, and
 * returns true if the command executed successfully. The command is allowed to
 * return tuples, but they are not inspected: this function simply reflects
 * whether the command succeeded or failed.
 */
static bool
ExecuteRemoteCommandPgShard(PGconn *connection, const char *sqlCommand)
{
	PGresult *result = PQexec(connection, sqlCommand);
	bool commandSuccessful = true;

	if (PQresultStatus(result) != PGRES_COMMAND_OK &&
		PQresultStatus(result) != PGRES_TUPLES_OK)
	{
		ReportRemoteError(connection, result);
		commandSuccessful = false;
	}

	PQclear(result);
	return commandSuccessful;
}
