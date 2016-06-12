/*-------------------------------------------------------------------------
 *
 * worker_transaction.c
 *
 * Routines for performing transactions across all workers.
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

#include "access/xact.h"
#include "distributed/connection_cache.h"
#include "distributed/multi_transaction.h"
#include "distributed/pg_dist_transaction.h"
#include "distributed/transaction_recovery.h"
#include "distributed/worker_manager.h"
#include "distributed/worker_transaction.h"
#include "utils/memutils.h"


/* Local functions forward declarations */
static List * OpenWorkerTransactions(void);
static void EnableXactCallback(void);
static void CompleteWorkerTransactions(XactEvent event, void *arg);


/* Global worker connection list */
static List *workerConnectionList = NIL;
static bool isXactCallbackRegistered = false;


/*
 * OpenWorkerTransactions opens connections to all workers and sends
 * BEGIN commands. Once opened, the remote transaction are committed
 * or aborted when the local transaction commits or aborts. Multiple
 * invocations of OpenWorkerTransactions will return the same list
 * of connections until the commit/abort.
 */
static List *
OpenWorkerTransactions(void)
{
	ListCell *workerNodeCell = NULL;
	List *workerList = NIL;
	List *connectionList = NIL;
	MemoryContext oldContext = NULL;
	int mockGroupId = 0;

	/* TODO: lock worker list */

	workerList = WorkerNodeList();
	oldContext = MemoryContextSwitchTo(TopTransactionContext);

	foreach(workerNodeCell, workerList)
	{
		WorkerNode *workerNode = (WorkerNode *) lfirst(workerNodeCell);
		char *nodeName = workerNode->workerName;
		int nodePort = workerNode->workerPort;
		PGconn *connection = NULL;

		TransactionConnection *transactionConnection = NULL;
		PGresult *result = NULL;

		connection = GetOrEstablishConnection(nodeName, nodePort);
		if (connection == NULL)
		{
			ereport(ERROR, (errmsg("could not open connection to %s:%d",
								   nodeName, nodePort)));
		}

		result = PQexec(connection, "BEGIN");
		if (PQresultStatus(result) != PGRES_COMMAND_OK)
		{
			ReportRemoteError(connection, result);
			PQclear(result);

			ereport(ERROR, (errmsg("could not start transaction on %s:%d",
								   nodeName, nodePort)));
		}

		PQclear(result);

		transactionConnection = palloc0(sizeof(TransactionConnection));

		transactionConnection->connectionId = 0;
		transactionConnection->groupId = mockGroupId++;
		transactionConnection->transactionState = TRANSACTION_STATE_OPEN;
		transactionConnection->connection = connection;

		connectionList = lappend(connectionList, transactionConnection);
	}

	MemoryContextSwitchTo(oldContext);

	return connectionList;
}


/*
 * SendCommandToWorkersInOrder sends a command to all workers in order.
 * Commands are committed on the workers when the local transaction
 * commits.
 */
void
SendCommandToWorkersInOrder(char *command)
{
	ListCell *connectionCell = NULL;

	if (workerConnectionList == NIL)
	{
		workerConnectionList = OpenWorkerTransactions();

		InitializeDistributedTransaction();
		EnableXactCallback();
	}

	foreach(connectionCell, workerConnectionList)
	{
		TransactionConnection *transactionConnection =
			(TransactionConnection *) lfirst(connectionCell);

		PGconn *connection = transactionConnection->connection;

		PGresult *result = PQexec(connection, command);
		if (PQresultStatus(result) != PGRES_COMMAND_OK)
		{
			char *nodeName = ConnectionGetOptionValue(connection, "host");
			char *nodePort = ConnectionGetOptionValue(connection, "port");

			ReportRemoteError(connection, result);

			ereport(ERROR, (errmsg("failed to send metadata change to %s:%s",
								   nodeName, nodePort)));
		}
	}
}


/*
 * SendCommandToWorkersInParallel sends a command to all workers in
 * parallel. Commands are committed on the workers when the local
 * transaction commits.
 */
void
SendCommandToWorkersInParallel(char *command)
{
	ListCell *connectionCell = NULL;

	if (workerConnectionList == NIL)
	{
		workerConnectionList = OpenWorkerTransactions();

		InitializeDistributedTransaction();
		EnableXactCallback();
	}

	foreach(connectionCell, workerConnectionList)
	{
		TransactionConnection *transactionConnection =
			(TransactionConnection *) lfirst(connectionCell);

		PGconn *connection = transactionConnection->connection;

		int querySent = PQsendQuery(connection, command);
		if (querySent == 0)
		{
			char *nodeName = ConnectionGetOptionValue(connection, "host");
			char *nodePort = ConnectionGetOptionValue(connection, "port");

			ReportRemoteError(connection, NULL);

			ereport(ERROR, (errmsg("failed to send metadata change to %s:%s",
								   nodeName, nodePort)));
		}
	}

	foreach(connectionCell, workerConnectionList)
	{
		TransactionConnection *transactionConnection =
			(TransactionConnection *) lfirst(connectionCell);

		PGconn *connection = transactionConnection->connection;

		PGresult *result = PQgetResult(connection);
		if (PQresultStatus(result) != PGRES_COMMAND_OK)
		{
			char *nodeName = ConnectionGetOptionValue(connection, "host");
			char *nodePort = ConnectionGetOptionValue(connection, "port");

			ReportRemoteError(connection, result);
			PQclear(result);

			ereport(ERROR, (errmsg("failed to apply metadata change on %s:%s",
								   nodeName, nodePort)));
		}

		PQclear(result);

		/* clear NULL result */
		PQgetResult(connection);
	}
}


/*
 * EnableXactCallback ensures the XactCallback for comitting/aborting
 * remote worker transactions is registered.
 */
static void
EnableXactCallback(void)
{
	if (!isXactCallbackRegistered)
	{
		RegisterXactCallback(CompleteWorkerTransactions, NULL);
		isXactCallbackRegistered = true;
	}
}


/*
 * CompleteWorkerTransaction commits or aborts pending worker transactions
 * when the local transaction commits or aborts.
 */
static void
CompleteWorkerTransactions(XactEvent event, void *arg)
{
	if (workerConnectionList == NIL)
	{
		/* nothing to do */
		return;
	}
	else if (event == XACT_EVENT_PRE_COMMIT)
	{
		/*
		 * Any failure here will cause local changes to be rolled back,
		 * and may leave a prepared transaction on the remote node.
		 */

		PrepareRemoteTransactions(workerConnectionList);

		/*
		 * We are now ready to commit the local transaction, followed
		 * by the remote transaction. As a final step, write commit
		 * records to a table. If there is a last-minute crash
		 * on the local machine, then the absence of these records
		 * will indicate that the remote transactions should be rolled
		 * back. Otherwise, the presence of these records indicates
		 * that the remote transactions should be committed.
		 */

		LogPreparedTransactions(workerConnectionList);

		return;
	}
	else if (event == XACT_EVENT_COMMIT)
	{
		/*
		 * A failure here may cause some prepared transactions to be
		 * left pending. However, the local change have already been
		 * committed and a commit record exists to indicate that the
		 * remote transaction should be committed as well.
		 */

		CommitRemoteTransactions(workerConnectionList, false);

		/*
		 * At this point, it is safe to remove the transaction records
		 * for all commits that have succeeded. However, we are no
		 * longer in a transaction and therefore cannot make changes
		 * to the metadata.
		 */
	}
	else if (event == XACT_EVENT_ABORT)
	{
		/*
		 * A failure here may cause some prepared transactions to be
		 * left pending. The local changes have already been rolled
		 * back and the absence of a commit record indicates that
		 * the remote transaction should be rolled back as well.
		 */

		AbortRemoteTransactions(workerConnectionList);
	}
	else
	{
		return;
	}

	/*
	 * Memory allocated in workerConnectionList will be reclaimed when
	 * TopTransactionContext is released.
	 */

	workerConnectionList = NIL;
}
