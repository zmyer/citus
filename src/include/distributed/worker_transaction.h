/*-------------------------------------------------------------------------
 *
 * worker_transaction.h
 *	  Type and function declarations used in performing transactions across
 *	  workers.
 *
 * Copyright (c) 2016, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef WORKER_TRANSACTION_H
#define WORKER_TRANSACTION_H


/* Functions declarations for worker transactions */
extern void SendCommandToWorkersInOrder(char *command);
extern void SendCommandToWorkersInParallel(char *command);


#endif /* WORKER_TRANSACTION_H */
