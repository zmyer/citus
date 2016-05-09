/*-------------------------------------------------------------------------
 *
 * multitree_debug_helper.h
 *	 Function declarations for debug printing of multi trees.
 *	 This file will be removed after subquery development is completed.
 *
 * Copyright (c) 2015, Citus Data, Inc.
 *
 * $Id$
 *
 *-------------------------------------------------------------------------
 */

#ifndef MULTITREE_DEBUG_HELPER_H
#define MULTITREE_DEBUG_HELPER_H

#include "lib/stringinfo.h"

#define SUBQUERY_LOG_LEVEL WARNING

/* debug functions */
extern bool PrintMultiPlan;
extern StringInfo PrintMultiTree(MultiNode *multiNode, int print);


#endif   /* MULTITREE_DEBUG_HELPER_H */
