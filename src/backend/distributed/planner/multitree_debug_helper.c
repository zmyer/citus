/*-------------------------------------------------------------------------
 *
 * multitree_debug_helper.c
 *	 Function definitions for debug printing of multi trees.
 *	 This file will be removed after subquery development is completed.
 *
 * Copyright (c) 2015, Citus Data, Inc.
 *
 * $Id$
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "distributed/multi_logical_planner.h"
#include "distributed/multitree_debug_helper.h"



/* config variable to print multi plans */
bool PrintMultiPlan = false;


/* beginning of debug functions to be removed */

static char* MultiNodeToString(CitusNodeTag nodeTag)
{
	CitusNodeTag tags[] = {
	T_MultiNode,
	T_MultiTreeRoot,
	T_MultiProject,
	T_MultiCollect,
	T_MultiSelect,
	T_MultiTable,
	T_MultiJoin,
	T_MultiPartition,
	T_MultiCartesianProduct,
	T_MultiExtendedOp,
	T_Job,
	T_MapMergeJob,
	T_MultiPlan,
	T_Task,
	T_ShardInterval,
	T_ShardPlacement};

	char *values[] = {
			"MultiNode",
			"MultiTreeRoot",
			"MultiProject",
			"MultiCollect",
			"MultiSelect",
			"MultiTable",
			"MultiJoin",
			"MultiPartition",
			"MultiCartesianProduct",
			"MultiExtendedOp",
			"Job",
			"MapMergeJob",
			"MultiPlan",
			"Task",
			"ShardInterval",
			"ShardPlacement"};
	int count = 16;
	int index = 0;
	char *result = NULL;

	for (index = 0; index < count; index++)
	{
		if (tags[index] == nodeTag)
		{
			result = values[index];
		}
	}

	return result;
}

StringInfo PrintMultiTree(MultiNode *multiNode, int print)
{
	StringInfo outString = makeStringInfo();

	appendStringInfo(outString, "{", NULL);

	if (multiNode != NULL)
	{
		appendStringInfo(outString, " %d [%s] ", (int) CitusNodeTag(multiNode),
									MultiNodeToString(CitusNodeTag(multiNode)));


		if (UnaryOperator(multiNode))
		{
			StringInfo childNodeString = PrintMultiTree(ChildNode((MultiUnaryNode*) multiNode), 0);
			appendStringInfo(outString, "%s", childNodeString->data);
		}

		if (BinaryOperator(multiNode))
		{
			StringInfo leftChildString = PrintMultiTree(((MultiBinaryNode *) multiNode)->leftChildNode, 0);
			StringInfo rightChildString = PrintMultiTree(((MultiBinaryNode *) multiNode)->rightChildNode, 0);

			appendStringInfo(outString, "%s , %s", leftChildString->data, rightChildString->data);
		}
	}

	appendStringInfo(outString, "}", NULL);

	if (print == 1)
	{
		ereport(SUBQUERY_LOG_LEVEL, (errmsg("%s", outString->data)));
	}
	return outString;
}
