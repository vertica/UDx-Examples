/* Copyright (c) 2005 - 2016 Hewlett Packard Enterprise Development LP  -*- Java -*-*/
/* 
 * Description: Example User Defined Analytic Function: Output nth row
 *
 * Create Date: Jan 7, 2015
 */

package com.vertica.JavaLibs;

import com.vertica.sdk.AnalyticFunction;
import com.vertica.sdk.AnalyticPartitionReader;
import com.vertica.sdk.AnalyticPartitionWriter;
import com.vertica.sdk.DestroyInvocation;
import com.vertica.sdk.ServerInterface;
import com.vertica.sdk.UdfException;

/**
 * User defined analytic function: Rank - works mostly the same as SQL-99 rank
 * with the ability to define as many order by columns as desired
 *
 * One difference is UDAn's do not currently require an order by clause in the DDL
 */
public class Rank extends AnalyticFunction {
	private int rank, numRowsWithSameOrder;

	@Override
	public void processPartition(ServerInterface srvInterface,
			AnalyticPartitionReader inputReader, AnalyticPartitionWriter outputWriter)
			throws UdfException, DestroyInvocation {
		rank = 0;
		numRowsWithSameOrder = 1;
		do{
			if(!inputReader.isNewOrderByKey()){
				++numRowsWithSameOrder;
			}else {
				rank += numRowsWithSameOrder;
				numRowsWithSameOrder = 1;
			}
			outputWriter.setLong(0, rank);
			outputWriter.next();
		}while(inputReader.next());
	}

}
