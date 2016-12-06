/* Copyright (c) 2005 - 2016 Hewlett Packard Enterprise Development LP  -*- JAVA -*-*/
/* 
 * Description: Example User Defined Analytic Function Lag.
 *
 * Create Date: Jan 7 2015
 */
package com.vertica.JavaLibs;

import java.util.ArrayList;

import com.vertica.sdk.AnalyticFunction;
import com.vertica.sdk.AnalyticPartitionReader;
import com.vertica.sdk.AnalyticPartitionWriter;
import com.vertica.sdk.DestroyInvocation;
import com.vertica.sdk.ServerInterface;
import com.vertica.sdk.SizedColumnTypes;
import com.vertica.sdk.UdfException;

/*
 * Lag takes in an input expression and an offset value which determines how far to
 * lag behind the current row, and returns the expression evaluated on that row.
 * This function mimics the built-in analytic function LAG, but
 * it is implemented only for integer input expressions.
 */

public class Lag extends AnalyticFunction {

	@Override
	public void processPartition(ServerInterface srvInterface,
			AnalyticPartitionReader inputReader, AnalyticPartitionWriter outputWriter)
			throws UdfException, DestroyInvocation {
		
		SizedColumnTypes inTypes = inputReader.getTypeMetaData();
		ArrayList<Integer> argCols = new ArrayList<Integer>();
		
		inTypes.getArgumentColumns(argCols);
		
		long  lagOffset = inputReader.getLong(argCols.get(1)); // the offset
		long currentOffset = 0;
		long value;
		
		do{
			if(currentOffset == lagOffset){
				value = inputReader.getLong(argCols.get(0)); // get lagged value.
				outputWriter.setLong(0, value);
				inputReader.next();
			}else {
				++currentOffset;
				outputWriter.setLongNull(0);
			}
		}while(outputWriter.next());
	}

}
