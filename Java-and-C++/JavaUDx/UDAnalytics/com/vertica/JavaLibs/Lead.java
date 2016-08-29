/* Copyright (c) 2005 - 2015 Hewlett Packard Enterprise Development LP  -*- Java -*- */
/* 
 * Description: Example User Defined Analytic Function Lead.
 *
 * Create Date: Jan 7, 2015
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
 * Lead takes in an integer expression and an offset value which determines how far to
 * move after the current row, and returns the expression evaluated on that row.
 * This function mimics the built-in analytic function LEAD, but
 * it is implemented only for integer input expressions.
 */

public class Lead extends AnalyticFunction{

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
		boolean moreRowsToWrite = true;
		
		do{
			if(currentOffset == lagOffset){
				value = inputReader.getLong(argCols.get(0)); // get ahead value.
				outputWriter.setLong(0, value);
				if(!outputWriter.next()){ 
					moreRowsToWrite = false; 
					break;
					}
			}else {
				++currentOffset;
			}
		}while(inputReader.next());
		
		if(!moreRowsToWrite) return;
		do{
			outputWriter.setLongNull(0);
		}while(outputWriter.next());
	}
		
	}

