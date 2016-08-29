/* Copyright (c) 2005 - 2015 Hewlett Packard Enterprise Development LP  -*- Java -*-*/
/*
 * Description: Example User Defined Analytic Function nth_value to output the value of the nth row.
 *
 * Create Date: Jan 7th, 2015
 */

package com.vertica.JavaLibs;

import java.util.ArrayList;

import com.vertica.sdk.AnalyticFunction;
import com.vertica.sdk.AnalyticPartitionReader;
import com.vertica.sdk.AnalyticPartitionWriter;
import com.vertica.sdk.DestroyInvocation;
import com.vertica.sdk.ParamReader;
import com.vertica.sdk.ServerInterface;
import com.vertica.sdk.SizedColumnTypes;
import com.vertica.sdk.UdfException;
/*
 * The analytic function nth_value(numeric a using parameters n) returns value "a" evaluated on 
 * the n-th row in the input partition defined by the OVER clause.
 * This function generalizes the built-in analytic functions FIRST_VALUE and LAST_VALUE, 
 * which return the first value and last value, respectively, of an input partition.
 */

public class NthValueUsingParam extends AnalyticFunction {

	@Override
	public void processPartition(ServerInterface srvInterface,
			AnalyticPartitionReader inputReader, AnalyticPartitionWriter outputWriter)
			throws UdfException, DestroyInvocation {
		
		// get the value of n from parameters
		ParamReader paramReader = srvInterface.getParamReader();
		long nParam = paramReader.getLong("n");
		SizedColumnTypes inTypes = inputReader.getTypeMetaData();
		ArrayList<Integer> argCols = new ArrayList<Integer>();
		inTypes.getArgumentColumns(argCols);
		
		// One function arguments expected.
		if(argCols.size() != 1 || 
				!inTypes.getColumnType(argCols.get(0)).isNumeric() ||
				nParam < 1){
			throw new UdfException(0, "One argument ( numeric) is expected with " +
					"the parameter n being a positive integer.");
		}
		
		long currentRow = 1;
		boolean nthRowExists = false;
		
		// Find the value of the n-th row
		do{
			if(currentRow == nParam){
				nthRowExists = true;
				break;
			}else{
				currentRow++;
			}
		}while(inputReader.next());
			
		if(nthRowExists){
			do{
				outputWriter.copyFromInput(0, inputReader, argCols.get(0));
			}while(outputWriter.next());
		}else{
			// the partition has less than n rows. output NULL values.
			do{
				outputWriter.setNumericNull(0);
			}while(outputWriter.next());
		}
	}

}
