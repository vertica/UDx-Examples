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
 * The analytic function nth_value(numeric a, int n) returns value "a" evaluated on 
 * the n-th row in the input partition defined by the OVER clause.
 * This function generalizes the built-in analytic functions FIRST_VALUE and LAST_VALUE, 
 * which return the first value and last value, respectively, of an input partition.
 */

public class NthValue extends AnalyticFunction {

	@Override
	public void processPartition(ServerInterface srvInterface,
			AnalyticPartitionReader reader, AnalyticPartitionWriter writer)
			throws UdfException, DestroyInvocation {
		
		SizedColumnTypes inputTypes = reader.getTypeMetaData();
		ArrayList<Integer> argCols = new ArrayList<Integer>();
		inputTypes.getArgumentColumns(argCols);
		long nParam = reader.getLong(argCols.get(1));
		
		// Two function arguments expected.
		if(argCols.size() != 2 ||
				!inputTypes.getColumnType(argCols.get(0)).isNumeric() ||
				!inputTypes.getColumnType(argCols.get(1)).isInt() ||
				nParam < 1){
			throw new UdfException(0, "Two arguments (numeric, int) are expected with the second argument being a positive integer.");
		}
		long currentRow = 1;
		boolean nthRowExists = false;
		
		// Find the value of n-th row
		do{
			if(currentRow == nParam){
				nthRowExists = true;
				break;
			}else{
				currentRow++;
			}
		}while(reader.next());
		
		// Output the n-th value.
		if(nthRowExists){
			do{
				writer.copyFromInput(0, reader, argCols.get(0));
			}while(writer.next());
		}else{
			do{
				writer.setNumericNull(0);
			}while(writer.next());
		}
	}

}
