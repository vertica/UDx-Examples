/* Copyright (c) 2005 - 2015 Hewlett Packard Enterprise Development LP  -*- Java -*-*/
/* 
 *
 * Description: Example User Defined Scalar Function: Add any number of ints
 *
 * Create Date: June 01, 2013
 */


package com.vertica.JavaLibs;

import com.vertica.sdk.*;
import java.util.ArrayList;
/*
 * This is a simple function that adds any numebr of integers and returns the result
 */
public class AddAnyIntsInfo extends ScalarFunctionFactory
{
    @Override
	public void getPrototype(ServerInterface srvInterface,
                             ColumnTypes argTypes,
                             ColumnTypes returnType)
    {
	argTypes.addAny();
        returnType.addInt();
    }

    public class AddAnyInts extends ScalarFunction
    {
	/*
	 * This method processes a block of rows in a single invocation.
	 *
	 * The inputs are retrieved via argReader
	 * The outputs are returned via resWriter
	 */
        @Override
        public void processBlock(ServerInterface srvInterface,
                                 BlockReader arg_reader,
                                 BlockWriter res_writer)
                    throws UdfException, DestroyInvocation
        {
	    SizedColumnTypes inTypes = arg_reader.getTypeMetaData();
	    ArrayList<Integer> argCols = new ArrayList<Integer>(); // Argument column indexes.
	    inTypes.getArgumentColumns(argCols);
	    // While we have inputs to process	
            do {
		long sum = 0;
		for (int i = 0; i < argCols.size(); ++i){
		    long a = arg_reader.getLong(i);
		    sum += a;
		}
                res_writer.setLong(sum);
                res_writer.next();
            } while (arg_reader.next());
        }
    }

    @Override
	public ScalarFunction createScalarFunction(ServerInterface srvInterface)
    {
        return new AddAnyInts();
    }
}
