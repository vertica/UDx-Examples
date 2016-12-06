/* Copyright (c) 2005 - 2016 Hewlett Packard Enterprise Development LP  -*- Java -*-*/
/* 
 *
 * Description: Example User Defined Scalar Function: Add 2 ints
 *
 * Create Date: June 1, 2013
 */


package com.vertica.JavaLibs;

import com.vertica.sdk.*;

public class Add2intsInfo extends ScalarFunctionFactory
{
    @Override
	public void getPrototype(ServerInterface srvInterface,
                             ColumnTypes argTypes,
                             ColumnTypes returnType)
    {
        argTypes.addInt();
        argTypes.addInt();
        returnType.addInt();
    }

    public class Add2ints extends ScalarFunction
    {
        @Override
        public void processBlock(ServerInterface srvInterface,
                                 BlockReader arg_reader,
                                 BlockWriter res_writer)
                    throws UdfException, DestroyInvocation
        {
            do {
                long a = arg_reader.getLong(0);
                long b = arg_reader.getLong(1);        
                res_writer.setLong(a+b);
		res_writer.next();
            } while (arg_reader.next());
        }
    }

    @Override
	public ScalarFunction createScalarFunction(ServerInterface srvInterface)
    {
        return new Add2ints();
    }
}
