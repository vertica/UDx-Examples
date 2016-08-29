/* Copyright (c) 2005 - 2015 Hewlett Packard Enterprise Development LP  -*- Java -*-*/
/* 
 * Description: Example User Defined Transform Function: Output top-k rows in each partition
 *
 * Create Date: June 1, 2013
 */

package com.vertica.JavaLibs;

import com.vertica.sdk.*;

// TopK per partition
public class TopKFactory extends TransformFunctionFactory
{
    @Override
    public TransformFunction createTransformFunction(ServerInterface srvInterface)
    { return new TopKPerPartition(); }

    @Override
    public void getReturnType(ServerInterface srvInterface, SizedColumnTypes input_types, SizedColumnTypes output_types)
    {
        for (int i=1; i<input_types.getColumnCount(); i++)
        {
            StringBuilder cname = new StringBuilder();
            cname.append("col").append(i);
            output_types.addArg(input_types.getColumnType(i), cname.toString());
        }
    }

    @Override
    public void getPrototype(ServerInterface srvInterface, ColumnTypes argTypes, ColumnTypes returnType)
    {
        argTypes.addInt();
        argTypes.addInt();
        argTypes.addInt();

        returnType.addInt();
        returnType.addInt();
    }

    public class TopKPerPartition extends TransformFunction
    {
        @Override
        public void processPartition(ServerInterface srvInterface, PartitionReader input_reader, PartitionWriter output_writer)
                    throws UdfException, DestroyInvocation
        {
            long cnt=0;
            do {
                long num = input_reader.getLong(0);
                long a = input_reader.getLong(1);
                long b = input_reader.getLong(2);

                // If we're already produced num tuples, then break
                if (cnt >= num)
                    break;

                output_writer.setLong(0, a);
                output_writer.setLong(1, b);
                output_writer.next();
                cnt++;
            } while (input_reader.next());
        }
    }
}
