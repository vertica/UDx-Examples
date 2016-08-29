/* Copyright (c) 2005 - 2015 Hewlett Packard Enterprise Development LP  -*- Java -*-*/
/* 
 * Description: Example User Defined Transform Function with parameters: Output top-k rows in each partition
 *
 * Create Date: June 1, 2013
 */

package com.vertica.JavaLibs;

import com.vertica.sdk.*;
import java.util.ArrayList;

// Polymorphic TopK per partition
public class TopKPerPartitionWithParamsFactory extends TransformFunctionFactory
{
    @Override
    public TransformFunction createTransformFunction(ServerInterface srvInterface)
    { return new TopKPerPartitionWithParams(); }

    @Override
    public void getReturnType(ServerInterface srvInterface, SizedColumnTypes inputTypes, SizedColumnTypes outputTypes)
    {
        ArrayList<Integer> argCols = new ArrayList<Integer>();
        inputTypes.getArgumentColumns(argCols);
        int colIdx = 0;
        for (int i = 0; i < argCols.size(); ++i)
        {
            StringBuilder cname = new StringBuilder();
            cname.append("col").append(colIdx++);
            outputTypes.addArg(inputTypes.getColumnType(argCols.get(i)), cname.toString());
        }
    }

    @Override
    public void getPrototype(ServerInterface srvInterface, ColumnTypes argTypes, ColumnTypes returnType)
    {
        argTypes.addInt();
	argTypes.addInt();
        returnType.addInt();
	returnType.addInt();
    }

     @Override
    public void getParameterType(ServerInterface srvInterface,
                                 SizedColumnTypes parameterTypes)
    {
        parameterTypes.addInt("k");
    }
}
