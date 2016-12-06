/* Copyright (c) 2005 - 2016 Hewlett Packard Enterprise Development LP  -*- Java -*-*/
/* 
 * Description: Example User Defined Transform Function: Output top-k rows in each partition
 *
 * Create Date: June 1, 2013
 */

package com.vertica.JavaLibs;

import com.vertica.sdk.*;
import java.util.ArrayList;

// Polymorphic TopK per partition
public class PolyTopKFactory extends TransformFunctionFactory
{
    @Override
    public TransformFunction createTransformFunction(ServerInterface srvInterface)
    { return new PolyTopKPerPartition(); }

    @Override
    public void getReturnType(ServerInterface srvInterface, SizedColumnTypes inputTypes, SizedColumnTypes outputTypes)
    {
        ArrayList<Integer> argCols = new ArrayList<Integer>();
        inputTypes.getArgumentColumns(argCols);
        int colIdx = 0;
        for (int i = 1; i < argCols.size(); ++i)
        {
            StringBuilder cname = new StringBuilder();
            cname.append("col").append(colIdx++);
            outputTypes.addArg(inputTypes.getColumnType(argCols.get(i)), cname.toString());
        }
    }

    @Override
    public void getPrototype(ServerInterface srvInterface, ColumnTypes argTypes, ColumnTypes returnType)
    {
        argTypes.addAny();
        returnType.addAny();
    }
}
