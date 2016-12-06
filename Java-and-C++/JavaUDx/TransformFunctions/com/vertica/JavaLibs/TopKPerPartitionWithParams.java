/* Copyright (c) 2005 - 2016 Hewlett Packard Enterprise Development LP  -*- Java -*-*/
/* 
 * Description: Example User Defined Transform Function with parameters: Output top-k rows in each partition
 *
 * Create Date: June 1, 2013
 */

package com.vertica.JavaLibs;

import com.vertica.sdk.*;
import java.util.ArrayList;

public class TopKPerPartitionWithParams extends TransformFunction
{
   
    @Override
    public void processPartition(ServerInterface srvInterface, PartitionReader inputReader, PartitionWriter outputWriter)
                throws UdfException, DestroyInvocation
    {
        // Sanity check the input we've been given
        SizedColumnTypes inTypes = inputReader.getTypeMetaData();
        ArrayList<Integer> argCols = new ArrayList<Integer>(); // Argument column indexes.
        inTypes.getArgumentColumns(argCols);
	ParamReader paramReader = srvInterface.getParamReader();
	long num = paramReader.getLong("k");      
        long cnt=0;
        do {
           
            // If we're already produced num tuples, then break
            if (cnt >= num)
                break;

            // Write the remaining arguments to output
            int owColIdx = 0;
            for (int i = 0; i < argCols.size(); ++i)
                outputWriter.copyFromInput(owColIdx++, inputReader, argCols.get(i));

            outputWriter.next();
            cnt++;
        } while (inputReader.next());
    }
}
