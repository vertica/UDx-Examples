/* Copyright (c) 2005 - 2015 Hewlett Packard Enterprise Development LP  -*- Java -*-*/
/* 
 * Description: Example Polymorphic User Defined Transform Function: Output top-k rows in each partition
 *
 * Create Date: June 1, 2013
 */
package com.vertica.JavaLibs;

import com.vertica.sdk.*;
import java.util.ArrayList;

public class PolyTopKPerPartition extends TransformFunction
{
    @Override
    public void processPartition(ServerInterface srvInterface, PartitionReader inputReader, PartitionWriter outputWriter)
                throws UdfException, DestroyInvocation
    {
        // Sanity check the input we've been given
        SizedColumnTypes inTypes = inputReader.getTypeMetaData();
        ArrayList<Integer> argCols = new ArrayList<Integer>(); // Argument column indexes.
        inTypes.getArgumentColumns(argCols);

        if (argCols.size() < 2)
            throw new UdfException(0, "Function takes at least 2 arguments, the first of which must be 'k'");

        VerticaType t = inTypes.getColumnType(argCols.get(0)); // first argument
        if (!t.isInt())
            throw new UdfException(0, "First argument must be an integer (the 'k' value)");

        long cnt=0;
        do {
            long num = inputReader.getLong(argCols.get(0));

            // If we're already produced num tuples, then break
            if (cnt >= num)
                break;

            // Write the remaining arguments to output
            int owColIdx = 0;
            for (int i = 1; i < argCols.size(); ++i)
                outputWriter.copyFromInput(owColIdx++, inputReader, argCols.get(i));

            outputWriter.next();
            cnt++;
        } while (inputReader.next());
    }
}
