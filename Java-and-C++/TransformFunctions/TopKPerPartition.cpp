/* Copyright (c) 2005 - 2016 Hewlett Packard Enterprise Development LP  -*- C++ -*-*/
/* 
 * Description: Example User Defined Transform Function: Output top-k rows in each partition
 *
 * Create Date: Apr 29, 2011
 */
#include "Vertica.h"
#include <sstream>

using namespace Vertica;

/*
 * Top K rows per partition - This transform function takes rows with 3
 * integers as input, and produces rows with 2 integers as output. The first
 * integer argument is the number of rows per partition to produce - for
 * example, calling this with first argument set to 3 will produce the top 3
 * rows per partition. Note that the Vertica server sorts the input
 * to this function according to the ordering specified in the OVER clause,
 * hence this function can simply output the first 'k' rows that it gets.
 */
class TopKPerPartition : public TransformFunction
{
    virtual void processPartition(ServerInterface &srvInterface, 
                                  PartitionReader &inputReader, 
                                  PartitionWriter &outputWriter)
    {
        try {
            vint cnt=0;
            do {
                vint num = inputReader.getIntRef(0);
                vint a = inputReader.getIntRef(1);
                vint b = inputReader.getIntRef(2);

                // If we're already produced num tuples, then break
                if (cnt >= num)
                    break;

                outputWriter.setInt(0, a);
                outputWriter.setInt(1, b);
                outputWriter.next();
                cnt++;
            } while (inputReader.next());
        } catch(std::exception& e) {
            // Standard exception. Quit.
            vt_report_error(0, "Exception while processing partition: [%s]", e.what());
        }
    }
};


/*
 * This class provides the meta-data associated with the transform function
 * shown above, as well as a way of instantiating objects of the class. The
 * meta-data information includes the prototype for the transform function (so
 * Vertica knows what types to pass as input, and what output types the
 * function produces), and information about the length/precision/scale of the
 * data types returned by the transform function (when applicable).
 */
class TopKFactory : public TransformFunctionFactory
{
    // Provide the function prototype information to the Vertica server (argument types + return types)
    virtual void getPrototype(ServerInterface &srvInterface, ColumnTypes &argTypes, ColumnTypes &returnType)
    {
        argTypes.addInt();
        argTypes.addInt();
        argTypes.addInt();

        returnType.addInt();
        returnType.addInt();
    }

    // Provide return type length/scale/precision information (given the input
    // type length/scale/precision), as well as column names
    virtual void getReturnType(ServerInterface &srvInterface, 
                               const SizedColumnTypes &inputTypes, 
                               SizedColumnTypes &outputTypes)
    {
        for (size_t i=1; i<inputTypes.getColumnCount(); i++)
        {
            std::stringstream cname;
            cname << "col" << i;
            outputTypes.addArg(inputTypes.getColumnType(i), cname.str());
        }
    }

    // Create an instance of the TransformFunction
    virtual TransformFunction *createTransformFunction(ServerInterface &srvInterface)
    { return vt_createFuncObject<TopKPerPartition>(srvInterface.allocator); }

};

RegisterFactory(TopKFactory);
