/* Copyright (c) 2005 - 2015 Hewlett Packard Enterprise Development LP  -*- C++ -*-*/
/* 
 * Description: Example User Defined Transform Function: Output top-k rows in each partition
 *
 * Create Date: Nov 01, 2014
 */
#include "Vertica.h"
#include <sstream>

using namespace Vertica;
using namespace std;

class TopKPerPartitionParams : public TransformFunction
{
    vint num;
    //setup the value of num once when the function is initialized
    virtual void setup(ServerInterface &srvInterface, const SizedColumnTypes &argTypes)
    {
        //get the number of records to be produced 
        ParamReader paramReader = srvInterface.getParamReader();            
        num = paramReader.getIntRef("k");           
    }    

    virtual void processPartition(ServerInterface &srvInterface, 
                                  PartitionReader &inputReader, 
                                  PartitionWriter &outputWriter)
    {
        try {
            vint cnt=0;
            do {
                vint a = inputReader.getIntRef(0);
                vint b = inputReader.getIntRef(1);  
                // If we're already produced num tuples, then break
                if (cnt >= num)
                    break;
                outputWriter.setInt(0, a);
                outputWriter.setInt(1, b);
                outputWriter.next();
                cnt++;
            } while (inputReader.next());
        } catch(exception& e) {
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
class TopKParamsFactory : public TransformFunctionFactory
{
    // Provide the function prototype information to the Vertica server (argument types + return types)
    virtual void getPrototype(ServerInterface &srvInterface, ColumnTypes &argTypes, ColumnTypes &returnType)
    {
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
        for (size_t i=0; i<inputTypes.getColumnCount(); i++)
        {
            std::stringstream cname;
            cname << "col" << i;
            outputTypes.addArg(inputTypes.getColumnType(i), cname.str());
        }
    }
    
    virtual void getParameterType(ServerInterface &srvInterface,
                                  SizedColumnTypes &parameterTypes)
    {
        parameterTypes.addInt("k");
    }
    // Create an instance of the TransformFunction
    virtual TransformFunction *createTransformFunction(ServerInterface &srvInterface)
    { return vt_createFuncObject<TopKPerPartitionParams>(srvInterface.allocator); }

};

RegisterFactory(TopKParamsFactory);
