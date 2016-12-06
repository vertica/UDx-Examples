/* Copyright (c) 2005 - 2016 Hewlett Packard Enterprise Development LP  -*- C++ -*-*/
/* 
 * Description: Example User Defined Transform Function: Output top-k rows in each partition
 *
 * Create Date: Nov 01, 2014
 */
#include "Vertica.h"
#include <sstream>

using namespace Vertica;

class PolyTopKPerPartition : public TransformFunction
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
            // Sanity check the input we've been given
            const SizedColumnTypes &inTypes = inputReader.getTypeMetaData();
            std::vector<size_t> argCols; // Argument column indexes.
            inTypes.getArgumentColumns(argCols);

            if (argCols.size() < 1)
                vt_report_error(0, "Function takes at least 1 argument, and an integer parameter 'k'");
            do {
              
                // If we're already produced num tuples, then break
                if (cnt >= num)
                    break;
              
                // Write the arguments to output
                size_t owColIdx = 0;
                for (std::vector<size_t>::iterator i = argCols.begin(); i < argCols.end(); i++)
                    outputWriter.copyFromInput(owColIdx++, inputReader, *i);
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
class PolyTopKPerPartitionFactory : public TransformFunctionFactory
{
    // Provide the function prototype information to the Vertica server (argument types + return types)
    virtual void getPrototype(ServerInterface &srvInterface, ColumnTypes &argTypes, ColumnTypes &returnType)
    {
        argTypes.addAny();
        returnType.addAny();
    }

    // Provide return type length/scale/precision information (given the input
    // type length/scale/precision), as well as column names
    virtual void getReturnType(ServerInterface &srvInterface, 
                               const SizedColumnTypes &inputTypes, 
                               SizedColumnTypes &outputTypes)
    {
        std::vector<size_t> argCols; // Argument column indexes.
        inputTypes.getArgumentColumns(argCols);        

        for (std::vector<size_t>::iterator i = argCols.begin(); i < argCols.end(); i++)
        {
            outputTypes.addArg(inputTypes.getColumnType(*i), inputTypes.getColumnName(*i));
        }
    }
    
    virtual void getParameterType(ServerInterface &srvInterface,
                                  SizedColumnTypes &parameterTypes)
    {
        parameterTypes.addInt("k");
    }
    // Create an instance of the TransformFunction
    virtual TransformFunction *createTransformFunction(ServerInterface &srvInterface)
    { return vt_createFuncObject<PolyTopKPerPartition>(srvInterface.allocator); }

};

RegisterFactory(PolyTopKPerPartitionFactory);
