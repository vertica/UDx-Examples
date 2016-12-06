/* Copyright (c) 2005 - 2016 Hewlett Packard Enterprise Development LP  -*- C++ -*-*/

/* 
 *
 * Description: Example User Defined Scalar Function: Add any number of ints
 *
 * Create Date: Nov 01, 2014
 */
#include "Vertica.h"
#include <exception>

using namespace Vertica;

/*
 * This is a simple function that adds any numebr of integers and returns the result
 */
class AddAnyInts : public ScalarFunction
{
public:

    /*
     * This method processes a block of rows in a single invocation.
     *
     * The inputs are retrieved via argReader
     * The outputs are returned via resWriter
     */
    virtual void processBlock(ServerInterface &srvInterface,
                              BlockReader &argReader,
                              BlockWriter &resWriter)
    {
        try {
            
            const SizedColumnTypes &inTypes = argReader.getTypeMetaData();
            std::vector<size_t> argCols; // Argument column indexes.
            inTypes.getArgumentColumns(argCols);

            // While we have inputs to process
            do {
                vint sum = 0;
                for (uint i = 0; i < argCols.size(); i++){
                    vint num = argReader.getIntRef(i);
                    sum += num;
                }
                resWriter.setInt(sum);
                resWriter.next();
            } while (argReader.next());
        } catch(std::exception& e) {
            // Standard exception. Quit.
            vt_report_error(0, "Exception while processing block: [%s]", e.what());
        }
    }
};

class AddAnyIntsFactory : public ScalarFunctionFactory
{
    // return an instance of AddAnyInts to perform the actual addition.
    virtual ScalarFunction *createScalarFunction(ServerInterface &interface)
    { return vt_createFuncObject<AddAnyInts>(interface.allocator); }

    virtual void getPrototype(ServerInterface &interface,
                              ColumnTypes &argTypes,
                              ColumnTypes &returnType)
    {
        argTypes.addAny();
        returnType.addInt();
    }
};

RegisterFactory(AddAnyIntsFactory);
