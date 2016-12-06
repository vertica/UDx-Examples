/* Copyright (c) 2005 - 2016 Hewlett Packard Enterprise Development LP  -*- C++ -*-
 *
 * Description: Example User Defined Scalar Function: Add 2 ints
 *
 * Create Date: Apr 29, 2011
 */
#include "Vertica.h"

class exception;
using namespace Vertica;

/*
 * This is a simple function that adds two integers and returns the result
 */
class Add2Ints : public ScalarFunction
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
            // Basic error checking
            if (argReader.getNumCols() != 2)
                vt_report_error(0, "Function only accept 2 arguments, but %zu provided", 
                                argReader.getNumCols());

            // While we have inputs to process
            do {
                const vint a = argReader.getIntRef(0);
                const vint b = argReader.getIntRef(1);
                resWriter.setInt(a+b);
                resWriter.next();
            } while (argReader.next());
        } catch(std::exception& e) {
            // Standard exception. Quit.
            vt_report_error(0, "Exception while processing block: [%s]", e.what());
        }
    }
};

class Add2IntsFactory : public ScalarFunctionFactory
{
    // return an instance of Add2Ints to perform the actual addition.
    virtual ScalarFunction *createScalarFunction(ServerInterface &interface)
    { return vt_createFuncObject<Add2Ints>(interface.allocator); }

    virtual void getPrototype(ServerInterface &interface,
                              ColumnTypes &argTypes,
                              ColumnTypes &returnType)
    {
        argTypes.addInt();
        argTypes.addInt();
        returnType.addInt();
    }
};

RegisterFactory(Add2IntsFactory);
