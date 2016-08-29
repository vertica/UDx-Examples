/* Copyright (c) 2005 - 2015 Hewlett Packard Enterprise Development LP  -*- C++ -*-*/

/* 
 *
 * Description: Example User Defined Scalar Function: Remove spaces
 *
 * Create Date: Apr 29, 2011
 */
#include "Vertica.h"

using namespace Vertica;

/*
 * This is a simple function that removes all spaces from the input string
 */
class RemoveSpace : public ScalarFunction
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
            if (argReader.getNumCols() != 1)
                vt_report_error(0, "Function only accept 1 arguments, but %zu provided", 
                                argReader.getNumCols());

            // While we have inputs to process
            do {
                // Get a copy of the input string
                std::string  inStr = argReader.getStringRef(0).str();
                char buf[inStr.size() + 1];


                // copy data from inStr to buf one character at a time
                const char *src = inStr.c_str();
                int len = 0;
                while (*src) {
                    if (*src != ' ') buf[len++] = *src;
                    src++;
                }
                buf[len] = '\0'; // null termiante

                // Copy string into results
                resWriter.getStringRef().copy(buf);
                resWriter.next();
            } while (argReader.next());
        } catch(std::exception& e) {
            // Standard exception. Quit.
            vt_report_error(0, "Exception while processing block: [%s]", e.what());
        }
    }
};

class RemoveSpaceFactory : public ScalarFunctionFactory
{
    // return an instance of RemoveSpace to perform the actual addition.
    virtual ScalarFunction *createScalarFunction(ServerInterface &interface)
    { return vt_createFuncObject<RemoveSpace>(interface.allocator); }

    virtual void getPrototype(ServerInterface &interface,
                              ColumnTypes &argTypes,
                              ColumnTypes &returnType)
    {
        argTypes.addVarchar();
        returnType.addVarchar();
    }

    virtual void getReturnType(ServerInterface &srvInterface,
                               const SizedColumnTypes &argTypes,
                               SizedColumnTypes &returnType)
    {
        const VerticaType &t = argTypes.getColumnType(0);
        returnType.addVarchar(t.getStringLength());
    }
};

RegisterFactory(RemoveSpaceFactory);

