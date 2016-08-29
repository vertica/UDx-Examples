/* Copyright (c) 2005 - 2015 Hewlett Packard Enterprise Development LP -*- C++ -*-*/
/* 
 *
 * Description: Example User Defined Scalar Function: Remove symbol
 *              This function removes the first 'n' occurrences of a 'symbol' from a string
 *              Function Parameters:
 *              - symbol (mandatory): the symbol to look for
 *              - n (optional): the number of occurrences to replace (default 1)
 *
 * Create Date: October 30, 2015
 */
#include "Vertica.h"

using namespace Vertica;

class RemoveSymbol : public ScalarFunction {
private:
    RemoveSymbol() : buf(NULL), symbol(0) { }

    char *buf; // auxiliary buffer to collect results
    char symbol; // symbol to remove
    int n; // number of occurrences to remove

    void setup(ServerInterface &srvInterface, const SizedColumnTypes &argTypes) {
        const VerticaType &t = argTypes.getColumnType(0);
        buf = new char[t.getStringLength()];
    }

    void destroy(ServerInterface &srvInterface, const SizedColumnTypes &argTypes) {
        // Release buf memory 
        delete[] buf;
        buf = NULL;
    }

public:
    RemoveSymbol(char symbol, int n) : buf(NULL), symbol(symbol), n(n) { }

    void processBlock(ServerInterface &srvInterface,
                      BlockReader &argReader,
                      BlockWriter &resWriter) {
        try {
            do {
                // Get a read-only pointer to the input string
                const VString &input = argReader.getStringRef(0);
                const char *data = input.data();
                const int len = input.length();

                // Remove symbol (at most n times) from input data
                int idx = 0; // buffer index
                int count = 0; // number of removals
                for (int i = 0; i < len; ++i) {
                    if (data[i] != symbol || count == n) {
                        buf[idx++] = data[i];
                    } else {
                        ++count;
                    }
                }

                // Output resulting buffer
                resWriter.getStringRef().copy(buf, idx);
                resWriter.next();
            } while (argReader.next());
        } catch(std::exception& e) {
            // Standard exception. Quit.
            vt_report_error(0, "Exception while processing block: [%s]", e.what());
        }
    }
};

class RemoveSymbolFactory : public ScalarFunctionFactory {
    ScalarFunction *createScalarFunction(ServerInterface &interface) {
        ParamReader params = interface.getParamReader();
        char symbol = 0;
        vint n = 1;

        // Get symbol parameter
        symbol = *params.getStringRef("symbol").data();

        // Check if optional parameter n was given
        if (params.containsParameter("n")) {
            const vint& pn = params.getIntRef("n");
            if (pn > 0) {
                n = pn;
            } else {
                vt_report_error(0, "Invalid parameter value %d", pn);
            }
        }

        return vt_createFuncObject<RemoveSymbol>(interface.allocator, symbol, n);
    }

    void getPrototype(ServerInterface &interface,
                      ColumnTypes &argTypes, ColumnTypes &returnType) {
        argTypes.addVarchar();
        returnType.addVarchar();
    }

    void getReturnType(ServerInterface &srvInterface,
                       const SizedColumnTypes &argTypes,
                       SizedColumnTypes &returnType) {
        const VerticaType &t = argTypes.getColumnType(0);
        returnType.addVarchar(t.getStringLength());
    }

    void getParameterType(ServerInterface &srvInterface,
                          SizedColumnTypes &parameterTypes) {
        // Add parameters with properties set
        SizedColumnTypes::Properties props;
        // Make parameters visible in user_function_parmeters
        props.visible = true;

        // Mandatory parameter
        props.required = true;
        props.canBeNull = false;
        props.comment = "Symbol to be removed";
        parameterTypes.addVarchar(1, "symbol", props);

        // Optional parameter
        props.required = false;
        props.canBeNull = false;
        props.comment = "Number of occurrences to remove";
        parameterTypes.addInt("n", props);
    }
};

RegisterFactory(RemoveSymbolFactory);
