/* Copyright (c) 2014- 2016 Hewlett Packard Enterprise Development LP  -*- C++ -*-*/
#include "Vertica.h"

using namespace Vertica;

const char * TEST_SESS_PARAM="test_session_param";

class SessionParamGetSet : public ScalarFunction
{
public:
    std::string pval;
    std::string wval;

    virtual void setup(ServerInterface &srvInterface,
            const SizedColumnTypes &argTypes)
    {
        if (srvInterface.getUDSessionParamReader().containsParameter(TEST_SESS_PARAM)) {
            pval = srvInterface.getUDSessionParamReader().getStringRef(TEST_SESS_PARAM).str();
        }
    }

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
                const VString &b = argReader.getStringRef(1);

                std::string bs = b.str();
                if (a > 0 && a <= 30000000 && a*bs.length() <= 30000000)
                {
                     wval = bs;
                     for (int i=1; i<a; ++i) wval+=bs;
                }

                resWriter.getStringRef().copy(pval);
                resWriter.next();
            } while (argReader.next());
        } catch(std::exception& e) {
            // Standard exception. Quit.
            vt_report_error(0, "Exception while processing block: [%s]", e.what());
        }
    }

    virtual void destroy(ServerInterface &srvInterface,
            const SizedColumnTypes &argTypes,
            SessionParamWriterMap &udParams)
    {
        if (wval.length())
        {
            udParams.getUDSessionParamWriter().getLongStringRef(TEST_SESS_PARAM).copy(wval);
        }
        else
        {
            udParams.getUDSessionParamWriter().clearParameter(TEST_SESS_PARAM);
        }
    }

};

class SessionParamGetSetFactory : public ScalarFunctionFactory
{
    // return an instance of Add2Ints to perform the actual addition.
    virtual ScalarFunction *createScalarFunction(ServerInterface &interface)
    { return vt_createFuncObject<SessionParamGetSet>(interface.allocator); }

    virtual void getPrototype(ServerInterface &interface,
                              ColumnTypes &argTypes,
                              ColumnTypes &returnType)
    {
        argTypes.addInt();
        argTypes.addVarchar();
        returnType.addLongVarchar();
    }

    virtual void getReturnType(ServerInterface &srvInterface,
                               const SizedColumnTypes &argTypes,
                               SizedColumnTypes &returnType)
    {
        returnType.addLongVarchar(30000000);
    }

    virtual void getParameterType(ServerInterface &srvInterface,
                                  SizedColumnTypes &parameterTypes)
    {
        parameterTypes.addInt("repeat_count");
        parameterTypes.addVarchar(65000, "value");
    }
};

RegisterFactory(SessionParamGetSetFactory);
