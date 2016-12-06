/* Copyright (c) 2005 - 2016 Hewlett Packard Enterprise Development LP  -*- C++ -*-*/
/* 
 * Description: Example User Defined Analytic Function Lag.
 *
 * Create Date: Nov 22, 2011
 */
#include "Vertica.h"
#include <sstream>

using namespace Vertica;

/*
 * Lag takes in an input expression and an offset value which determines how far to
 * lag behind the current row, and returns the expression evaluated on that row.
 * This function mimics the built-in analytic function LAG, but
 * it is implemented only for integer input expressions.
 */
class Lag : public AnalyticFunction
{
    virtual void processPartition(ServerInterface &srvInterface, 
                                  AnalyticPartitionReader &inputReader, 
                                  AnalyticPartitionWriter &outputWriter)
    {
        try {
            const SizedColumnTypes &inTypes = inputReader.getTypeMetaData();
            std::vector<size_t> argCols; // Argument column indexes.
            inTypes.getArgumentColumns(argCols);

            vint lagOffset = inputReader.getIntRef(argCols.at(1)); // the offset.
            vint currentOffset = 0;
            vint value;

            do {
                if (currentOffset == lagOffset) {
                    value = inputReader.getIntRef(argCols.at(0)); // get lagged value.
                    outputWriter.setInt(0, value);
                    inputReader.next();
                }
                else {
                    ++currentOffset;
                    outputWriter.setInt(0, vint_null);
                }
            } while (outputWriter.next());
        } catch(std::exception& e) {
            // Standard exception. Quit.
            vt_report_error(0, "Exception while processing partition: [%s]", e.what());
        }
    }
};

class LagFactory : public AnalyticFunctionFactory
{
    virtual void getPrototype(ServerInterface &srvInterface, ColumnTypes &argTypes, ColumnTypes &returnType)
    {
        argTypes.addInt();
        argTypes.addInt();
        returnType.addInt();
    }

    virtual void getReturnType(ServerInterface &srvInterface, 
                               const SizedColumnTypes &inputTypes, 
                               SizedColumnTypes &outputTypes)
    {
        outputTypes.addInt();
    }

    virtual AnalyticFunction *createAnalyticFunction(ServerInterface &srvInterface)
    { return vt_createFuncObject<Lag>(srvInterface.allocator); }

};

RegisterFactory(LagFactory);
