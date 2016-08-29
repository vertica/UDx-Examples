/* Copyright (c) 2005 - 2015 Hewlett Packard Enterprise Development LP  -*- C++ -*- */
/* 
 * Description: Example User Defined Analytic Function Lead.
 *
 * Create Date: Nov 22, 2011
 */
#include "Vertica.h"
#include <sstream>

using namespace Vertica;
using namespace std;


/*
 * Lead takes in an integer expression and an offset value which determines how far to
 * move after the current row, and returns the expression evaluated on that row.
 * This function mimics the built-in analytic function LEAD, but
 * it is implemented only for integer input expressions.
 */
class Lead : public AnalyticFunction
{
    virtual void processPartition(ServerInterface &srvInterface, 
                                  AnalyticPartitionReader &inputReader, 
                                  AnalyticPartitionWriter &outputWriter)
    {
        try {
            const SizedColumnTypes &inTypes = inputReader.getTypeMetaData();
            vector<size_t> argCols; // Argument column indexes.
            inTypes.getArgumentColumns(argCols);

            vint leadOffset = inputReader.getIntRef(argCols.at(1)); // the offset.
            vint currentOffset = 0;
            vint value;
            bool moreRowsToWrite = true;

            do {
                if (currentOffset == leadOffset) {
                    value = inputReader.getIntRef(argCols.at(0)); // get ahead value.
                    outputWriter.setInt(0, value);
                    if (!outputWriter.next()) { moreRowsToWrite = false; break; }
                }
                else {
                    ++currentOffset;
                }
            } while (inputReader.next());

            if (!moreRowsToWrite) return;
            do {
                outputWriter.setInt(0, vint_null);
            } while (outputWriter.next());
        } catch(exception& e) {
            // Standard exception. Quit.
            vt_report_error(0, "Exception while processing partition: [%s]", e.what());
        }
    }
};

class LeadFactory : public AnalyticFunctionFactory
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
    { return vt_createFuncObject<Lead>(srvInterface.allocator); }

};

RegisterFactory(LeadFactory);
