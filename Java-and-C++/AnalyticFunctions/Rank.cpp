/* Copyright (c) 2005 - 2015 Hewlett Packard Enterprise Development LP  -*- C++ -*-*/
/* 
 * Description: Example User Defined Analytic Function: Output nth row
 *
 * Create Date: Nov 22, 2011
 */
#include "Vertica.h"
#include <sstream>

using namespace Vertica;
using namespace std;



/**
 * User defined analytic function: Rank - works mostly the same as SQL-99 rank
 * with the ability to define as many order by columns as desired
 *
 * One difference is UDAn's do not currently require an order by clause in the DDL
 */
class Rank : public AnalyticFunction
{
    virtual void processPartition(ServerInterface &srvInterface, 
                                  AnalyticPartitionReader &inputReader, 
                                  AnalyticPartitionWriter &outputWriter)
    {
        try {
            rank = 0;
            numRowsWithSameOrder = 1;
            do {
                if (!inputReader.isNewOrderByKey()) {
                    ++numRowsWithSameOrder;
                }
                else {
                    rank += numRowsWithSameOrder;
                    numRowsWithSameOrder = 1;
                }
                outputWriter.setInt(0, rank);
                outputWriter.next();
            } while (inputReader.next());
        } catch(exception& e) {
            // Standard exception. Quit.
            vt_report_error(0, "Exception while processing partition: %s", e.what());
        }
    }

private:
    vint rank, numRowsWithSameOrder;
};

class RankFactory : public AnalyticFunctionFactory
{
    virtual void getPrototype(ServerInterface &srvInterface, ColumnTypes &argTypes, ColumnTypes &returnType)
    {
        returnType.addInt();
    }

    virtual void getReturnType(ServerInterface &srvInterface, 
                               const SizedColumnTypes &inputTypes, 
                               SizedColumnTypes &outputTypes)
    {
        outputTypes.addInt();
    }

    virtual void getParameterType(ServerInterface &srvInterface,
                                  SizedColumnTypes &parameterTypes)
    {
        // No parameters needed for Analytic Function Rank
    }

    virtual AnalyticFunction *createAnalyticFunction(ServerInterface &srvInterface)
    { return vt_createFuncObject<Rank>(srvInterface.allocator); }

};

RegisterFactory(RankFactory);

