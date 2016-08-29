/* Copyright (c) 2005 - 2015 Hewlett Packard Enterprise Development LP -*- C++ -*- */
/* 
 * Description: Example User Defined Aggregate Function: Average
 *
 */

#include "Vertica.h"
#include <time.h> 
#include <sstream>
#include <iostream>

using namespace Vertica;
using namespace std;



/****
 * Example implementation of Average: intermediate is a 2 part type: running
 * sum and count.
 ***/
class Average : public AggregateFunction
{
    virtual void initAggregate(ServerInterface &srvInterface, 
                       IntermediateAggs &aggs)
    {
        try {
            VNumeric &sum = aggs.getNumericRef(0);
            sum.setZero();

            vint &cnt = aggs.getIntRef(1);
            cnt = 0;
        } catch(exception& e) {
            // Standard exception. Quit.
            vt_report_error(0, "Exception while initializing intermediate aggregates: [%s]", e.what());
        }
    }
    
    void aggregate(ServerInterface &srvInterface, 
                   BlockReader &argReader, 
                   IntermediateAggs &aggs)
    {
        try {
            VNumeric &sum = aggs.getNumericRef(0);
            vint     &cnt = aggs.getIntRef(1);

            do {
                const VNumeric &input = argReader.getNumericRef(0);
                if (!input.isNull()) {
                    sum.accumulate(&input);
                    cnt++;
                }
            } while (argReader.next());
        } catch(exception& e) {
            // Standard exception. Quit.
            vt_report_error(0, "Exception while processing aggregate: [%s]", e.what());
        }
    }

    virtual void combine(ServerInterface &srvInterface, 
                         IntermediateAggs &aggs, 
                         MultipleIntermediateAggs &aggsOther)
    {
        try {
            VNumeric       &mySum      = aggs.getNumericRef(0);
            vint           &myCount    = aggs.getIntRef(1);

            // Combine all the other intermediate aggregates
            do {
                const VNumeric &otherSum   = aggsOther.getNumericRef(0);
                const vint     &otherCount = aggsOther.getIntRef(1);
            
                // Do the actual accumulation 
                mySum.accumulate(&otherSum);
                myCount += otherCount;

            } while (aggsOther.next());
        } catch(exception& e) {
            // Standard exception. Quit.
            vt_report_error(0, "Exception while combining intermediate aggregates: [%s]", e.what());
        }
    }

    virtual void terminate(ServerInterface &srvInterface, 
                           BlockWriter &resWriter, 
                           IntermediateAggs &aggs)
    {
        try {
            // Metadata about the type (to allow creation)
            const VerticaType  &numtype = aggs.getTypeMetaData().getColumnType(0);
            const VNumeric     &sum     = aggs.getNumericRef(0);

            // Get the count as a numeric by making a local numeric
            //uint64 tmp[sum.getMaxSize() / sizeof(uint64)];
            uint64* tmp = (uint64*)malloc(numtype.getMaxSize() / sizeof(uint64));
            VNumeric cnt(tmp, numtype.getNumericPrecision(), numtype.getNumericScale());
            cnt.copy(aggs.getIntRef(1)); // convert to numeric!

            VNumeric &out = resWriter.getNumericRef();
            if (cnt.isZero())
                out.setZero();
            else
                out.div(&sum, &cnt);
        } catch(exception& e) {
            // Standard exception. Quit.
            vt_report_error(0, "Exception while computing aggregate output: [%s]", e.what());
        }
    }

    InlineAggregate()
};


/*
 * This class provides the meta-data associated with the aggregate function
 * shown above, as well as a way of instantiating objects of the class. 
 */
class AverageFactory : public AggregateFunctionFactory
{
    virtual void getPrototype(ServerInterface &srvfloaterface, 
                              ColumnTypes &argTypes, 
                              ColumnTypes &returnType)
    {
        argTypes.addNumeric();
        returnType.addNumeric();
    }

    // Provide return type length/scale/precision information (given the input
    // type length/scale/precision), as well as column names
    virtual void getReturnType(ServerInterface &srvfloaterface, 
                               const SizedColumnTypes &inputTypes, 
                               SizedColumnTypes &outputTypes)
    {
        int int_part = inputTypes.getColumnType(0).getNumericPrecision();
        int frac_part = inputTypes.getColumnType(0).getNumericScale();
        outputTypes.addNumeric(int_part+frac_part, frac_part);
    }

    virtual void getIntermediateTypes(ServerInterface &srvInterface,
                                      const SizedColumnTypes &inputTypes, 
                                      SizedColumnTypes 
                                      &intermediateTypeMetaData)
    {
        int int_part = inputTypes.getColumnType(0).getNumericIntegral();
        int frac_part = inputTypes.getColumnType(0).getNumericFractional();
        intermediateTypeMetaData.addNumeric(int_part+frac_part, frac_part); // intermediate sum
        intermediateTypeMetaData.addInt(); // count of items
    }

    // Create an instance of the AggregateFunction
    virtual AggregateFunction *createAggregateFunction(ServerInterface &srvfloaterface)
    { return vt_createFuncObject<Average>(srvfloaterface.allocator); }

};

RegisterFactory(AverageFactory);

