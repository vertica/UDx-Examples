/* Copyright (c) 2005 - 2016 Hewlett Packard Enterprise Development LP -*- C++ -*- */
/* 
 * Description: Example User Defined Aggregate Function: Average
 *
 */

#include "Vertica.h"
#include <time.h> 
#include <sstream>
#include <iostream>

using namespace Vertica;


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

            vint &count = aggs.getIntRef(1);
            count = 0;
        } catch(std::exception &e) {
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
            vint     &count = aggs.getIntRef(1);

            do {
                const VNumeric &input = argReader.getNumericRef(0);
                if (!input.isNull()) {
                    sum.accumulate(&input);
                    count++;
                }
            } while (argReader.next());
        } catch(std::exception &e) {
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
        } catch(std::exception &e) {
            // Standard exception. Quit.
            vt_report_error(0, "Exception while combining intermediate aggregates: [%s]", e.what());
        }
    }

    virtual void terminate(ServerInterface &srvInterface, 
                           BlockWriter &resWriter, 
                           IntermediateAggs &aggs)
    {
        try {

            // Get the count as a numeric by making a local numeric
            // The largest int has about 20 digits. GO with that precision
            const int32 MAX_INT_PRECISION = 20;
            const int32 prec = Basics::getNumericWordCount(MAX_INT_PRECISION);
            uint64 words[prec];
            VNumeric count(words, prec, 0 /*scale*/);
            count.copy(aggs.getIntRef(1)); // convert to numeric!

            VNumeric &out = resWriter.getNumericRef();
            if (count.isZero()) {
                // Input column had no rows. Return NULL
                out.setNull();
            } else {
                // Compute actual average
                const VNumeric &sum = aggs.getNumericRef(0);
                out.div(&sum, &count);
            }
        } catch(std::exception &e) {
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
        const VerticaType &inType = inputTypes.getColumnType(0);
        outputTypes.addNumeric(inType.getNumericPrecision(), inType.getNumericScale());
    }

    virtual void getIntermediateTypes(ServerInterface &srvInterface,
                                      const SizedColumnTypes &inputTypes, 
                                      SizedColumnTypes &intermediateTypeMetaData)
    {
        const VerticaType &inType = inputTypes.getColumnType(0);

        // intermediate sum
        int32 interPrec = inType.getNumericPrecision() + 3; // provision 1000x precision if possible
        const int32 MAX_NUMERIC_PREC = 1024;
        if (interPrec > MAX_NUMERIC_PREC) {
            interPrec = MAX_NUMERIC_PREC;
        }
        intermediateTypeMetaData.addNumeric(interPrec, inType.getNumericScale());
        intermediateTypeMetaData.addInt(); // count of items
    }

    // Create an instance of the AggregateFunction
    virtual AggregateFunction *createAggregateFunction(ServerInterface &srvInterface)
    { return vt_createFuncObject<Average>(srvInterface.allocator); }

};

RegisterFactory(AverageFactory);

