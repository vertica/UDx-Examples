/* Copyright (c) 2005 - 2016 Hewlett Packard Enterprise Development LP -*- C++ -*- */
/* 
 * Description: Example User Defined Aggregate Function to count the number of times
 *              a specific integer appears in the input
 *
 * Create Date: Nov 01, 2014
 */

#include "Vertica.h"
#include <time.h> 
#include <sstream>
#include <iostream>

using namespace Vertica;

/**
 * User Defined Aggregate Function CountOccurences that takes in integers
 * and counts the occurences of a number specified by the parameter n
 */
class CountOccurences : public AggregateFunction
{
    vint n; // integer to look for

public:
    CountOccurences(): n(vint_null) { }

    virtual void setup(ServerInterface &srvInterface,
                       const SizedColumnTypes &argTypes) {
        // Get the value of n from the parameters
        ParamReader paramReader = srvInterface.getParamReader();
        if (!paramReader.containsParameter("n")) {
            vt_report_error(0, "You must provide a value for parameter n");
        }
        n = paramReader.getIntRef("n");
    }

    virtual void initAggregate(ServerInterface &srvInterface, 
                               IntermediateAggs &aggs)
    {
        vint &count = aggs.getIntRef(0);
        count = 0;
    }

    void aggregate(ServerInterface &srvInterface, 
                   BlockReader &arg_reader, 
                   IntermediateAggs &aggs)
    {
        vint &count = aggs.getIntRef(0);
        do {
            const vint &input = arg_reader.getIntRef(0);
            
            if (n == input) {
                count++;
            }
        } while (arg_reader.next());
    }

    virtual void combine(ServerInterface &srvInterface, 
                         IntermediateAggs &aggs, 
                         MultipleIntermediateAggs &aggs_other)
    {
        vint &myCount = aggs.getIntRef(0);

        // Combine all the other intermediate aggregates
        do {
            const vint &otherCount = aggs_other.getIntRef(0);
            // Do the actual accumulation 
            myCount += otherCount;
        } while (aggs_other.next());
    }

    virtual void terminate(ServerInterface &srvInterface, 
                           BlockWriter &res_writer, 
                           IntermediateAggs &aggs)
    {
        res_writer.setInt(aggs.getIntRef(0));
    }
       
    InlineAggregate()
};


/*
 * This class provides the metadata associated with the function
 * shown above, as well as a way of instantiating objects of the class. 
 */
class CountOccurencesFactory : public AggregateFunctionFactory
{
    virtual void getPrototype(ServerInterface &srvfloaterface, 
                              ColumnTypes &argTypes, 
                              ColumnTypes &returnType)
    {
        argTypes.addInt();
        returnType.addInt();
    }

    virtual void getReturnType(ServerInterface &srvfloaterface, 
                               const SizedColumnTypes &input_types, 
                               SizedColumnTypes &output_types)
    {
        output_types.addInt();
    }

    virtual void getParameterType(ServerInterface &srvInterface,
                                  SizedColumnTypes &parameterTypes)
    {
        parameterTypes.addInt("n");
    }

    virtual void getIntermediateTypes(ServerInterface &srvInterface,
                                      const SizedColumnTypes &input_types, 
                                      SizedColumnTypes &intermediateTypeMetaData)
    {
        intermediateTypeMetaData.addInt(); // intermediate count of items
    }

    virtual AggregateFunction *createAggregateFunction(ServerInterface &srvInterface)
    { return vt_createFuncObject<CountOccurences>(srvInterface.allocator); }
};

RegisterFactory(CountOccurencesFactory);
