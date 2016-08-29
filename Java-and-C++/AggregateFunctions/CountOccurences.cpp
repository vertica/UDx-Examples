/* Copyright (c) 2005 - 2015 Hewlett Packard Enterprise Development LP -*- C++ -*- */
/* 
 * Description: Example User Defined Aggregate Function: CountOccurences
 *
 * Create Date: Nov 01, 2014
 */

#include "Vertica.h"
#include <time.h> 
#include <sstream>
#include <iostream>

using namespace Vertica;
using namespace std;

/**
 * User Defined Aggregate Function CountOccurences that takes in floats and count the occurences of a 
 * number specified by the parameter n
 */
class CountOccurences : public AggregateFunction
{

    virtual void initAggregate(ServerInterface &srvInterface, 
                               IntermediateAggs &aggs)
    {
         //get the value of n for the parameters
        ParamReader paramReader = srvInterface.getParamReader();
        vfloat n = paramReader.getFloatRef("n");
        vfloat &num = aggs.getFloatRef(0);
        num = n;
        vint &cnt = aggs.getIntRef(1);
        cnt = 0;
    }

    void aggregate(ServerInterface &srvInterface, 
                   BlockReader &arg_reader, 
                   IntermediateAggs &aggs)
    {
        vint &cnt = aggs.getIntRef(1);
        vfloat &num = aggs.getFloatRef(0);
        do {
            const vfloat &input = arg_reader.getFloatRef(0);
            
            if (num==input) {
                cnt++;
            }
        } while (arg_reader.next());

    }

    virtual void combine(ServerInterface &srvInterface, 
                         IntermediateAggs &aggs, 
                         MultipleIntermediateAggs &aggs_other)
    {
        vint &myCount    = aggs.getIntRef(1);

        // Combine all the other intermediate aggregates
        do {
            const vint &otherCount = aggs_other.getIntRef(1);                        
            // Do the actual accumulation 
            myCount += otherCount;
        } while (aggs_other.next());

    }

    virtual void terminate(ServerInterface &srvInterface, 
                           BlockWriter &res_writer, 
                           IntermediateAggs &aggs)
    {
        res_writer.setInt(aggs.getIntRef(1));
    }
       
    InlineAggregate()
};


/*
 * This class provides the meta-data associated with the function
 * shown above, as well as a way of instantiating objects of the class. 
 */
class CountOccurencesFactory : public AggregateFunctionFactory
{
    virtual void getPrototype(ServerInterface &srvfloaterface, 
                              ColumnTypes &argTypes, 
                              ColumnTypes &returnType)
    {
        argTypes.addFloat();
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
        parameterTypes.addFloat("n");
    }
    virtual void getIntermediateTypes(ServerInterface &srvInterface,
                                      const SizedColumnTypes &input_types, 
                                      SizedColumnTypes 
                                      &intermediateTypeMetaData)
    {
        intermediateTypeMetaData.addFloat(); // the number to be counted
        intermediateTypeMetaData.addInt(); // count of items
    }

    virtual AggregateFunction *createAggregateFunction(ServerInterface &srvfloaterface)
    { return vt_createFuncObject<CountOccurences>(srvfloaterface.allocator); }

};

RegisterFactory(CountOccurencesFactory);

