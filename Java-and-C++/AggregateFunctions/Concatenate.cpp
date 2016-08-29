/* Copyright (c) 2005 - 2015 Hewlett Packard Enterprise Development LP -*- C++ -*- */
/* 
 * Description: Example User Defined Aggregate Function: Concatenate strings
 *
 */

#include "Vertica.h"
#include <time.h> 
#include <sstream>
#include <iostream>

using namespace Vertica;
using namespace std;


/**
 * User Defined Aggregate Function concatenate that takes in strings and concatenates
 * them together. Right now, the max length of the resulting string is ten times the
 * maximum length of the input string.
 */
class Concatenate : public AggregateFunction
{

    virtual void initAggregate(ServerInterface &srvInterface, IntermediateAggs &aggs)
    {
        try {
            VString &concat = aggs.getStringRef(0);
            concat.copy("");
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
            VString &concat = aggs.getStringRef(0);
            string word = concat.str();
            uint32 maxSize = aggs.getTypeMetaData().getColumnType(0).getStringLength();
            do {
                const VString &input = argReader.getStringRef(0);

                if (!input.isNull()) {
                    if ((word.length() + input.length()) > maxSize) break;
                    word.append(input.str());
                }
            } while (argReader.next());
            concat.copy(word);
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
            uint32 maxSize = aggs.getTypeMetaData().getColumnType(0).getStringLength();
            VString myConcat = aggs.getStringRef(0);

            do {
                const VString otherConcat = aggsOther.getStringRef(0);
                if ((myConcat.length() + otherConcat.length()) <= maxSize) {
                    string word = myConcat.str();
                    word.append(otherConcat.str());
                    myConcat.copy(word);
                }
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
            const VString &concat = aggs.getStringRef(0);
            VString &result = resWriter.getStringRef();

            result.copy(&concat);
        } catch(exception& e) {
            // Standard exception. Quit.
            vt_report_error(0, "Exception while computing aggregate output: [%s]", e.what());
        }
    }

    InlineAggregate()
};


class ConcatenateFactory : public AggregateFunctionFactory
{
    virtual void getIntermediateTypes(ServerInterface &srvInterface, const SizedColumnTypes &inputTypes, SizedColumnTypes &intermediateTypeMetaData)
    {
        int input_len = inputTypes.getColumnType(0).getStringLength();
        intermediateTypeMetaData.addVarchar(input_len*10);
    }

    virtual void getPrototype(ServerInterface &srvfloaterface, ColumnTypes &argTypes, ColumnTypes &returnType)
    {
        argTypes.addVarchar();
        returnType.addVarchar();
    }

    virtual void getReturnType(ServerInterface &srvfloaterface, 
                               const SizedColumnTypes &inputTypes, 
                               SizedColumnTypes &outputTypes)
    {
        int input_len = inputTypes.getColumnType(0).getStringLength();
        outputTypes.addVarchar(input_len*10);
    }

    virtual AggregateFunction *createAggregateFunction(ServerInterface &srvfloaterface)
    { return vt_createFuncObject<Concatenate>(srvfloaterface.allocator); }

};

RegisterFactory(ConcatenateFactory);

