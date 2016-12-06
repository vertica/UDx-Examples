/* Copyright (c) 2005 - 2016 Hewlett Packard Enterprise Development LP -*- C++ -*- */
/* 
 * Description: Example User Defined Aggregate Function: Get the longest strings
 *
 */

#include "Vertica.h"
#include <time.h> 
#include <sstream>
#include <iostream>

using namespace Vertica;


/**
 * User Defined Aggregate Function that takes in strings and gets the longest string.
 * If many strings have lengths equal to the longest string, it returns the string
 * that sorts last in ascending order.
 *
 * Note: NULL values are skipped.
 */
class LongestString : public AggregateFunction
{
private:
    /**
     * Get the longest string between the input strings and save it in longestStr
     * with length in longestStrLen
     */
    void getLongestString(VString &longestStr, vsize &longestStrLen, const VString &otherStr) {
        // Skip NULL strings
        if (otherStr.isNull()) {
            return;
        }

        const vsize otherStrLen = otherStr.length();
        if (longestStr.isNull() || otherStrLen > longestStrLen) {
            longestStr.copy(otherStr);
            longestStrLen = otherStrLen;
        } else if (otherStrLen == longestStrLen) {
            // Keep the greater string
            if (longestStr.compare(&otherStr) < 0) {
                longestStr.copy(otherStr);
            }
        }
    }

public:
    virtual void initAggregate(ServerInterface &srvInterface, IntermediateAggs &aggs)
    {
        VString &longestStr = aggs.getStringRef(0);
        longestStr.setNull();
    }

    void aggregate(ServerInterface &srvInterface,
                   BlockReader &argReader,
                   IntermediateAggs &aggs)
    {
        try {
            VString &longestStr = aggs.getStringRef(0);
            vsize longestStrLen = longestStr.isNull()? 0 : longestStr.length();

            do {
                // Compare input strings with the longest string so far
                // Update the longest string accordingly
                const VString &input = argReader.getStringRef(0);
                getLongestString(longestStr, longestStrLen, input);
            } while (argReader.next());
        } catch(std::exception& e) {
            // Standard exception. Quit.
            vt_report_error(0, "Exception while processing aggregate: [%s]", e.what());
        }
    }

    virtual void combine(ServerInterface &srvInterface, 
                         IntermediateAggs &aggs, 
                         MultipleIntermediateAggs &aggsOther)
    {
        try {
            VString &longestStr = aggs.getStringRef(0);
            vsize longestStrLen = longestStr.isNull()? 0 : longestStr.length();

            do {
                // Compare input strings with the longest string so far
                // Update the longest string accordingly
                const VString &otherLongestStr = aggsOther.getStringRef(0);
                getLongestString(longestStr, longestStrLen, otherLongestStr);
            } while (aggsOther.next());
        } catch(std::exception& e) {
            // Standard exception. Quit.
            vt_report_error(0, "Exception while combining intermediate aggregates: [%s]", e.what());
        }
    }

    virtual void terminate(ServerInterface &srvInterface, 
                           BlockWriter &resWriter, 
                           IntermediateAggs &aggs)
    {
        try {
            // Copy the longest string found in the output string
            const VString &longestStr = aggs.getStringRef(0);
            VString &result = resWriter.getStringRef();
            result.copy(&longestStr);
        } catch(std::exception& e) {
            // Standard exception. Quit.
            vt_report_error(0, "Exception while computing aggregate output: [%s]", e.what());
        }
    }

    InlineAggregate()
};


class LongestStringFactory : public AggregateFunctionFactory
{
    virtual void getIntermediateTypes(ServerInterface &srvInterface, const SizedColumnTypes &inputTypes, SizedColumnTypes &intermediateTypeMetaData)
    {
        // A copy of the longest string found in the input block
        intermediateTypeMetaData.addVarchar(inputTypes.getColumnType(0).getStringLength());
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
        outputTypes.addVarchar(inputTypes.getColumnType(0).getStringLength());
    }

    virtual AggregateFunction *createAggregateFunction(ServerInterface &srvfloaterface)
    { return vt_createFuncObject<LongestString>(srvfloaterface.allocator); }

};

RegisterFactory(LongestStringFactory);
