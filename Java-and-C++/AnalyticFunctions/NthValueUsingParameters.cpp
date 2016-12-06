/* Copyright (c) 2005 - 2016 Hewlett Packard Enterprise Development LP  -*- C++ -*-*/
/*
 * Description: Example User Defined Analytic Function nth_value to output the value of the nth row.
 *
 * Create Date: Nov 01, 2014
 */
#include "Vertica.h"
#include <sstream>

using namespace Vertica;


/*
 * The analytic function nth_value(numeric a using parameters n) returns value "a" evaluated on 
 * the n-th row in the input partition defined by the OVER clause.
 * This function generalizes the built-in analytic functions FIRST_VALUE and LAST_VALUE, 
 * which return the first value and last value, respectively, of an input partition.
 */

class NthValueUsingParams : public AnalyticFunction
{
    virtual void processPartition(ServerInterface &srvInterface, 
                                  AnalyticPartitionReader &inputReader, 
                                  AnalyticPartitionWriter &outputWriter)
    {
        try {

            //get the value of n from the parameters
            ParamReader paramReader = srvInterface.getParamReader();
            vint nParam = paramReader.getIntRef("n");
            const SizedColumnTypes &inTypes = inputReader.getTypeMetaData();
            std::vector<size_t> argCols; // Argument column indexes.
            inTypes.getArgumentColumns(argCols);
            // One function arguments expected.
            if (argCols.size() != 1 ||
                !inTypes.getColumnType(argCols.at(0)).isNumeric() ||
                nParam < 1) // n positive?
                vt_report_error(1, "One argument (numeric) is expected with the parameter n being a positive integer.");
        
            vint currentRow = 1;
            bool nthRowExists = false;
        
            // Find the value of the n-th row.
            do {
                if (currentRow == nParam) {
                    nthRowExists = true;
                    break;
                } else {
                    currentRow++;
                }
            } while (inputReader.next());

            // Output the n-th value.
            if (nthRowExists) {
                do {
                    outputWriter.copyFromInput(0 /*dest*/, inputReader, argCols.at(0) /*src*/);
                } while (outputWriter.next());
            } else {
                // The partition has less than n rows. Output NULL values.
                do {
                    outputWriter.setNull(0);
                } while (outputWriter.next());
            }
        } catch(std::exception& e) {
            // Standard exception. Quit.
            vt_report_error(0, "Exception while processing partition: [%s]", e.what());
        }
    }
};

class NthValueUsingParamsFactory : public AnalyticFunctionFactory
{
    virtual void getPrototype(ServerInterface &srvInterface, ColumnTypes &argTypes, ColumnTypes &returnType)
    {
        argTypes.addNumeric(); // numeric expression.
        returnType.addNumeric();
    }

    virtual void getReturnType(ServerInterface &srvInterface,
                               const SizedColumnTypes &inputTypes,
                               SizedColumnTypes &outputTypes)
    {
        std::vector<size_t> argCols; // Argument column indexes.
        inputTypes.getArgumentColumns(argCols);

        // One argument is expected with optional Pby and Oby clauses.
        if (argCols.size() !=  1 || !inputTypes.getColumnType(argCols.at(0)).isNumeric())
                vt_report_error(1, "One arguments (numeric) is expected with the parameter n being a positive integer.");

        const VerticaType &vt = inputTypes.getColumnType(argCols.at(0));
        outputTypes.addNumeric(vt.getNumericPrecision(), vt.getNumericScale());
    }
    virtual void getParameterType(ServerInterface &srvInterface,
                                  SizedColumnTypes &parameterTypes)
    {
        parameterTypes.addInt("n");
    }
    virtual AnalyticFunction *createAnalyticFunction(ServerInterface &srvInterface)
    { return vt_createFuncObject<NthValueUsingParams>(srvInterface.allocator); }
};

RegisterFactory(NthValueUsingParamsFactory);
