package com.vertica.JavaLibs;

import java.util.ArrayList;

import com.vertica.sdk.AnalyticFunction;
import com.vertica.sdk.AnalyticFunctionFactory;
import com.vertica.sdk.ColumnTypes;
import com.vertica.sdk.ServerInterface;
import com.vertica.sdk.SizedColumnTypes;
import com.vertica.sdk.UdfException;
import com.vertica.sdk.VerticaType;

public class NthValueUsingParamFactory extends AnalyticFunctionFactory {

	@Override
	public AnalyticFunction createAnalyticFunction(ServerInterface srvInterface) {
		return new NthValueUsingParam();
	}

	@Override
	public void getPrototype(ServerInterface srvInterface, ColumnTypes argTypes,
			ColumnTypes returnType) {
		argTypes.addNumeric(); // numeric expression.
		returnType.addNumeric();
	}

	@Override
	public void getReturnType(ServerInterface srvInterface, SizedColumnTypes argTypes,
			SizedColumnTypes returnType) throws UdfException {
		ArrayList<Integer> argCols = new ArrayList<Integer>(); // argument column indexes
		argTypes.getArgumentColumns(argCols);
		
		// one argument is expected with optional Pby and Oby clauses.
		
		if(argCols.size() != 1 ||
				!argTypes.getColumnType(argCols.get(0)).isNumeric()){
			throw new UdfException(0, "One arguments (numeric) are expected with the parameter being a positive integer.");
		}
		
		VerticaType dt = argTypes.getColumnType(argCols.get(0));
		returnType.addNumeric(dt.getNumericPrecision(), dt.getNumericScale());
		
	}

	@Override
	public void getParameterType(ServerInterface srvInterface,
			SizedColumnTypes parameterTypes) {
		parameterTypes.addInt("n");
	}
	
}
