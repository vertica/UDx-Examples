package com.vertica.JavaLibs;

import java.util.ArrayList;

import com.vertica.sdk.AnalyticFunction;
import com.vertica.sdk.AnalyticFunctionFactory;
import com.vertica.sdk.ColumnTypes;
import com.vertica.sdk.ServerInterface;
import com.vertica.sdk.SizedColumnTypes;
import com.vertica.sdk.UdfException;
import com.vertica.sdk.VerticaType;

public class NthValueFactory extends AnalyticFunctionFactory {

	@Override
	public AnalyticFunction createAnalyticFunction(ServerInterface arg0) {
		return new NthValue();
	}

	@Override
	public void getPrototype(ServerInterface arg0, ColumnTypes arg1,
			ColumnTypes arg2) {
		arg1.addNumeric(); // numeric expression.
		arg1.addInt(); // n-th row argument.
		arg2.addNumeric();
	}

	@Override
	public void getReturnType(ServerInterface arg0, SizedColumnTypes arg1,
			SizedColumnTypes arg2) throws UdfException {
		ArrayList<Integer> argCols = new ArrayList<Integer>();
		arg1.getArgumentColumns(argCols);
		
		// Two arguments are expected with optional Pby and Oby clauses. 
		if(argCols.size() != 2 ||
				!arg1.getColumnType(argCols.get(0)).isNumeric()){
			throw new UdfException(0, "Two arguments (numeric, int) are expected with the second argument being a positive integer.");
		}
		
		VerticaType dt = arg1.getColumnType(argCols.get(0));
		arg2.addNumeric(dt.getNumericPrecision(), dt.getNumericScale());
	}

}
