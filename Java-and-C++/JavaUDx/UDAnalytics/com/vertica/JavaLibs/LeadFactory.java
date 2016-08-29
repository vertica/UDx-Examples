package com.vertica.JavaLibs;

import com.vertica.sdk.AnalyticFunction;
import com.vertica.sdk.AnalyticFunctionFactory;
import com.vertica.sdk.ColumnTypes;
import com.vertica.sdk.ServerInterface;
import com.vertica.sdk.SizedColumnTypes;
import com.vertica.sdk.UdfException;

public class LeadFactory extends AnalyticFunctionFactory {

	@Override
	public AnalyticFunction createAnalyticFunction(ServerInterface srvInterface) {
			return new Lead();
	}

	@Override
	public void getPrototype(ServerInterface srvInterface, ColumnTypes argTypes,
			ColumnTypes returnTypes) {
	       argTypes.addInt();
	       argTypes.addInt();
	       returnTypes.addInt();
	}

	@Override
	public void getReturnType(ServerInterface srvInterface, SizedColumnTypes argTypes,
			SizedColumnTypes returnTypes) throws UdfException {
		returnTypes.addInt();
	}
}
