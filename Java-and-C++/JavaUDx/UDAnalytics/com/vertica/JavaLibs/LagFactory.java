package com.vertica.JavaLibs;

import com.vertica.sdk.AnalyticFunction;
import com.vertica.sdk.AnalyticFunctionFactory;
import com.vertica.sdk.ColumnTypes;
import com.vertica.sdk.ServerInterface;
import com.vertica.sdk.SizedColumnTypes;
import com.vertica.sdk.UdfException;

public class LagFactory extends AnalyticFunctionFactory {

	@Override
	public AnalyticFunction createAnalyticFunction(ServerInterface srvInterface) {
		return new Lag();
	}

	@Override
	public void getPrototype(ServerInterface srvInterface, ColumnTypes argTypes,
			ColumnTypes returnType) {
		argTypes.addInt();
		argTypes.addInt();
		returnType.addInt();
	}

	@Override
	public void getReturnType(ServerInterface srvInterface, SizedColumnTypes argTypes,
			SizedColumnTypes returnType) throws UdfException {
		returnType.addInt();
	}

}
