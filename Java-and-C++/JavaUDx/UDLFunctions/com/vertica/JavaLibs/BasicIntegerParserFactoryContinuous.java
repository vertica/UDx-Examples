package com.vertica.JavaLibs;

import com.vertica.sdk.ParserFactory;
import com.vertica.sdk.PerColumnParamReader;
import com.vertica.sdk.PlanContext;
import com.vertica.sdk.ServerInterface;
import com.vertica.sdk.SizedColumnTypes;
import com.vertica.sdk.UDParser;
import com.vertica.sdk.UdfException;

public class BasicIntegerParserFactoryContinuous extends ParserFactory{

	@Override
	public UDParser prepare(ServerInterface arg0, PerColumnParamReader arg1,
			PlanContext arg2, SizedColumnTypes arg3) throws UdfException {
		
		return new BasicIntegerParserContinuous();
	}

	@Override
	public void getParserReturnType(ServerInterface srvInterface,
			PerColumnParamReader perColumnParamReader, PlanContext plancontext, SizedColumnTypes argTypes,
			SizedColumnTypes returnTypes) throws UdfException {
		returnTypes.addInt(argTypes.getColumnName(0));	
		}

	@Override
	public void getParameterType(ServerInterface srvInterface,
			SizedColumnTypes parameterTypes) {
		super.getParameterType(srvInterface, parameterTypes);
	}

}
