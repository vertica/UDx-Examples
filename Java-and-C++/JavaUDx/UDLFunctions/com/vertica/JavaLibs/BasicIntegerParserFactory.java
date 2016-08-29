/* Copyright (c) 2005 - 2015 Hewlett Packard Enterprise Development LP  -*- Java -*-
 * 
 *  Create Date: September 10, 2013 
 * */

package com.vertica.JavaLibs;

import com.vertica.sdk.ParserFactory;
import com.vertica.sdk.PerColumnParamReader;
import com.vertica.sdk.PlanContext;
import com.vertica.sdk.ServerInterface;
import com.vertica.sdk.SizedColumnTypes;
import com.vertica.sdk.UDParser;
import com.vertica.sdk.UdfException;

public class BasicIntegerParserFactory extends ParserFactory {

	@Override
	public void plan(ServerInterface srvInterface,
	            PerColumnParamReader perColumnParamReader,
	            PlanContext planCtxt) {
	        /* Check parameters */
	        // TODO: Figure out what parameters I should have; then make sure I have them

	        /* Populate planData */
	        // Nothing to do here
	    }

	@Override
	public void getParserReturnType(ServerInterface srvInterface,
	            PerColumnParamReader perColumnParamReader,
	            PlanContext planCtxt,
	             SizedColumnTypes argTypes,
	            SizedColumnTypes returnType) {
	        // We only and always have a single integer column
	        returnType.addInt(argTypes.getColumnName(0));
	    }
	@Override
	public UDParser prepare(ServerInterface srvInterface,
			PerColumnParamReader perColumnParamReader, PlanContext planCtxt,
			SizedColumnTypes returnType) throws UdfException {
		
		return new BasicIntegerParser();
	}

}
