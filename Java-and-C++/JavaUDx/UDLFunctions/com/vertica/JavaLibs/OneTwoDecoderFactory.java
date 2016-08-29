/* Copyright (c) 2005 - 2015 Hewlett Packard Enterprise Development LP  -*- Java -*-
 * 
 *  Create Date: September 10, 2013 
 * */

package com.vertica.JavaLibs;

import java.util.ArrayList;
import java.util.Vector;

import com.vertica.sdk.FilterFactory;
import com.vertica.sdk.PlanContext;
import com.vertica.sdk.ServerInterface;
import com.vertica.sdk.SizedColumnTypes;
import com.vertica.sdk.UDFilter;
import com.vertica.sdk.UdfException;

public class OneTwoDecoderFactory extends FilterFactory {

	@Override
	public void plan(ServerInterface srvInterface,PlanContext planCtxt) throws UdfException {
	        ArrayList<String> args = srvInterface.getParamReader().getParamNames();

	        if (!(args.size() == 0 ||
	                (args.size() == 2
	                        && args.contains("char_to_replace") 
	                        && args.contains("char_to_replace_with")))) // No args
	        {
	            StringBuilder ss = new StringBuilder();
	            ss.append("Invalid arguments to OneTwoDecoderFactory:");
	            ss.append("[");
	            for (int i = 0; i < args.size(); i++) {
	            	ss.append(args.get(i));
	                if (i != args.size() - 1) ss.append(", ");
	            }
	            ss.append("]");
	            throw new UdfException(0, ss.toString());
	        }

	        if (args.size() == 2) {
	            {
	            	String char_to_replace = srvInterface.getParamReader().getString("char_to_replace");
	                planCtxt.getWriter().setString("char_to_replace",char_to_replace);
	                assert(char_to_replace.length() == 1);
	            }
	            {
	                String char_to_replace_with = srvInterface.getParamReader().getString("char_to_replace_with");
	                planCtxt.getWriter().setString("char_to_replace_with",char_to_replace_with);
	                assert(char_to_replace_with.length() == 1);
	            }
	        } else {
	            planCtxt.getWriter().setString("char_to_replace","1");
	            planCtxt.getWriter().setString("char_to_replace_with","2");
	        }
	        /* Populate planData */
	        // Nothing to do here
	    }
	@Override
	public UDFilter prepare(ServerInterface srvInterface, PlanContext planCtxt) throws UdfException {
		String char_to_replace = planCtxt.getWriter().getString("char_to_replace");
        String char_to_replace_with = planCtxt.getWriter().getString("char_to_replace_with");
       
		return new OneTwoDecoder(char_to_replace, char_to_replace_with);
	}
	
	@Override
	public void getParameterType(ServerInterface srvInterface,
             SizedColumnTypes parameterTypes) {
		parameterTypes.addVarchar(1, "char_to_replace");
		parameterTypes.addVarchar(1, "char_to_replace_with");
	}
}
