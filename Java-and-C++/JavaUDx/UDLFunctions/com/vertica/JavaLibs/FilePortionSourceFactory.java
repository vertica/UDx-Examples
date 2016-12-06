/* Copyright (c) 2005 - 2016 Hewlett Packard Enterprise Development LP  -*- Java -*-
 * 
 *  Create Date: September 10, 2013 
 */
package com.vertica.JavaLibs;

import java.io.File;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.Vector;
import com.vertica.sdk.ExecutorPlanContext;
import com.vertica.sdk.NodeSpecifyingPlanContext;
import com.vertica.sdk.ParamReader;
import com.vertica.sdk.Portion;
import com.vertica.sdk.ServerInterface;
import com.vertica.sdk.SizedColumnTypes;
import com.vertica.sdk.SourceFactory;
import com.vertica.sdk.UDSource;
import com.vertica.sdk.UdfException;

public class FilePortionSourceFactory extends SourceFactory {
	@Override
	public void plan(ServerInterface srvInterface,
            NodeSpecifyingPlanContext planCtxt) throws UdfException {
        if (!srvInterface.getParamReader().containsParameter("file")) {
            throw new UdfException(0, "Required parameter \"file\" not found");
        }

        if (srvInterface.getParamReader().containsParameter("offsets")
                && !planCtxt.canApportionSource()) {
            throw new UdfException(0, "\"offsets\" parameter provided but source cannot be apportioned");
        }

		findExecutionNodes(srvInterface.getParamReader(), planCtxt, srvInterface.getCurrentNodeName());
    }
	
	@Override
	public void getParameterType(ServerInterface srvInterface, SizedColumnTypes parameterTypes) {
		parameterTypes.addVarchar(65000, "file");
		parameterTypes.addVarchar(65000, "nodes");
        parameterTypes.addVarchar(65000, "offsets");
	}

    @Override
    public boolean isSourceApportionable() {
        return true;
    }

    @Override
    public int getDesiredThreads(ServerInterface srvInterface, ExecutorPlanContext planCtxt) throws UdfException {
		//TODO
		// Do glob expansion; if the path contains '*', find all matching files.
        // Note that this has to be done in the prepare() method:
        // plan() runs on the initiator node, which may be a totally different
        // computer from the execution node that runs the actual query.
        // If we're trying to load a particular, say, directory full of files
        // on the local filesystem of the execution node, well, plan() doesn't
        // see the execution node's local filesystem, it sees the initiator's
        // local filesystem, so a glob expansion won't work at all.
        // prepare(), on the other hand, runs on the execution node.  So it's
        // fine to access local files and resources.
        // no apportioning unless the "offsets" parameter is provided
    
        if (!srvInterface.getParamReader().containsParameter("offsets")) {
            // only one file
            return 1;
        }

        // otherwise we'll round-robin the offsets to nodes
        String[] offsetStr = srvInterface.getParamReader().getString("offsets").split(",");
        int[] offsets = new int[offsetStr.length];
        for (int i = 0; i < offsetStr.length; i++) {
            try {
                offsets[i] = Integer.parseInt(offsetStr[i]);
            } catch (NumberFormatException e) {
                throw new UdfException(0, String.format("Error parsing offset \"%s\"", offsetStr[i]), e);
            }
        }
        Arrays.sort(offsets);

        File file = new File(srvInterface.getParamReader().getString("file"));
        if (!file.exists()) {
            throw new UdfException(0, String.format("File \"%s\" does not exist", file.getPath().toString()));
        }

        final long fileSize = file.length();

        List<String> executionNodes = planCtxt.getTargetNodes();
        List<Portion> myPortions = new ArrayList<Portion>();
        for (int i = 0; i < offsets.length; i++) {
            if (executionNodes.get(i % executionNodes.size())
                    .equals(srvInterface.getCurrentNodeName())) {
                long start = offsets[i];
                long size = (i == offsets.length - 1) ? (fileSize - start) : (offsets[i + 1] - start);

                myPortions.add(new Portion(start, size, i == 0));
            }
        }

        // save our portions for later so we don't have to re-compute them
        planCtxt.getWriter().setObject("myPortions", myPortions);

        return myPortions.size();
    }

	@Override
	public ArrayList<UDSource> prepareUDSources(ServerInterface srvInterface,
			ExecutorPlanContext planCtxt) throws UdfException {
		ArrayList<UDSource> sources = new ArrayList<UDSource>();
		String filename = srvInterface.getParamReader().getString("file");

        if (planCtxt.getReader().containsParameter("myPortions")) {
            List<Portion> myPortions = planCtxt.getWriter().<List<Portion>> getObject("myPortions");
            for (Portion p : myPortions) {
                sources.add(new FilePortionSource(filename, p));
            }
        } else {
            // just one source: the whole file
            sources.add(new FileSource(filename));
        }
		
		return sources;
	}
	
	private void findExecutionNodes(ParamReader args,
            NodeSpecifyingPlanContext planCtxt, String defaultList) throws UdfException {
	    String nodes;
	    ArrayList<String> clusterNodes = new ArrayList<String>(planCtxt.getClusterNodes());
	    ArrayList<String> executionNodes = new ArrayList<String>();

	    // If we found the nodes arg,
	    if (args.containsParameter("nodes")) {
	        nodes = args.getString("nodes");
	    } else if (defaultList != "" ) {
	        nodes = defaultList;
	    } else {
	        // We have nothing to do here.
	        return;
	    }

        // Check for "offsets" parameter which would limit the number of nodes
        int nodeLimit = -1;
        if (args.containsParameter("offsets")) {
            String[] offsets = args.getString("offsets").split(",");
            nodeLimit = offsets.length;

            if (nodeLimit == 0) {
                throw new UdfException(0, "Parameter \"offsets\" must be a comma-separated list of offsets");
            }
        }

	    // Check for special magic values first
	    if (nodes == "ALL NODES") {
            if (nodeLimit < 0) {
                executionNodes = clusterNodes;
            } else {
                executionNodes = new ArrayList<String>(clusterNodes.subList(0, nodeLimit));
            }
	    } else if (nodes == "ANY NODE") {
            Collections.shuffle(clusterNodes);
            if (nodeLimit < 0) {
                executionNodes.add(clusterNodes.get(0));
            } else {
                executionNodes = new ArrayList<String>(clusterNodes.subList(0, nodeLimit));
            }
	    } else if (nodes == "") {
	        // Return the empty nodes list.
	        // Vertica will deal with this case properly.
	    } else {
	        // Have to actually parse the string	      
	        // "nodes" is a comma-separated list of node names.
	    	String[] nodeNames = nodes.split(",");
	    	
            nodeLimit = nodeLimit < 0 ? clusterNodes.size() : nodeLimit;
	        for (int i = 0; i < nodeNames.length; i++){
	        	if (clusterNodes.contains(nodeNames[i]) && executionNodes.size() < nodeLimit) {
	        		executionNodes.add(nodeNames[i]);
                } else {
	            	String msg = String.format("Specified node '%s' but no node by that name is available.  Available nodes are \"%s\".",
	                        nodeNames[i], clusterNodes.toString());
	            	throw new UdfException(0, msg);
	            }
	        }
	    }

	    planCtxt.setTargetNodes(executionNodes);
	}
}
