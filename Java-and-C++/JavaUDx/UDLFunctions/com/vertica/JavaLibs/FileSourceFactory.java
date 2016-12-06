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
import com.vertica.sdk.NodeSpecifyingPlanContext;
import com.vertica.sdk.ParamReader;
import com.vertica.sdk.Portion;
import com.vertica.sdk.ServerInterface;
import com.vertica.sdk.SizedColumnTypes;
import com.vertica.sdk.SourceFactory;
import com.vertica.sdk.UDSource;
import com.vertica.sdk.UdfException;

public class FileSourceFactory extends SourceFactory {
    @Override
    public void plan(ServerInterface srvInterface,
            NodeSpecifyingPlanContext planCtxt) throws UdfException {
        if (!srvInterface.getParamReader().containsParameter("file")) {
            throw new UdfException(0, "Required parameter \"file\" not found");
        }

        findExecutionNodes(srvInterface.getParamReader(), planCtxt, srvInterface.getCurrentNodeName());
    }

    @Override
    public void getParameterType(ServerInterface srvInterface, SizedColumnTypes parameterTypes) {
        parameterTypes.addVarchar(65000, "file");
        parameterTypes.addVarchar(256, "file_split_regex"); /* delimiter for splitting file names */
        parameterTypes.addVarchar(65000, "nodes");
    }

    @Override
    public ArrayList<UDSource> prepareUDSources(ServerInterface srvInterface,
            NodeSpecifyingPlanContext planCtxt) throws UdfException {
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

        String filename = srvInterface.getParamReader().getString("file");

        if (srvInterface.getParamReader().containsParameter("file_split_regex")) {
            ArrayList<UDSource> sources = new ArrayList<UDSource>();
            String[] fileNames = filename.split(srvInterface.getParamReader().getString("file_split_regex"));
            for (int i = 0; i < fileNames.length; i++) {
                sources.add(new FileSource(fileNames[i]));
            }
            return sources;
        } else {
            return new ArrayList<UDSource>(Collections.singletonList(new FileSource(filename)));
        }
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

        // Check for special magic values first
        if (nodes == "ALL NODES") {
            executionNodes = clusterNodes;
        } else if (nodes == "ANY NODE") {
            Collections.shuffle(clusterNodes);
            executionNodes.add(clusterNodes.get(0));
        } else if (nodes == "") {
            // Return the empty nodes list.
            // Vertica will deal with this case properly.
        } else {
            // Have to actually parse the string          
            // "nodes" is a comma-separated list of node names.
            String[] nodeNames = nodes.split(",");

            for (int i = 0; i < nodeNames.length; i++){
                if (clusterNodes.contains(nodeNames[i])) {
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
