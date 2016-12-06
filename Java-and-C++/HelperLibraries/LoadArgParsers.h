/* Copyright (c) 2005 - 2016 Hewlett Packard Enterprise Development LP  -*- C++ -*-*/

#include <string>
#include <vector>
#include <set>
#include <utility>

#include "Vertica.h"


#ifndef LOADARGPARSERS_H_
#define LOADARGPARSERS_H_

struct ArgEntry {
    std::string argName;
    bool required;
    Vertica::VerticaType type;
};

struct ColArgEntry : public ArgEntry {
    std::string colName;
};


/**
 * Validate a set of UDL arguments against an arg spec.
 * The arg spec specifies the type of each argument by name.
 * It also allows certain arguments to be optional:  If
 * an optional argument is found, it's not an error, but if
 * it's not found, it's also not an error.
 *
 * Throws an exception (via vt_report_error()) if arguments
 * not in the spec are found, or if arguments in the spec
 * are not found.pe
 *
 */
inline void validateArgs(std::string fnName, std::vector<ArgEntry> &argDescs, Vertica::ParamReader args) {
    std::vector<std::string> argNames = args.getParamNames();

    bool found = false;
    // Do the naive n^2 loop.
    // Typically the number of arguments is small enough
    // that the performance difference should be negligible.
    // If not, well, it's example code; an O(n log n)
    // merge-compare should be doable with a bit more work.

    // Check for extraneous arguments and type-mismatched arguments
    for (size_t argName = 0; argName < argNames.size(); argName++) {
        for (size_t argDesc = 0; argDesc < argDescs.size(); argDesc++) {
            if (argNames[argName] == argDescs[argDesc].argName) {
                /*Vertica::VerticaType argType = args.getTypeMetaData().getColumnType(args.getIndex(argNames[argName]));
                if (argType != argDescs[argDesc].type) {
                    vt_report_error(0, "Type mismatch in arg '%s' of '%s'; was expecting '%s', got '%s'",
                            argNames[argName].c_str(), fnName.c_str(), argDescs[argDesc].type.getTypeStr(), argType.getTypeStr());
                }*/
                found = true;
                break;
            }
        }

        if (!found) {
            vt_report_error(0, "Found unexpected argument in '%s': '%s'", fnName.c_str(), argNames[argName].c_str());
        }

        found = false;
    }

    // Check that all required arguments were required
    for (size_t argDesc = 0; argDesc < argDescs.size(); argDesc++) {
        if (!argDescs[argDesc].required) break;
        for (size_t argName = 0; argName < argNames.size(); argName++) {
            if (argNames[argName] == argDescs[argDesc].argName) {
                found = true;
                break;
            }
        }

        if (!found) {
            vt_report_error(0, "Didn't find expected argument '%s' in '%s'", argDescs[argDesc].argName.c_str(), fnName.c_str());
        }

        found = false;
    }
}

/**
 * Validate a set of UDL arguments against an arg spec.
 * The arg spec specifies the type of each argument by name.
 * It also allows certain arguments to be optional:  If
 * an optional argument is found, it's not an error, but if
 * it's not found, it's also not an error.
 *
 * The arg spec here is per-column; it deals with (and matches)
 * per-column arguments.  A column name of "" (the empty string)
 * signifies a match to all columns.
 *
 * Throws an exception (via vt_report_error()) if arguments
 * not in the spec are found, or if arguments in the spec
 * are not found.
 */
inline void validatePerColumnArgs(std::string fnName, std::vector<ColArgEntry> &argDescs, Vertica::PerColumnParamReader args) {
    std::vector< std::pair<std::string, std::string> > argNames;
    std::vector<std::string> colNames = args.getColumnNames();
    for (size_t i=0; i<colNames.size(); ++i) {
        Vertica::ParamReader &params = args.getColumnParamReader(colNames[i]);
        std::vector<std::string> paramNames = params.getParamNames();
        for (size_t j=0; j<paramNames.size(); ++j) {
            argNames.push_back(std::pair<std::string,std::string>(colNames[i], paramNames[j]));
        }
    }

    bool found = false;
    // Do the naive n^2 loop.
    // Typically the number of arguments is small enough
    // that the performance difference should be negligible.
    // If not, well, it's example code; an O(n log n)
    // merge-compare should be doable with a bit more work.

    // Check for extraneous arguments and type-mismatched arguments
    for (size_t argName = 0; argName < argNames.size(); argName++) {
        for (size_t argDesc = 0; argDesc < argDescs.size(); argDesc++) {
            if (argNames[argName].second == argDescs[argDesc].argName
                    && (argNames[argName].first == argDescs[argDesc].colName || argDescs[argDesc].colName == "")) {
                const std::string &colName = argNames[argName].first,
                    &paramName = argNames[argName].second;
                Vertica::VerticaType argType = args.getColumnParamReader(colName).getType(paramName);
                if (argType != argDescs[argDesc].type) {
                    vt_report_error(0, "Type mismatch in arg '%s.%s' of '%s'; was expecting '%s', got '%s'",
                            argNames[argName].first.c_str(), argNames[argName].second.c_str(), fnName.c_str(), argDescs[argDesc].type.getTypeStr(), argType.getTypeStr());
                }
                found = true;
                break;
            }
        }

        if (!found) {
            vt_report_error(0, "Found unexpected argument in '%s': '%s.%s'", fnName.c_str(), argNames[argName].first.c_str(), argNames[argName].second.c_str());
        }

        found = false;
    }

    // Check that all required arguments were required
    for (size_t argDesc = 0; argDesc < argDescs.size(); argDesc++) {
        if (!argDescs[argDesc].required) break;

        // require a match of all columns in this case
        // we're not given all column names, so, just go with all columns that we know about
        if (argDescs[argDesc].colName == "") {
            // Find all known column names (use a set for deduplication)
            std::set<std::string> names;
            for (size_t argName = 0; argName < argNames.size(); argName++) {
                names.insert(argNames[argName].first);
            }
            // Find all columns that have an arg by the given name
            std::set<std::string> found_names;
            for (size_t argName = 0; argName < argNames.size(); argName++) {
                if (argNames[argName].second == argDescs[argDesc].argName) {
                    found_names.insert(argNames[argName].first);
                }
            }

            // These two sets should be the same size.  If not...
            if (found_names.size() != names.size()) {
                vt_report_error(0, "Column argument '%s' is required by function '%' but was not provided for all columns", argDescs[argDesc].argName.c_str(), fnName.c_str());
            }
        }
        else {
            // Required for just one column
            for (size_t argName = 0; argName < argNames.size(); argName++) {
                if (argNames[argName].second == argDescs[argDesc].argName
                        && argNames[argName].first == argDescs[argDesc].colName) {
                    found = true;
                    break;
                }
            }

            if (!found) {
                vt_report_error(0, "Didn't find expected argument '%s.%s' in '%s'", argDescs[argDesc].colName.c_str(), argDescs[argDesc].argName.c_str(), fnName.c_str());
            }

            found = false;
        }
    }
}

// @INTERNAL helper function
inline std::string _nodeNames(const std::vector<std::string> &executionNodes) {
    // Helper for error-reporting.  Not for external use.
    std::stringstream ss;

    for (size_t i = 0; i < executionNodes.size(); i++) {
        ss << executionNodes[i];
        if (i != executionNodes.size() - 1) {
            ss << ',';
        }
    }

    return ss.str();
}

/**
 * checkExecutionNodesHelper
 *
 * Given a set of arguments 'args' and a mutable list of nodes 'executionNodes',
 * look for the nodes-list argument 'nodes_arg_name' in args.
 * If it's present, modify executionNodes to only contain listed nodes.
 *
 * Calls vt_report_error() if a listed node is not available.
 *
 * There are some special values for the nodes list:
 * - "ALL NODES": executionNodes is left unmodified, containing the full list of nodes
 * - "ANY NODE": One node is chosen at random from
 */
inline void findExecutionNodes(Vertica::ParamReader args, Vertica::NodeSpecifyingPlanContext &planCtxt, std::string defaultList = "", std::string nodes_arg_name = "nodes") {
    std::string nodes;
    const std::vector<std::string> clusterNodes = planCtxt.getClusterNodes();
    std::vector<std::string> executionNodes;

    // If we found the nodes arg,
    if (args.containsParameter(nodes_arg_name)) {// &&
//            args.getTypeMetaData().getColumnType(args.getIndex(nodes_arg_name)).isStringType()) {
        nodes = args.getStringRef(nodes_arg_name).str();
    } else if (defaultList != "") {
        nodes = defaultList;
    } else {
        // We have nothing to do here.
        return;
    }

    // Check for special magic values first
    if (nodes == "ALL NODES") {
        // executionNodes is already set correctly
        executionNodes = clusterNodes;
    }
    else if (nodes == "ANY NODE") {
        // Seed the random number generator
        time_t seconds;
        time(&seconds);
        srand((unsigned int)seconds);

        // Pick a node at random
        std::string tmpNode = clusterNodes[rand()%clusterNodes.size()];

        // Get rid of all the other nodes; return just the one
        executionNodes.push_back(tmpNode);
    } else if (nodes == "") {
        // Return the empty nodes list.
        // Vertica will deal with this case properly.
    } else {
        // Have to actually parse the string

        // The STL doesn't really have a string tokenizer.
        // So, let's pretend, using stringstream and getline (which takes a custom delimiter).
        // "nodes" is a comma-separated list of node names.
        std::stringstream ss(nodes);
        std::string nodeName;

        bool found = false;
        while (std::getline(ss, nodeName, ',')) {
            for (size_t i = 0; i < clusterNodes.size(); i++) {
                if (clusterNodes[i] == nodeName) {
                    executionNodes.push_back(nodeName);
                    found = true;
                    break;
                }
            }

            if (!found) {
                // If we get here, the named node wasn't found in the list.
                vt_report_error(0, "Specified node '%s' but no node by that name is available.  Available nodes are \"%s\".",
                        nodeName.c_str(), _nodeNames(clusterNodes).c_str());
            }

            found = false;
        }
    }

    planCtxt.setTargetNodes(executionNodes);
}


#endif // LOADARGPARSERS_H_
