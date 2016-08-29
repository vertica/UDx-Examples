/* Copyright (c) 2005 - 2015 Hewlett Packard Enterprise Development LP  -*- C++ -*-*/

/********************************
 * Example Vertica UDL that implements loading all files from a directory.
 ********************************/
#include "Vertica.h"
#include <fstream>

#include <dirent.h>
#include <stdlib.h>
#include <sys/stat.h>

#include <iostream>
#include <curl/curl.h>

#include "VPDistLib.h"

#include <set>
using namespace Vertica;




class VPDistSource : public UDSource {

public:
    VPDistSource(const std::string &url, const std::string &filename) : 
        _filename(filename), _vlib(url), _stream(NULL) {}

    void setup(ServerInterface &srvInterface) {
        _vlib.open();
        _stream = _vlib.getFile(_filename);
    }

    void destroy(ServerInterface &srvInterface) {
        delete _stream;
    }

private:
    std::string _filename;
    VDistLib   _vlib;
    VDistStream * _stream;

    virtual StreamState process(ServerInterface &srvInterface, DataBuffer &output) 
    {
        try {
            // Read data out of stream until we exhaust output space or input data
            while (true) {
                size_t avail = output.size - output.offset;
                const StreamChunk *chunk = _stream->peekChunk();
                
                if (!avail) return OUTPUT_NEEDED;
                if (!chunk) return DONE;
                
                memcpy(output.buf, chunk->data(), chunk->size());

                // update our state
                output.offset += chunk->size();
                _stream->consumeChunk();
            }
        } catch (VDistStreamException &e) {
            ereport(ERROR,
                    (errmsg("Stream error: %s", e.getMessage().c_str()),
                     errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)));
        }
        return DONE; // UNREACHABLE
    }
};

class VPDistSourceFactory : public SourceFactory 
{

public:


    // Tell Vertica what parameter types we need
    virtual void getParameterType(ServerInterface &srvInterface,
                                  SizedColumnTypes &parameterTypes) 
    {
        parameterTypes.addVarchar(64000, "url");
    }
 

    
    virtual void plan(ServerInterface &srvInterface,
                      NodeSpecifyingPlanContext &planCtx)
    {
        ParamWriter &pwriter = planCtx.getWriter();

        VDistLib vlib(getUrl(srvInterface));
        vlib.open();

        const std::vector<std::string>& nodes = 
          planCtx.getTargetNodes();

        // Keep track of which nodes actually have something useful to do
        std::set<size_t> usedNodes;

        // Get the files that the source has
        std::vector<std::string> files = vlib.getFiles();

        // Assign the files to the nodes in round robin fashion
        size_t nodeIdx = 0;
        std::stringstream ss;
        for (size_t i=0; i<files.size(); ++i) {
            // Param named nodename:i, value is file name
            ss.str("");
            ss << nodes[nodeIdx] << ":" << i;
            const std::string fieldName = ss.str();

            // Set the appropriate field
            pwriter.getStringRef(fieldName).copy(files[i]);

            // select next node, wrapping as necessary
            usedNodes.insert(nodeIdx);
            nodeIdx = (nodeIdx+1) % nodes.size();
        }

        // Set which nodes should be used
        std::vector<std::string> usedNodesStr;
        for (std::set<size_t>::iterator it = usedNodes.begin(); 
             it != usedNodes.end(); 
             ++it)  {
            usedNodesStr.push_back(nodes[*it]);
        }
        planCtx.setTargetNodes(usedNodesStr);
            

        vlib.close();
    }

    // Instantiate the actual user defined sources.
    virtual std::vector<UDSource*> prepareUDSources(ServerInterface &srvInterface,
                                                    NodeSpecifyingPlanContext &planCtx) 
    {
        const std::string nodeName = srvInterface.getCurrentNodeName();
        const std::string url      = getUrl(srvInterface);

        std::vector<UDSource*> retVal;

        // Find all the files destined for this node
        ParamReader &preader = planCtx.getReader();

        std::vector<std::string> paramNames = preader.getParamNames();
        for (std::size_t i=0; i<paramNames.size(); ++i) 
        {
            const std::string &paramName = paramNames[i];

            // if the param name starts with this node name, get the value
            // (which is a filename) and make a new source for reading that file
            size_t pos = paramName.find(nodeName);
            if (pos == 0) {
                std::string filename = preader.getStringRef(paramName).str();
                retVal.push_back
                  (vt_createFuncObject<VPDistSource>(srvInterface.allocator, url, filename));
            }
        }
        
        
        return retVal;
    }

private:
    // return the url that this source should pull from
    std::string getUrl(ServerInterface &si) {
       return si.getParamReader().getStringRef("url").str();
   }

};

RegisterFactory(VPDistSourceFactory);





