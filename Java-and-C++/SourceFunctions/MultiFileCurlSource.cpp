/* Copyright (c) 2005 - 2015 Hewlett Packard Enterprise Development LP  -*- C++ -*-*/

#include <set>
#include "Vertica.h"
#include "ContinuousUDSource.h"

#include "curl_support/VDistLib.h"

VDistStreamProducer* openURL(const std::string &url, Vertica::ServerInterface &srvInterface)
{
    return Vertica::vt_createFuncObject<VDistStreamProducer>(srvInterface.allocator, url);
}

/**
 * cURLSource
 *
 * The cURLSource Source takes one argument at the SQL command line
 * - "url" -- The URL of a plain-text file containing a list of
 *            other URLs (one per line) of files to download.
 *
 * This source will download that list of files, then distribute the files
 * among the nodes in the cluster; each node will then download the files
 * that it is given.  So cURLSource serves as a simple way to
 * distribute the loading of a large number of files quickly and easily
 * across a large Vertica cluster.
 */
class cURLSource : public ContinuousUDSource {
public:
    cURLSource(std::string url, std::string filename)
        : url(url), filename(filename) {}

    void initialize(Vertica::ServerInterface &srvInterface) {
        stream = openURL(filename, getServerInterface());
    }

    void run() {
        stream->produce(*this, WriteMemoryCallback);
    }

    void deinitialize(Vertica::ServerInterface &srvInterface) {
        /* nothing to do */
    }

    static size_t
    WriteMemoryCallback(void *contents, size_t size, size_t nmemb, void *userp)
    {
        cURLSource *src = (cURLSource *)userp;
        return src->cw.write(contents, size*nmemb);
    }

    // Needed by some of the curl infrastructure
    int                   _errorCode;
    char                  _errorMessage[1024];

private:
    std::string url;
    std::string filename;

    VDistStreamProducer* stream;
};

class cURLSourceFactory : public Vertica::SourceFactory
{

public:
    // Tell Vertica what parameter types we need
    virtual void getParameterType(Vertica::ServerInterface &srvInterface,
                                  Vertica::SizedColumnTypes &parameterTypes)
    {
        parameterTypes.addVarchar(65000, "url");
    }

    virtual void plan(Vertica::ServerInterface &srvInterface,
                      Vertica::NodeSpecifyingPlanContext &planCtx)
    {
        Vertica::ParamWriter &pwriter = planCtx.getWriter();

        VDistLib vlib(getUrl(srvInterface));
        vlib.open();

        const std::vector<std::string>& nodes =
          planCtx.getTargetNodes();

        // Keep track of which nodes actually have something useful to do
        std::set<size_t> usedNodes;

        // Get the files that the source has
        std::vector<std::string> files = vlib.getFiles("");

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
    virtual std::vector<Vertica::UDSource*> prepareUDSources(Vertica::ServerInterface &srvInterface,
                                                    Vertica::NodeSpecifyingPlanContext &planCtx)
    {
        const std::string nodeName = srvInterface.getCurrentNodeName();
        const std::string url      = getUrl(srvInterface);

        std::vector<Vertica::UDSource*> retVal;

        // Find all the files destined for this node
        Vertica::ParamReader &preader = planCtx.getReader();

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
                  (Vertica::vt_createFuncObject<cURLSource>(srvInterface.allocator, url, filename));
            }
        }


        return retVal;
    }

private:
    // return the url that this source should pull from
    std::string getUrl(Vertica::ServerInterface &si) {
       return si.getParamReader().getStringRef("url").str();
   }

};

RegisterFactory(cURLSourceFactory);












