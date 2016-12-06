/* Copyright (c) 2005 - 2016 Hewlett Packard Enterprise Development LP  -*- C++ -*-*/


#include "Vertica.h"
#include <curl/curl.h>
#include "curl_fopen.h"

#include <iostream>

using namespace Vertica;

class CurlSource : public UDSource {
private:
    URL_FILE *handle;
    std::string url;

    virtual StreamState process(ServerInterface &srvInterface, DataBuffer &output) {
        output.offset = url_fread(output.buf, 1, output.size, handle);
        return url_feof(handle) ? DONE : OUTPUT_NEEDED;
    }

public:
    CurlSource(std::string url) : url(url) {}

    void setup(ServerInterface &srvInterface) {
        handle = url_fopen(url.c_str(),"r");
	if (!handle) { vt_report_error(0, "Could not open specified URL"); }
    }

    void destroy(ServerInterface &srvInterface) {
        url_fclose(handle);
    }

    virtual std::string getUri() {return url;}
};

class CurlSourceFactory : public SourceFactory {
public:

    virtual void plan(ServerInterface &srvInterface,
            NodeSpecifyingPlanContext &planCtxt) {
        std::vector<std::string> args = srvInterface.getParamReader().getParamNames();

        /* Check parameters */
        if (args.size() != 1 || find(args.begin(), args.end(), "url") == args.end()) {
            vt_report_error(0, "Must have exactly one argument, 'url'");
        }

        /* Populate planData */
        planCtxt.getWriter().getLongStringRef("url").copy(srvInterface.getParamReader().getStringRef("url"));
        
        /* Munge nodes list */
        std::vector<std::string> executionNodes = planCtxt.getClusterNodes();
        while (executionNodes.size() > 1) executionNodes.pop_back();  // Only run on one node.  Don't care which.
        planCtxt.setTargetNodes(executionNodes);
    }


    virtual std::vector<UDSource*> prepareUDSources(ServerInterface &srvInterface,
            NodeSpecifyingPlanContext &planCtxt) {
        std::vector<UDSource*> retVal;
        retVal.push_back(vt_createFuncObject<CurlSource>(srvInterface.allocator,
                planCtxt.getReader().getStringRef("url").str()));
        return retVal;
    }

    virtual void getParameterType(ServerInterface &srvInterface,
                                  SizedColumnTypes &parameterTypes) {
        parameterTypes.addLongVarchar(30000000, "url");
    }
};
RegisterFactory(CurlSourceFactory);
