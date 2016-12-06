/* Copyright (c) 2005 - 2016 Hewlett Packard Enterprise Development LP  -*- C++ -*-*/

#include "Vertica.h"

using namespace Vertica;

/**
 * Do nothing source. Accepts any args, but as the name suggests, does nothing with them.
 **/
class NoOpSource : public UDSource
{
public:
    StreamState process(ServerInterface &srvInterface, DataBuffer &dataBuffer) {
        return DONE;
    }
};

class NoOpSourceFactory : public SourceFactory {
public:

    virtual void plan(ServerInterface &srvInterface,
            NodeSpecifyingPlanContext &planCtxt)
    {}

    virtual std::vector<UDSource*> prepareUDSources(ServerInterface &srvInterface,
            NodeSpecifyingPlanContext &planCtxt) {
        std::vector<UDSource*> retVal;
        retVal.push_back(vt_createFuncObject<NoOpSource>(srvInterface.allocator));
        return retVal;
    }

    virtual void getParameterType(ServerInterface &srvInterface, SizedColumnTypes &parameterTypes)
    {}
};
RegisterFactory(NoOpSourceFactory);
