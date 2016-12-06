/* Copyright (c) 2005 - 2016 Hewlett Packard Enterprise Development LP  -*- C++ -*-*/

#include "Vertica.h"
#include "LoadArgParsers.h"
#include <stdio.h>
#include <glob.h>

using namespace Vertica;

class FileSource : public UDSource {
protected:
    FILE *handle;
    std::string filename;

    virtual StreamState process(ServerInterface &srvInterface, DataBuffer &output) {
        output.offset += fread(output.buf + output.offset, 1, output.size - output.offset, handle);
        return feof(handle) ? DONE : OUTPUT_NEEDED;
    }

public:
    FileSource(const std::string &filename) : filename(filename) {}

    virtual void setup(ServerInterface &srvInterface) {
        handle = fopen(filename.c_str(),"r");

        // Validate the file handle; make sure we can read from this file
        if (handle == NULL) {
            vt_report_error(0, "Error opening file [%s]", filename.c_str());
        }
    }

    virtual void destroy(ServerInterface &srvInterface) {
        fclose(handle);
    }

    virtual std::string getUri() {return filename;}

    virtual vint getSize() {
        struct stat file;
        if (stat(filename.c_str(), &file)) {
            return vint_null;
        } else {
            return file.st_size;
        }
    }
};

class FileSourceFactory : public SourceFactory {
public:

    virtual void plan(ServerInterface &srvInterface,
            NodeSpecifyingPlanContext &planCtxt) {

        /* Check parameters */
        std::vector<ArgEntry> argSpec;
        argSpec.push_back((ArgEntry){"file", true, VerticaType(VarcharOID, -1)});
        argSpec.push_back((ArgEntry){"nodes", false, VerticaType(VarcharOID, -1)});
        validateArgs("FileSource", argSpec, srvInterface.getParamReader());

        /* Populate planData */
        // Nothing to do here
        
        /* Munge nodes list */
        findExecutionNodes(srvInterface.getParamReader(), planCtxt, srvInterface.getCurrentNodeName());
    }


    virtual std::vector<UDSource*> prepareUDSources(ServerInterface &srvInterface,
            NodeSpecifyingPlanContext &planCtxt) {
        std::vector<UDSource*> retVal;
        std::string filename = srvInterface.getParamReader().getStringRef("file").str();

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
        glob_t globbuf;
        globbuf.gl_offs = 0;

        int ret = glob(filename.c_str(), GLOB_ERR, NULL, &globbuf);

        if (ret == GLOB_NOSPACE)
          vt_report_error(0, "Out of memory when expanding glob: %s", filename.c_str());
        else if (ret == GLOB_ABORTED)
          vt_report_error(0, "Read error when expanding glob: %s", filename.c_str());
        else if (ret == GLOB_NOMATCH)
        {
            retVal.push_back(vt_createFuncObject<FileSource>(srvInterface.allocator,
                    filename));
        }
        else
        {
            for (size_t count = 0; count < globbuf.gl_pathc; count++)
                retVal.push_back(vt_createFuncObject<FileSource>(srvInterface.allocator,
                        std::string(globbuf.gl_pathv[count])));
        }
        globfree(&globbuf);

        return retVal;
    }

    virtual void getParameterType(ServerInterface &srvInterface,
                                  SizedColumnTypes &parameterTypes) {
        parameterTypes.addVarchar(65000, "file");
        parameterTypes.addVarchar(65000, "nodes");
    }
};
RegisterFactory(FileSourceFactory);
