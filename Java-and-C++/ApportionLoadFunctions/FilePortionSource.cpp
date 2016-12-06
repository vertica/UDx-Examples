/* Copyright (c) 2005 - 2016 Hewlett Packard Enterprise Development LP  -*- C++ -*-*/

#include "Vertica.h"
#include "LoadArgParsers.h"
#include "../examples/SourceFunctions/filelib.cpp"
#include <stdio.h>
#include <glob.h>
#include <iostream>
#include <string>
#include <algorithm>

using namespace Vertica;

struct PortionInfo {
    std::string filename;
    Portion portion;
};
struct SortFilesByPortionSize {
    bool operator()(const PortionInfo &left, const PortionInfo &right) const {
        return left.portion.size < right.portion.size;
    }
};

class FilePortionSource : public FileSource {
private:
    Portion portion;

public:
    FilePortionSource(const std::string &filename, Portion p) : FileSource(filename), portion(p) {}

    // This function is required for apportion load to get source's portion information
    Portion getPortion() {
        return portion;
    }

    void setup(ServerInterface &srvInterface) {
        FileSource::setup(srvInterface);
        // seek portion to its file offset
        int r = fseek(handle, portion.offset, SEEK_SET);
        if (r) {
            vt_report_error(0, "disk seek failed for file %s at offset %lld", filename.c_str(), portion.offset);
        }
    }

    virtual vint getSize() {
        return portion.size;
    }
};

class FilePortionSourceFactory : public SourceFactory {
public:
    virtual void plan(ServerInterface &srvInterface,
            NodeSpecifyingPlanContext &planCtxt) {

        /* Check parameters */
        std::vector<ArgEntry> argSpec;
        argSpec.push_back((ArgEntry){"file", true, VerticaType(VarcharOID, -1)});
        argSpec.push_back((ArgEntry){"nodes", false, VerticaType(VarcharOID, -1)});
        argSpec.push_back((ArgEntry){"offsets", false, VerticaType(VarcharOID, -1)});
        argSpec.push_back((ArgEntry){"local_min_portion_size", false, VerticaType(Int8OID, -1)});
        validateArgs("FilePortionSource", argSpec, srvInterface.getParamReader());

        /* Populate planData */
        // Nothing to do here
        
        /* Munge nodes list */

        // only add the nodes specified in "nodes" arg, into execution node list;
        std::string nodes_arg;
        if (srvInterface.getParamReader().containsParameter("nodes")) {
            nodes_arg = srvInterface.getParamReader().getStringRef("nodes").str();
        } else {
            nodes_arg = srvInterface.getCurrentNodeName();
        }
        findExecutionNodes(srvInterface.getParamReader(), planCtxt, nodes_arg);

        std::vector<std::string> exe_nodes = planCtxt.getTargetNodes();
        uint num_nodes_needed = exe_nodes.size();
        if (srvInterface.getParamReader().containsParameter("offsets")) {
            std::string offsets = srvInterface.getParamReader().getStringRef("offsets").str();
            uint num_portions = std::count(offsets.begin(), offsets.end(), ',') + 1;
            if (num_nodes_needed > num_portions) {
                num_nodes_needed = num_portions;
            }
        }

        // Set which nodes should be used
        std::vector<std::string> usedNodesStr;
        // now give each node a unique id so that they know how to grab portions from a source..
        Vertica::ParamWriter &pwriter = planCtxt.getWriter();
        for(uint i = 0; i < num_nodes_needed; i++)
        {
            srvInterface.log("plan(): actual execution nodes were: %s", exe_nodes[i].c_str());
            pwriter.setInt(exe_nodes[i], i);
            usedNodesStr.push_back(exe_nodes[i]);
        }
        planCtxt.setTargetNodes(usedNodesStr);
    }

    /* how many threads do we want to use? */
    virtual ssize_t getDesiredThreads(ServerInterface &srvInterface,
            ExecutorPlanContext &planCtxt) {
        const std::string filename = srvInterface.getParamReader().getStringRef("file").str();

        /* 
         * expand the glob - at least one thread per source, certainly.
         * This and all subsequent operations run on each executor node
         * (whereas plan() runs only on the initiator node),
         * so it is fine to access and use any local files and resources
         */
        std::vector<std::string> paths;

        glob_t globbuf;
        globbuf.gl_offs = 0;
        int globres = glob(filename.c_str(), GLOB_ERR, NULL, &globbuf);
        if (globres == GLOB_NOSPACE) {
            vt_report_error(0, "Out of memory when expanding glob: %s", filename.c_str());
        } else if (globres == GLOB_ABORTED) {
            vt_report_error(0, "Read error when expanding glob: %s", filename.c_str());
        } else if (globres == GLOB_NOMATCH) {
            vt_report_error(0, "No files matching pattern [%s] were found", filename.c_str());
        } else {
            for (size_t count = 0; count < globbuf.gl_pathc; count++) {
                paths.push_back(globbuf.gl_pathv[count]);
            }
        }
        globfree(&globbuf);

        const std::string nodeName = srvInterface.getCurrentNodeName();
        const size_t nodeId = planCtxt.getWriter().getIntRef(nodeName);
        const size_t numNodes = planCtxt.getTargetNodes().size();
        if (!planCtxt.canApportionSource()) {
            /* no apportioning, so the number of files is the final number of sources */
            std::vector<std::string> *expanded =
                vt_createFuncObject<std::vector<std::string> >(srvInterface.allocator, paths);
            /* save expanded paths so we don't have to compute expansion again */
            planCtxt.getWriter().setPointer("expanded", expanded);
            return expanded->size();
        } else if (srvInterface.getParamReader().containsParameter("offsets")) {
            std::vector<std::string> *expanded =
                vt_createFuncObject<std::vector<std::string> >(srvInterface.allocator, paths);
            /* save expanded paths so we don't have to compute expansion again */
            planCtxt.getWriter().setPointer("expanded", expanded);

            /* 
             * if the offsets are specified, then we will have a fixed number of portions per file.
             * Round-robin assign offsets to nodes.
             */
            std::vector<size_t> offsets;
            std::string offsetsSpec = srvInterface.getParamReader().getStringRef("offsets").str();
            
            size_t start = 0, end = 0;
            while (end != std::string::npos) {
                end = offsetsSpec.find(',', start);
                offsets.push_back(atoi(offsetsSpec.substr(start,
                                end == std::string::npos ? std::string::npos : end - start).c_str()));
                start = end > (std::string::npos - 1) ? std::string::npos : end + 1;
            }
            std::sort(offsets.begin(), offsets.end());

            /* construct the portions that this node will actually handle.
             * This isn't changing (since the offset assignments are fixed),
             * so we'll build the Portion objects now and make them available
             * to prepareUDSourcesExecutor() by putting them in the ExecutorContext.
             * 
             * We don't know the size of the last portion, since it depends on the file
             * size.  Rather than figure it out here we will indicate it with -1 and
             * defer that to prepareUDSourcesExecutor().
             */
            std::vector<Portion> *portions =
                vt_createFuncObject<std::vector<Portion> >(srvInterface.allocator);

            for (std::vector<size_t>::const_iterator offset = offsets.begin();
                    offset != offsets.end(); ++offset) {
                Portion p(*offset);
                p.is_first_portion = (offset == offsets.begin());
                p.size = (offset + 1 == offsets.end() ? -1 : (*(offset + 1) - *offset));

                if ((offset - offsets.begin()) % numNodes == nodeId) {
                    portions->push_back(p);
                    srvInterface.log("FilePortionSource: assigning portion %ld: [offset = %lld, size = %lld]",
                            offset - offsets.begin(), p.offset, p.size);
                }
            }

            planCtxt.getWriter().setPointer("portions", portions);

            /* total number of threads we want is the number of portions per file, which is fixed */
            return portions->size() * expanded->size();
        }

        /*
         * Here we generate portions dynamically.  We want to target the actual number of
         * threads, which will be available in prepareUDSourcesExecutor().  Here we only know
         * how many the resource pool allows.
         *
         * We can compute here how many portions we WOULD use given the number of threads,
         * and then adjust that later.
         *
         * For simplicity's sake, each node will handle each file.  First we will figure out
         * the offset that this node is responsible for.
         *
         * We'll start by figuring out what this node is responsible for.
         */
        std::map<std::string, Portion> *portions =
            vt_createFuncObject<std::map<std::string, Portion> >(srvInterface.allocator);
        planCtxt.getWriter().setPointer("portionMap", portions);

        for (std::vector<std::string>::const_iterator file = paths.begin();
                file != paths.end(); ++file) {
            const size_t fileSize = getFileSize(*file);
            Portion p;
            p.offset = (fileSize / numNodes) * nodeId;
            p.size = nodeId == numNodes - 1 ?
                fileSize - p.offset : (fileSize / numNodes); /* last node gets the rest */
            p.is_first_portion = p.offset == 0;

            if (p.size > 0 || nodeId == numNodes - 1) {
                (*portions)[*file] = p;
            }
        }

        /* avoid requesting more portions than we have threads */
        if (portions->size() >= planCtxt.getMaxAllowedThreads()) {
            return paths.size();
        }

        /*
         * Now, we have more possible threads than portions.
         * There's no guarantee that we will actually get that many, but let's figure out
         * how many it makes sense to use.
         *
         * Simple scheme: minimum portion size will be 1MB (unless provided as a parameter).
         * As long as the largest portion is at least 2MB, we'll split it into two
         * evenly-sized pieces.  We won't actually split here, just figure out
         * how much splitting we want to do (halting early if we reach the max threads).
         */
        const vint localMinPortionSize =
            srvInterface.getParamReader().containsParameter("local_min_portion_size") ?
            srvInterface.getParamReader().getIntRef("local_min_portion_size") : 1024 * 1024;
        if (localMinPortionSize <= 0) {
            vt_report_error(0, "parameter \"local_min_portion_size\" must be positive");
        }

        std::vector<PortionInfo> splitPortions;
        for (std::map<std::string, Portion>::const_iterator portion = portions->begin();
                portion != portions->end(); ++portion) {
            PortionInfo info;
            info.filename = portion->first;
            info.portion = portion->second;

            splitPortions.push_back(info);
        }

        for (size_t i = 0;
                i < splitPortions.size() && splitPortions.size() < planCtxt.getMaxAllowedThreads();) {
            PortionInfo &info = splitPortions[i];
            if (info.portion.size >= 2 * localMinPortionSize) {
                const size_t portionSize = info.portion.size;
                PortionInfo split;
                split.filename = info.filename;
                split.portion.offset = info.portion.offset + portionSize / 2;
                split.portion.size = portionSize / 2;
                split.portion.is_first_portion = false;
                info.portion.size = (portionSize / 2) + (portionSize % 2);

                splitPortions.push_back(split);
            } else {
                i++;
            }
        }
        return splitPortions.size();
    }

    virtual std::vector<UDSource*> prepareUDSourcesExecutor(ServerInterface &srvInterface,
            ExecutorPlanContext &planCtxt) {
        std::vector<UDSource *> sources;
        if (!planCtxt.canApportionSource()) {
            const std::vector<std::string> *expandedPaths =
                planCtxt.getWriter().getPointer<std::vector<std::string> >("expanded");
            if (expandedPaths == NULL) {
                vt_report_error(0, "Expanded glob not found in context");
            }

            for (std::vector<std::string>::const_iterator filename = expandedPaths->begin();
                    filename != expandedPaths->end(); ++filename) {
                sources.push_back(vt_createFuncObject<FileSource>(srvInterface.allocator, *filename));
            }
        } else if (srvInterface.getParamReader().containsParameter("offsets")) {
            const std::vector<std::string> *expandedPaths =
                planCtxt.getWriter().getPointer<std::vector<std::string> >("expanded");
            if (expandedPaths == NULL) {
                vt_report_error(0, "Expanded glob not found in context");
            }

            std::vector<Portion> *portions =
                planCtxt.getWriter().getPointer<std::vector<Portion> >("portions");
            if (portions == NULL) {
                vt_report_error(0, "Portions not found in context");
            }
            prepareCustomizedPortions(srvInterface, planCtxt, sources, *expandedPaths, *portions);
        } else {
            std::map<std::string, Portion> *portions =
                planCtxt.getWriter().getPointer<std::map<std::string, Portion> >("portionMap");
            if (portions == NULL) {
                vt_report_error(0, "Portions map not found in context");
            }
            prepareGeneratedPortions(srvInterface, planCtxt, sources, *portions);
        }

        return sources;
    }

    // This function is required for apportion load to get source factory's apportionability
    virtual bool isSourceApportionable() {
        return true; 
    }

    /* prepare portions as determined via the "offsets" parameter */
    void prepareCustomizedPortions(ServerInterface &srvInterface,
                                   ExecutorPlanContext &planCtxt,
                                   std::vector<UDSource *> &sources,
                                   const std::vector<std::string> &expandedPaths,
                                   std::vector<Portion> &portions) {
        for (std::vector<std::string>::const_iterator filename = expandedPaths.begin();
                filename != expandedPaths.end(); ++filename) {
            /* 
             * the "portions" vector contains the portions which were generated in
             * "getDesiredThreads"
             */
            const size_t fileSize = getFileSize(*filename);
            for (std::vector<Portion>::const_iterator portion = portions.begin();
                    portion != portions.end(); ++portion) {
                Portion fportion(*portion);
                if (fportion.size == -1) {
                    /* as described above, this means from the offset to the end */
                    fportion.size = fileSize - portion->offset;
                    sources.push_back(vt_createFuncObject<FilePortionSource>(srvInterface.allocator,
                                *filename, fportion));
                } else if (fportion.size > 0) {
                    sources.push_back(vt_createFuncObject<FilePortionSource>(srvInterface.allocator,
                                *filename, fportion));
                }
            }
        }
    }

    void prepareGeneratedPortions(ServerInterface &srvInterface,
                                  ExecutorPlanContext &planCtxt,
                                  std::vector<UDSource *> &sources,
                                  std::map<std::string, Portion> initialPortions) {
        if ((ssize_t) initialPortions.size() >= planCtxt.getLoadConcurrency()) {
            /* all threads will be used, don't bother splitting into portions */
            for (std::map<std::string, Portion>::const_iterator file = initialPortions.begin();
                    file != initialPortions.end(); ++file) {
                sources.push_back(vt_createFuncObject<FilePortionSource>(srvInterface.allocator,
                            file->first, file->second));
            }
            return;
        }

        /* now we can split files to take advantage of potentially-unused threads */

        /* first sort by size (descending), then we will split the largest first */
        struct SortFilesByPortionSize sortFiles = SortFilesByPortionSize();

        std::vector<PortionInfo> portionHeap;
        for (std::map<std::string, Portion>::const_iterator file = initialPortions.begin();
                file != initialPortions.end(); ++file) {
            PortionInfo portion;
            portion.filename = file->first;
            portion.portion = file->second;
            portionHeap.push_back(portion);
        }
        std::make_heap(portionHeap.begin(), portionHeap.end(), sortFiles);

        const vint localMinPortionSize =
            srvInterface.getParamReader().containsParameter("local_min_portion_size") ?
            srvInterface.getParamReader().getIntRef("local_min_portion_size") : 1024 * 1024;
        while (!portionHeap.empty() && (ssize_t) portionHeap.size() < planCtxt.getLoadConcurrency()) {
            std::pop_heap(portionHeap.begin(), portionHeap.end(), sortFiles);
            PortionInfo &portion = portionHeap.back();

            const size_t portionSize = portion.portion.size;

            if ((vint) portionSize >= 2 * localMinPortionSize) {
                PortionInfo split;
                split.filename = portion.filename;
                split.portion.offset = portion.portion.offset + portionSize / 2;
                split.portion.size = portionSize / 2;
                split.portion.is_first_portion = false;

                portion.portion.size = (portionSize / 2) + (portionSize % 2);
                std::push_heap(portionHeap.begin(), portionHeap.end(), sortFiles);

                portionHeap.push_back(split);
                std::push_heap(portionHeap.begin(), portionHeap.end(), sortFiles);
            } else {
                /* too small to split */
                break;
            }
        }

        for (std::vector<PortionInfo>::const_iterator portion = portionHeap.begin();
                portion != portionHeap.end(); ++portion) {
            sources.push_back(vt_createFuncObject<FilePortionSource>(srvInterface.allocator,
                        portion->filename, portion->portion));
        }
    }

    off_t getFileSize(const std::string &filename) const {
        struct stat st;
        if (stat(filename.c_str(), &st) == -1) {
            vt_report_error(0, "Error in stat() for file [%s]", filename.c_str());
        }
        return st.st_size;
    }

    virtual void getParameterType(ServerInterface &srvInterface,
                                  SizedColumnTypes &parameterTypes) {
        parameterTypes.addVarchar(65000, "file");
        parameterTypes.addVarchar(65000, "nodes");
        parameterTypes.addVarchar(65000, "offsets");
        parameterTypes.addInt("local_min_portion_size");
    }
};
RegisterFactory(FilePortionSourceFactory);
