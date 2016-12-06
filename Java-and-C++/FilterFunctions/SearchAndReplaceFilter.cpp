/* Copyright (c) 2005 - 2016 Hewlett Packard Enterprise Development LP  -*- C++ -*-*/

#include "Vertica.h"
#include "ContinuousUDFilter.h"

#include <algorithm>
using std::min;

using namespace Vertica;

class SearchAndReplaceFilter : public ContinuousUDFilter {
private:
    const std::string pattern, replace_with;

public:
    SearchAndReplaceFilter(std::string pattern, std::string replace_with)
        : pattern(pattern), replace_with(replace_with) {}

    static const size_t RESERVE = 256;

    void run() {
        // Reserve data in 256-byte chunks.
        // Be careful about not missing a pattern that's on a chunk border.

        while (!cr.isEof()) {
            size_t in_size = cr.reserve(RESERVE);
            char *in_ptr = (char*)cr.getDataPtr();

            size_t out_size = cw.reserve(RESERVE);
            char *out_ptr = (char*)cw.getDataPtr();

            size_t pos;
            size_t max_size = min(in_size, out_size);

            if (max_size < pattern.size()) break; // No more matches here

            for (pos = 0; pos < max_size - pattern.size() + 1; pos++) {
                if ((in_ptr[pos] == pattern[0])  // Check the first byte inline
                    && (memcmp(&in_ptr[pos], pattern.c_str(), pattern.size()) == 0)) {

                    // We found the pattern.
                    // We're going to be playing games with the output stream,
                    // so seek through everything we're done with first.
                    if (pos) cr.seek(pos);
                    if (pos) cw.seek(pos);

                    // Make sure we have enough space in the output stream for it
                    cw.reserve(replace_with.size());

                    // Then copy it to the output stream
                    memcpy(cw.getDataPtr(), replace_with.c_str(), replace_with.size());
                    cw.seek(replace_with.size());

                    // We don't need to do anything else with the pattern in
                    // the input stream
                    cr.seek(pattern.size());

                    // Reset position to 0 so that we don't try to seek
                    pos = 0;
                    break;
                }
                out_ptr[pos] = in_ptr[pos];
            }

            if (pos) cr.seek(pos);
            if (pos) cw.seek(pos);
        }

        // We know the last few bytes can't contain an instance of pattern;
        // it's too short.  So just copy them.
        cw.reserve(pattern.size()-1);
        size_t data_left = cr.reserve(RESERVE);
        memcpy(cw.getDataPtr(), cr.getDataPtr(), data_left);
        cr.seek(data_left);
        cw.seek(data_left);
    }
};



class SearchAndReplaceFilterFactory : public FilterFactory {
public:
    virtual void plan(ServerInterface &srvInterface,
            PlanContext &planCtxt) {
        std::vector<std::string> args = srvInterface.getParamReader().getParamNames();

        if (!(args.size() == 2
                && find(args.begin(), args.end(), "pattern") != args.end()
                && find(args.begin(), args.end(), "replace_with") != args.end())) // No args
        {
            vt_report_error(0, "Invalid arguments to OneTwoDecoderFactory.  Please specify 'pattern' and 'replace_with'.");
        }

        if (srvInterface.getParamReader().getStringRef("pattern").length() == 0) {
            vt_report_error(0, "Can't have zero-length 'pattern'; must have something to match with");
        }
        if (srvInterface.getParamReader().getStringRef("pattern").length() > 1000) {
            vt_report_error(0, "'pattern' is too long!; must be less than 1000 characters");
        }
    }

    virtual UDFilter* prepare(ServerInterface &srvInterface,
            PlanContext &planCtxt) {
        std::string pattern = srvInterface.getParamReader().getStringRef("pattern").str();
        std::string replace_with = srvInterface.getParamReader().getStringRef("replace_with").str();
        return vt_createFuncObject<SearchAndReplaceFilter>(srvInterface.allocator, pattern, replace_with);
    }

    virtual void getParameterType(ServerInterface &srvInterface,
                                  SizedColumnTypes &parameterTypes) {
        parameterTypes.addVarchar(65000, "pattern");
        parameterTypes.addVarchar(65000, "replace_with");
    }
};
RegisterFactory(SearchAndReplaceFilterFactory);

