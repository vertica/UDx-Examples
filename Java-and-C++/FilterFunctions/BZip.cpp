/* Copyright (c) 2005 - 2015 Hewlett Packard Enterprise Development LP  -*- C++ -*-*/

#include <bzlib.h>

#include "Vertica.h"
using namespace Vertica;


/**
 * GZipUnpacker. Decodes .gz format.
 */
class BZipUnpacker : public UDFilter {
private:
    bz_stream bStream;

protected:
    virtual void setup(ServerInterface &srvInterface) {
        bStream.next_in = NULL;
        bStream.avail_in = 0;
        bStream.bzalloc = NULL;
        bStream.bzfree = NULL;
        bStream.opaque = NULL;

        int bReturn = BZ2_bzDecompressInit(&bStream,0,0);

        if (bReturn != BZ_OK)
        {
            vt_report_error(0, "Error occurred during BZIP initialization.  BZIP error code: %d", bReturn);
        }
    }

    virtual StreamState process(ServerInterface &srvInterface,
                                  DataBuffer      &input,
                                  InputState       input_state,
                                  DataBuffer      &output)
    {
        int bReturn;

        do {
            bStream.next_in = input.buf + input.offset;
            bStream.avail_in = input.size - input.offset;

            bStream.next_out = output.buf + output.offset;
            bStream.avail_out = output.size - output.offset;

            bReturn = BZ2_bzDecompress(&bStream);

            if (bReturn == BZ_OK || bReturn == BZ_STREAM_END)
            {
                input.offset += (input.size - input.offset) - bStream.avail_in;
                output.offset += (output.size - output.offset) - bStream.avail_out;
            }
            else
            {
                vt_report_error(0, "Error occurred during BZIP decompression.  BZIP error code: %d", bReturn);
            }

            // In case of corrupt data, end early
            if (bReturn == BZ_OK && bStream.avail_in == 0 && input_state == END_OF_FILE) {
                return DONE;
            }

            if (bReturn == BZ_STREAM_END) {
                if (!(input_state == END_OF_FILE && input.offset == input.size)) {
                    // Support concatenated bzip files
                    // If two or more bzip files are concatenated together, the
                    // bzip library will realize that it's at the end of the first
                    // file; we have to reset it to start with the second file.
                    destroy(srvInterface);
                    setup(srvInterface);
                }
                else {
                    return DONE;
                }
            }
        } while (bReturn == BZ_OK && output.offset != output.size && input.offset != input.size);

        if (output.offset == output.size) {
            return OUTPUT_NEEDED;
        } else if (input.offset == input.size) {
            return INPUT_NEEDED;
        } else {
            return KEEP_GOING;
        }
    }

    virtual void destroy(ServerInterface &srvInterface) {
        BZ2_bzDecompressEnd(&bStream);
    }

public:
        BZipUnpacker() {}
};


class BZipUnpackerFactory : public FilterFactory {
public:
    virtual void plan(ServerInterface &srvInterface,
            PlanContext &planCtxt)
    { /* No plan-time setup work to do */ }

    virtual UDFilter* prepare(ServerInterface &srvInterface,
            PlanContext &planCtxt)
    {
        return vt_createFuncObject<BZipUnpacker>(srvInterface.allocator);
    }
};
RegisterFactory(BZipUnpackerFactory);

