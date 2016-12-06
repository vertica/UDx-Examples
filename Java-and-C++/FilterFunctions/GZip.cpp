/* Copyright (c) 2005 - 2016 Hewlett Packard Enterprise Development LP  -*- C++ -*-*/

#include <zlib.h>

#include "Vertica.h"
using namespace Vertica;


/**
 * GZipUnpacker. Decodes .gz format.
 */
class GZipUnpacker : public UDFilter {
private:
    z_stream_s zStream;

protected:
    virtual void setup(ServerInterface &srvInterface) {
        zStream.next_in = Z_NULL;
        zStream.avail_in = 0;
        zStream.zalloc = Z_NULL;
        zStream.zfree = Z_NULL;
        zStream.opaque = Z_NULL;

        //The 2nd parameter tells zlib to detect gzip/zlib.
        int zReturn = inflateInit2(&zStream,32 + MAX_WBITS);

        if (zReturn != Z_OK)
        {
            vt_report_error(0, "Error occurred during ZLIB initialization.  ZLIB error code: %d, Message: %s", zReturn, zStream.msg);
        }
    }

    virtual StreamState process(ServerInterface &srvInterface,
                                  DataBuffer      &input,
                                  InputState       input_state,
                                  DataBuffer      &output)
    {
        int zReturn;

        do {
            zStream.next_in = (Bytef*)(input.buf + input.offset);
            zStream.avail_in = input.size - input.offset;

            zStream.next_out = (Bytef*)(output.buf + output.offset);
            zStream.avail_out = output.size - output.offset;

            // Maybe done or maybe output was needed too?
            if (zStream.avail_in==0 && input_state==END_OF_FILE) return DONE;
            if (zStream.avail_out==0) return OUTPUT_NEEDED;

            zReturn = inflate(&zStream, Z_NO_FLUSH);

            // According to zlib manual, inflate() returns Z_BUF_ERROR if no progress is 
            // possible or if there was not enough room in the output buffer when Z_FINISH 
            // is used. Note that Z_BUF_ERROR is not fatal, and inflate() can be called 
            // again with more input and more output space to continue decompressing.
            if (zReturn == Z_BUF_ERROR) {
                input.offset += (input.size - input.offset) - zStream.avail_in;
                output.offset += (output.size - output.offset) - zStream.avail_out;

                if (input_state == END_OF_FILE && zStream.avail_in == 0) {
                    return DONE;
                } else if (zStream.avail_out == 0) {
                    return OUTPUT_NEEDED;
                } else {
                    return INPUT_NEEDED;
                }
            }
            else if (zReturn == Z_OK || zReturn == Z_STREAM_END)
            {
                input.offset += (input.size - input.offset) - zStream.avail_in;
                output.offset += (output.size - output.offset) - zStream.avail_out;
            }
            else
            {
                vt_report_error(0, "Error occurred during ZLIB decompression.  ZLIB error code: %d, Message: %s", zReturn, zStream.msg);
            }

            // In case of corrupt data, end early
            if (zReturn == Z_OK && zStream.avail_in == 0 && input_state == END_OF_FILE) {
                return DONE;
            }

            if (zReturn == Z_STREAM_END) {
                if (!(input_state == END_OF_FILE && input.offset == input.size)) {
                    // Support concatenated gzip files
                    // If two or more gzip files are concatenated together, the
                    // gzip library will realize that it's at the end of the first
                    // file; we have to reset it to start with the second file.
                    destroy(srvInterface);
                    setup(srvInterface);
                }
                else {
                    return DONE;
                }
            }
        } while (zReturn == Z_OK && output.offset != output.size && input.offset != input.size); 

        if (output.offset == output.size) {
            return OUTPUT_NEEDED;
        } else if (input.offset == input.size) {
            return INPUT_NEEDED;
        } else {
            return KEEP_GOING;
        }
    }

    virtual void destroy(ServerInterface &srvInterface) {
        inflateEnd(&zStream);
    }

public:
        GZipUnpacker() {}
};


class GZipUnpackerFactory : public FilterFactory {
public:
    virtual void plan(ServerInterface &srvInterface,
            PlanContext &planCtxt)
    { /* No plan-time setup work to do */ }

    virtual UDFilter* prepare(ServerInterface &srvInterface,
            PlanContext &planCtxt)
    {
        return vt_createFuncObject<GZipUnpacker>(srvInterface.allocator);
    }
};
RegisterFactory(GZipUnpackerFactory);

