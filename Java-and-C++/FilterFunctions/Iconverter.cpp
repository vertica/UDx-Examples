/* Copyright (c) 2005 - 2015 Hewlett Packard Enterprise Development LP  -*- C++ -*-*/

#include "Vertica.h"

using namespace Vertica;

#include <string>
#include <stdlib.h>
#include <iconv.h>
#include <errno.h>


class Iconverter : public UDFilter
{
private:
    std::string fromEncoding, toEncoding;
    iconv_t cd; // the conversion descriptor opened
    uint converted; // how many characters have been converted

protected:
    virtual void setup(ServerInterface &srvInterface) {
        cd = iconv_open(toEncoding.c_str(), fromEncoding.c_str());
        if (cd == (iconv_t)(-1)) {
            vt_report_error(0, "Error initializing iconv: %m");
        }
    }
    virtual void destroy(ServerInterface &srvInterface) {
        if (cd != (iconv_t)(-1)) {
            // free iconv resources
            iconv_close(cd);
        }
    }

    virtual StreamState process(ServerInterface &srvInterface, DataBuffer &input, InputState input_state,
                                DataBuffer &output)
    {
        char *input_buf = (char *)input.buf + input.offset;
        char *output_buf = (char *)output.buf + output.offset;
        size_t inBytesLeft = input.size - input.offset, outBytesLeft = output.size - output.offset;

        // end of input
        if (input_state == END_OF_FILE && inBytesLeft == 0)
        {
            // Gnu libc iconv doc says, it is good practice to finalize the
            // outbuffer for stateful encodings (by calling with null inbuffer).
            //
            // http://www.gnu.org/software/libc/manual/html_node/Generic-Conversion-Interface.html
            iconv(cd, NULL, NULL, &output_buf, &outBytesLeft);
            // output buffer can be updated by this operation
            output.offset = output.size - outBytesLeft;
            return DONE;
        }

        size_t ret = iconv(cd, &input_buf, &inBytesLeft, &output_buf, &outBytesLeft);

        // if conversion is successful, we ask for more input, as input has not reached EOF.
        StreamState retStatus = input_state == END_OF_FILE ? DONE : INPUT_NEEDED;
        if (ret == (size_t)(-1))
        {
            // seen an error
            switch (errno)
            {
            case E2BIG:
                // input size too big, not a problem, ask for more output.
                retStatus = OUTPUT_NEEDED;
                break;
            case EINVAL:
                // input stops in the middle of a byte sequence, not a problem, ask for more input
                break;
            case EILSEQ:
                // invalid sequence seen, throw
                // TODO: reporting the wrong byte position
                vt_report_error(1, "Invalid byte sequence when doing %u-th conversion", converted);
            case EBADF:
                // something wrong with descriptor, throw
                vt_report_error(0, "Invalid descriptor");
            default:
                VIAssert("Uncommon error");
                break;
            }
        }
        else
        {
            converted += ret;
        }

        // move position pointer
        input.offset = input.size - inBytesLeft;
        output.offset = output.size - outBytesLeft;

        return retStatus;
    }

public:
    Iconverter(const std::string &from, const std::string &to) :
        fromEncoding(from), toEncoding(to), converted(0) {}
};

class IconverterFactory : public FilterFactory
{
public:
    virtual void plan(ServerInterface &srvInterface,
            PlanContext &planCtxt) {
        std::vector<std::string> args = srvInterface.getParamReader().getParamNames();

        /* Check parameters */
        if (!(args.size() == 0 ||
                (args.size() == 1 && find(args.begin(), args.end(), "from_encoding") != args.end()) ||
                (args.size() == 2
                        && find(args.begin(), args.end(), "from_encoding") != args.end()
                        && find(args.begin(), args.end(), "to_encoding") != args.end()))) {
            vt_report_error(0, "Invalid arguments.  Must specify either no arguments, or 'from_encoding' alone, or 'from_encoding' and 'to_encoding'.");
        }

        /* Populate planData */
        // By default, we do UTF16->UTF8, and x->UTF8

        VString from_encoding = planCtxt.getWriter().getStringRef("from_encoding");
        VString to_encoding = planCtxt.getWriter().getStringRef("to_encoding");

        from_encoding.copy("UTF-16");
        to_encoding.copy("UTF-8");

        if (args.size() == 2)
        {
            from_encoding.copy(srvInterface.getParamReader().getStringRef("from_encoding"));
            to_encoding.copy(srvInterface.getParamReader().getStringRef("to_encoding"));
        }
        else if (args.size() == 1)
        {
            from_encoding.copy(srvInterface.getParamReader().getStringRef("from_encoding"));
        }

        if (!from_encoding.length()) {
            vt_report_error(0, "The empty string is not a valid from_encoding value");
        }
        if (!to_encoding.length()) {
            vt_report_error(0, "The empty string is not a valid to_encoding value");
        }
    }

    virtual UDFilter* prepare(ServerInterface &srvInterface,
            PlanContext &planCtxt) {
        return vt_createFuncObject<Iconverter>(srvInterface.allocator,
                planCtxt.getReader().getStringRef("from_encoding").str(),
                planCtxt.getReader().getStringRef("to_encoding").str());
    }

    virtual void getParameterType(ServerInterface &srvInterface,
                                  SizedColumnTypes &parameterTypes) {
        parameterTypes.addVarchar(32, "from_encoding");
        parameterTypes.addVarchar(32, "to_encoding");
    }
};
RegisterFactory(IconverterFactory);
