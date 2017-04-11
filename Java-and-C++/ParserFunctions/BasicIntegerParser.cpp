/* Copyright (c) 2005 - 2016 Hewlett Packard Enterprise Development LP  -*- C++ -*-*/

#include "Vertica.h"
#include "ContinuousUDParser.h"

using namespace Vertica;

#include <string>
#include <sstream>

/**
 * Basic Integer parser
 * Parses a string of integers separated by non-numeric characters.
 * Uses the built-in Vertica SDK API.
 */
class BasicIntegerParser : public UDParser {
private:
    vint strToInt(const std::string &str) {
        vint retVal;
        std::stringstream ss;
        ss << str;
        ss >> retVal;
        return retVal;
    }
    vint strToInt(const char* start, const char* end) {
        std::string str(start, end);
        return strToInt(str);
    }

public:
    virtual StreamState process(ServerInterface &srvInterface, DataBuffer &input, InputState input_state) {
        // WARNING: This implementation is not trying for efficiency.
        // It is trying for simplicity, for demonstration purposes.

        size_t start = input.offset;
        const size_t end = input.size;

        do {
            bool found_newline = false;
            size_t numEnd = start;
            for (; numEnd < end; numEnd++) {
                if (input.buf[numEnd] < '0' || input.buf[numEnd] > '9') {
                    found_newline = true;
                    break;
                }
            }

            if (!found_newline) {
                input.offset = start;
                if (input_state == END_OF_FILE) {
                    // If we're at end-of-file,
                    // emit the last integer (if any) and return DONE.
                    if (start != end) {
                        writer->setInt(0, strToInt(input.buf + start, input.buf + numEnd));
                        writer->next();
                    }
                    return DONE;
                } else {
                    // Otherwise, we need more data.
                    return INPUT_NEEDED;
                }
            }

            writer->setInt(0, strToInt(input.buf + start, input.buf + numEnd));
            writer->next();

            start = numEnd + 1;
        } while (true);
    }
};

class BasicIntegerParserFactory : public ParserFactory {
public:
    virtual void plan(ServerInterface &srvInterface,
            PerColumnParamReader &perColumnParamReader,
            PlanContext &planCtxt) {
        /* Check parameters */
        // TODO: Figure out what parameters I should have; then make sure I have them

        /* Populate planData */
        // Nothing to do here
    }

    virtual UDParser* prepare(ServerInterface &srvInterface,
            PerColumnParamReader &perColumnParamReader,
            PlanContext &planCtxt,
            const SizedColumnTypes &returnType) {

        return vt_createFuncObject<BasicIntegerParser>(srvInterface.allocator);
    }

    virtual void getParserReturnType(ServerInterface &srvInterface,
            PerColumnParamReader &perColumnParamReader,
            PlanContext &planCtxt,
            const SizedColumnTypes &argTypes,
            SizedColumnTypes &returnType) {
        // We only and always have a single integer column
        returnType.addInt(argTypes.getColumnName(0));
    }
};
RegisterFactory(BasicIntegerParserFactory);
