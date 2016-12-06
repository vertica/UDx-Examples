/*Copyright (c) 2005 - 2016 Hewlett Packard Enterprise Development LP  -*- C++ -*-*/

#include "Vertica.h"
#include "ContinuousUDParser.h"

using namespace Vertica;

#include <string>
#include <sstream>

/**
 * Basic Integer parser
 * Parses a string of integers separated by non-numeric characters.
 * Uses the ContinuousUDParser API provided with the examples,
 * as a wrapper on top of the built-in Vertica SDK API.
 */
class BasicIntegerParser : public ContinuousUDParser {
private:
    // campaign for the conservation of keystrokes
    char *ptr(size_t pos = 0) { return ((char*)cr.getDataPtr()) + pos; }

    vint strToInt(const std::string &str) {
        vint retVal;
        std::stringstream ss;
        ss << str;
        ss >> retVal;
        return retVal;
    }
public:
    virtual void run() {
        // WARNING: This implementation is not trying for efficiency.
        // It is trying to exercise ContinuousUDParser,
        // and to be quick to implement.

        // This parser assumes a single-column input, and
        // a stream of ASCII integers split by non-numeric characters.
        size_t pos = 0;
        size_t reserved = cr.reserve(pos+1);
        while (!cr.isEof() || reserved == pos+1) {
            while (reserved == pos+1 && (*ptr(pos) >= '0' && *ptr(pos) <= '9')) {
                pos++;
                reserved = cr.reserve(pos+1);
            }
            std::string st(ptr(), pos);
            writer->setInt(0, strToInt(st));
            writer->next();
            while (reserved == pos+1 && !(*ptr(pos) >= '0' && *ptr(pos) <= '9')) {
                pos++;
                reserved = cr.reserve(pos+1);
            }
            cr.seek(pos);
            pos = 0;
            reserved = cr.reserve(pos+1);
        }
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
