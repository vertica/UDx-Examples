/* Copyright (c) 2005 - 2015 Hewlett Packard Enterprise Development LP  -*- C++ -*-*/


#include "Vertica.h"
#include "StringParsers.h"
#include "csv.h"

using namespace Vertica;

// Note, the class template is mostly for demonstration purposes,
// so that the same class can use each of two string-parsers.
// Custom parsers can also just pick a string-parser to use.

/**
 * A parser that parses something approximating the "official" CSV format
 * as defined in IETF RFC-4180:  <http://tools.ietf.org/html/rfc4180>
 * Oddly enough, many "CSV" files don't actually conform to this standard
 * for one reason or another.  But for sources that do, this parser should
 * be able to handle the data.
 * Note that the CSV format does not specify how to handle different
 * data types; it is entirely a string-based format.
 * So we just use standard parsers based on the corresponding column type.
 */
template <class StringParsersImpl>
class LibCSVParser : public UDParser {
public:
    LibCSVParser() : colNum(0), currSrvInterface(0) {}

    // Keep a copy of the information about each column.
    // Note that Vertica doesn't let us safely keep a reference to
    // the internal copy of this data structure that it shows us.
    // But keeping a copy is fine.
    SizedColumnTypes colInfo;

    // An instance of the class containing the methods that we're
    // using to parse strings to the various relevant data types
    StringParsersImpl sp;

    /// Current column index
    size_t colNum;

    /// Parsing state for libcsv
    struct csv_parser parser;

    std::string bad_field;

    // Format strings
    std::vector<std::string> formatStrings;

    ServerInterface *currSrvInterface;

    /**
     * Given a field in string form (a pointer to the first character and
     * a length), submit that field to Vertica.
     * `colNum` is the column number from the input file; how many fields
     * it is into the current record.
     */
    bool handleField(size_t colNum, char* start, size_t len) {
        if (colNum >= colInfo.getColumnCount()) {
            // Ignore column overflow
            return false;
        }
        // Empty colums are null.
        if (len==0) {
            writer->setNull(colNum);
            return true;
        } else {
            return parseStringToType(start, len, colNum, colInfo.getColumnType(colNum), writer, sp);
        }
    }

    static void handle_record(void *data, size_t len, void *p) {
        if (!static_cast<LibCSVParser*>(p)->handleField(static_cast<LibCSVParser*>(p)->colNum++, (char*)data, len)) {
            static_cast<LibCSVParser*>(p)->bad_field = std::string((char*)data, len);
        }
    }

    static void handle_end_of_row(int c, void *ptr) {
        // Ignore 'c' (the terminating character); trust that it's correct
        LibCSVParser *p = static_cast<LibCSVParser*>(ptr);
        p->colNum = 0;

        if (p->bad_field.empty()) {
            p->writer->next();
        } else {
            // libcsv doesn't give us the whole row to reject.
            // So just write to the log.
            // TODO: Come up with something more clever.
            if (p->currSrvInterface) {
                p->currSrvInterface->log("Invalid CSV field value: '%s'  Row skipped.",
                                        p->bad_field.c_str());
            }
            p->bad_field = "";
        }
    }

    virtual StreamState process(ServerInterface &srvInterface, DataBuffer &input, InputState input_state) {
        currSrvInterface = &srvInterface;

        size_t processed;
        while ((processed = csv_parse(&parser, input.buf + input.offset, input.size - input.offset,
                handle_record, handle_end_of_row, this)) > 0) {
            input.offset += processed;
        }

        if (input_state == END_OF_FILE && input.size == input.offset) {
            csv_fini(&parser, handle_record, handle_end_of_row, this);
            currSrvInterface = NULL;
            return DONE;
        }

        currSrvInterface = NULL;
        return INPUT_NEEDED;
    }

    virtual void setup(ServerInterface &srvInterface, SizedColumnTypes &returnType);
    virtual void destroy(ServerInterface &srvInterface, SizedColumnTypes &returnType) {
        csv_free(&parser);
    }
};

template <class StringParsersImpl>
void LibCSVParser<StringParsersImpl>::setup(ServerInterface &srvInterface, SizedColumnTypes &returnType) {
    csv_init(&parser, CSV_APPEND_NULL);
    colInfo = returnType;
}

template <>
void LibCSVParser<FormattedStringParsers>::setup(ServerInterface &srvInterface, SizedColumnTypes &returnType) {
    csv_init(&parser, CSV_APPEND_NULL);
    colInfo = returnType;
    if (formatStrings.size() != returnType.getColumnCount()) {
        formatStrings.resize(returnType.getColumnCount(), "");
    }
    sp.setFormats(formatStrings);
}

template <class StringParsersImpl>
class LibCSVParserFactoryTmpl : public ParserFactory {
public:
    virtual void plan(ServerInterface &srvInterface,
            PerColumnParamReader &perColumnParamReader,
            PlanContext &planCtxt) {}

    virtual UDParser* prepare(ServerInterface &srvInterface,
            PerColumnParamReader &perColumnParamReader,
            PlanContext &planCtxt,
            const SizedColumnTypes &returnType)
    {
        return vt_createFuncObject<LibCSVParser<StringParsersImpl> >(srvInterface.allocator);
    }
};

typedef LibCSVParserFactoryTmpl<StringParsers> LibCSVParserFactory;
RegisterFactory(LibCSVParserFactory);

typedef LibCSVParserFactoryTmpl<FormattedStringParsers> FormattedLibCSVParserFactory;
RegisterFactory(FormattedLibCSVParserFactory);
