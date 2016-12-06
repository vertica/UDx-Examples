/* Copyright (c) 2005 - 2016 Hewlett Packard Enterprise Development LP  -*- C++ -*-*/


#include "ContinuousUDParser.h"
#include "StringParsers.h"
#include <pthread.h>

#include <boost/tokenizer.hpp>

using namespace Vertica;
typedef std::vector<std::string> Strings;
typedef std::map<std::string,std::string> KVMap;

// Internal wrapper variable
class TSSManager
{
public:
    pthread_key_t tss_key;
    TSSManager() { pthread_key_create(&tss_key, NULL); }
    ~TSSManager() { pthread_key_delete(tss_key); }
};

/**
 * Parses csv format input.
 */
class CsvParser : public ContinuousUDParser
{
public:

    // Ctor.
    CsvParser(char delimiter, char terminator, char escape, char enclosed_by, bool trailingNulls,
              const std::string &null, const Strings &colFormats, const Strings &colNullVals) :
        terminator(terminator), separator(escape, delimiter, enclosed_by), trailingNulls(trailingNulls),
        null(null), colFormats(colFormats), colNullVals(colNullVals)
    {}

    // Process all of the input. Called once. ContinuousUDParser hides the details
    // about filling input buffers and flushing output buffers.
    virtual void run()
    {
        // Re-use buffer; memory allocated in pool is not release until execution stops.
        TSSBuffer tssBuffer(this);

        // Parse input, until there is no more.
        while (!cr.isEof()) {
            Line line = readLine();
            if (writeRecord(line)) {
                writer->next();
                recordsAcceptedInBatch++;    // number of rows.
            }
            // Seek past line.
            cr.seek(line.length);
            // Seek past record terminator.
            if (!cr.isEof()) cr.seek(1);
        }
    }

    // Initialization.
    virtual void initialize(ServerInterface &srvInterface, SizedColumnTypes &returnType)
    {
        // Same as what we were told in getParserReturnType.
        colInfo = returnType;
    }

    // Gets allocator.
    VTAllocator *getAllocator() { return getServerInterface().allocator; }

private:
    // Type specifier for each output column.
    SizedColumnTypes colInfo;

    // Record terminator.
    const char terminator;

    // Csv separator. Specifies escape, column delimiter and enclosed_by.
    typedef boost::escaped_list_separator<char> Separator;
    Separator separator;

    // Allow lines with too few columns, pad with NULLs.
    bool trailingNulls;

    // Default null value (per column overrides this).
    std::string null;

    // Format strings.
    Strings colFormats;

    // Null value (empty string by default).
    Strings colNullVals;

    // A buffer that can grow (within bounds). Uses allocator on the ServerInterface.
    struct Buffer
    {
        char *dat;
        size_t capacity;
        const size_t maxCapacity;
        CsvParser *parser;

        // Ctor.
        Buffer(CsvParser *parser, const size_t m = 1024LL * 1024LL * 1024LL) :
            dat(NULL), capacity(0), maxCapacity(m), parser(parser)
        {}

        // Grow the buffer if needed; limited by maxCapacity.
        void grow(size_t newcap) {
            if (newcap > capacity) {
                if (newcap > maxCapacity) {
                    ereport(ERROR, (errmsg("Invalid buffer enlargement request size %lu",
                                           newcap),
                                    errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED)));
                }
                char *olddat = dat;
                size_t oldcap = capacity;
                capacity = newcap;
                dat = (char *)parser->getAllocator()->alloc(capacity);
                if (olddat) vsmemcpy(dat, olddat, oldcap);
            }
        }
    };

    // Thread specific buffer.
    struct TSSBuffer
    {
        static TSSManager TSSBuffer__manager;
        bool isOwner;

        // Ctor.
        TSSBuffer(CsvParser *parser = NULL) :
            isOwner(false)
        {
            if (parser) {
                // Set once.
                VIAssert(getBuffer()==NULL);
                isOwner = true;
                ::pthread_setspecific(TSSBuffer__manager.tss_key, new Buffer(parser));
            }
        }
        // Dtor.
        ~TSSBuffer()
        {
            if (isOwner) {
                ::pthread_setspecific(TSSBuffer__manager.tss_key, NULL);
                delete getBuffer();
            }
        }

        // Ways to get tss buffer.
        Buffer *getBuffer() const {
            return (Buffer *)::pthread_getspecific(TSSBuffer__manager.tss_key);
        }
        Buffer &operator*() const { return *getBuffer(); }
        Buffer *operator->() const { return getBuffer(); }
    };

    // A line of input. Provides an "iterator" interface to tokenizer.
    struct Line
    {
        const char *data;
        size_t length;

        // Ctor.
        Line(const char *data = NULL, size_t length = 0) :
            data(data), length(length)
        {}

        // Iterator.
        typedef const char * const_iterator;
        const_iterator begin() const { return data; }
        const_iterator end() const { return data + length; }
    };

    // A token. Re-uses buffer by stashing it in TSS (memory is allocated in a pool
    // and not reaped until execution is torn down).
    struct Token
    {
        // TSS pointer to buffer.
        TSSBuffer buffer;
        char *dat;
        size_t len, capacity;

        // Ctor.
        Token() : dat(buffer->dat), len(0), capacity(buffer->capacity)
        {}

        // Gets the data.
        char *data() const { return dat; }
        // Gets the length.
        size_t length() const { return len; }

        // Append a character.
        Token &operator+=(char v) {
            VIAssert(buffer.getBuffer());
            if (len+1 >= capacity) {
                buffer->grow(2 * std::max((size_t)1,capacity));
                dat = buffer->dat;
                capacity = buffer->capacity;
            }
            dat[len++] = v;
            dat[len] = '\0'; // Null terminated.
            return *this;
        }
    };

    // Read characters up to record terminator.
    Line readLine()
    {
        // Don't hold a pointer to return value of getDataPtr; it can change
        // when reserve is called.
        size_t pos = 0;
        while (!cr.isEof()) {
            const char *data = (char *)cr.getDataPtr();
            while (pos < cr.capacity()) {
                if (*(data+pos) == terminator) {
                    return Line(data, pos);
                }
                ++pos;
            }

            // Need more input?
            if (pos == cr.capacity()) {
                // Reserve one more (framework handles efficient resizing).
                size_t reserved = cr.reserve(pos+1);
                // Break if no more input available.
                if (pos == reserved) {
                    return Line(data, pos);
                }
            }
        }

        // EOF.
        return Line();
    }

    // Parse record from line and write it.
    bool writeRecord(const Line &line)
    {
        try {
            // String parser.
            FormattedStringParsers sp(colFormats);
            // Tokenizer.
            typedef boost::tokenizer<Separator, Line::const_iterator, Token> Tokenizer;
            Tokenizer tokenizer(line.begin(), line.end(), separator);
            unsigned iCol = 0; // column index
            for (Tokenizer::iterator i=tokenizer.begin(); i!=tokenizer.end(); ++i, ++iCol) {
                // Reject if extra columns.
                if (iCol == colInfo.getColumnCount()) {
                    rejectTooManyCols(line);
                    return false;
                }

                // Parse string; treats empty as NULL.
                VerticaType type = colInfo.getColumnType(iCol);
                const std::string nullVl = colNullVals[iCol];
                bool isNull = nullVl.length() == i->length() &&
                    ::strncmp(nullVl.data(), i->data(), nullVl.length()) == 0;
                if (isNull) {
                    writer->setNull(iCol);
                } else if (i->length() == 0) {
                    if (!type.isStringType()) {
                        rejectInvalidCol(line, *i, iCol);
                        return false;
                    }
                    // Empty string.
                    writer->getStringRef(iCol).copy(i->data(), i->length());
                } else {
                    bool ok = parseStringToType(i->data(), i->length(), iCol, type, writer, sp);
                    if (!ok) {
                        rejectInvalidCol(line, *i, iCol);
                        return false;
                    }
                }
            }

            // Too few columns? Ok if 1 column + empty=NULL or TRAILING NULLCOLS.
            if (iCol < colInfo.getColumnCount()) {
                if (iCol==0 && colNullVals[iCol].empty()) {
                    ; // Ok.
                } else if (!trailingNulls) {
                    rejectTooFewCols(line);
                    return false;
                }
                for ( ; iCol < colInfo.getColumnCount(); ++iCol) {
                    writer->setNull(iCol);
                }
            }

            return true;
        } catch (std::exception &e) {
            rejectInvalidLine(line, e.what());
        }

        return false;
    }

    void rejectTooFewCols(const Line &line)
    {
        std::ostringstream oss;
        oss << "Invalid line: fewer than " << colInfo.getColumnCount() << " columns";
        rejectGeneric(line, oss.str());
    }

    void rejectTooManyCols(const Line &line)
    {
        std::ostringstream oss;
        oss << "Invalid line: more than " << colInfo.getColumnCount() << " columns";
        rejectGeneric(line, oss.str());
    }

    void rejectInvalidCol(const Line &line, const Token &col, unsigned iCol)
    {
        std::ostringstream oss;
        VerticaType t(colInfo.getColumnType(iCol));
        std::string name(colInfo.getColumnName(iCol)),
            value(col.data(), col.length());
        oss << "Invalid column: "
            << "name=" << name << ", "
            << "index=" << iCol+1 << ", "
            << "type=" << t.getPrettyPrintStr() << ", "
            << "value=" << value.substr(0,64) << ((value.length()>64) ? "[...]" : "");
        rejectGeneric(line, oss.str());
    }

    void rejectInvalidLine(const Line &line, const char *reason)
    {
        std::ostringstream oss;
        oss << "Invalid line: " << reason;
        rejectGeneric(line, oss.str());
    }

    void rejectGeneric(const Line &line, const std::string &msg)
    {
        RejectedRecord rr(msg, line.data, line.length,
                          std::string(1, terminator)
                          );
        crej.reject(rr);
    }
};

class CsvParserFactory : public ParserFactory
{
public:
    virtual void plan(ServerInterface &srvInterface,
                      PerColumnParamReader &perColumnParamReader,
                      PlanContext &planCtxt)
    {}

    virtual UDParser* prepare(ServerInterface &srvInterface,
            PerColumnParamReader &perColumnParamReader,
            PlanContext &planCtxt,
            const SizedColumnTypes &returnType)
    {
        ParamReader params(srvInterface.getParamReader());
 
        // Defaults.
        std::string delimiter(","), terminator("\n"),
            escape("\\"), enclosed_by("\""), null;
        bool trailingNulls = false;

        // Params.
        if (params.containsParameter("delimiter"))
            delimiter = params.getStringRef("delimiter").str();
        if (params.containsParameter("record_terminator"))
            terminator = params.getStringRef("record_terminator").str();
        if (params.containsParameter("escape"))
            escape = params.getStringRef("escape").str();
        if (params.containsParameter("enclosed_by"))
            enclosed_by = params.getStringRef("enclosed_by").str();
        if (params.containsParameter("trailing_nullcols"))
            trailingNulls = params.getBoolRef("trailing_nullcols");
        if (params.containsParameter("null"))
            null = params.getStringRef("null").str();

        // Only per column.
        if (params.containsParameter("format")) {
            vt_report_error(0, "Parameter format can only be used as a column option");
        }

        // Per column params.
        Strings colFormats, colNullVals;
        colFormats.resize(returnType.getColumnCount(), "");
        colNullVals.resize(returnType.getColumnCount(), null);
        for (size_t i = 0; i < returnType.getColumnCount(); ++i) {
            const std::string &cname(returnType.getColumnName(i));
            if (perColumnParamReader.containsColumn(cname)) {
                ParamReader &colParams = perColumnParamReader.getColumnParamReader(cname);
                if (colParams.containsParameter("format"))
                    colFormats[i] = colParams.getStringRef("format").str();
                if (colParams.containsParameter("null"))
                    colNullVals[i] = colParams.getStringRef("null").str();
            }
        }
        
        
        // Create CsvParser; pass in params.
        return vt_createFuncObject<CsvParser>(
                                srvInterface.allocator,
                                delimiter[0],
                                terminator[0],
                                escape[0],
                                enclosed_by[0],
                                trailingNulls,
                                null,
                                colFormats,
                                colNullVals);
    }

    virtual void getParserReturnType(ServerInterface &srvInterface,
                                     PerColumnParamReader &perColumnParamReader,
                                     PlanContext &planCtxt,
                                     const SizedColumnTypes &colTypes,
                                     SizedColumnTypes &returnType)
    {
        // Types dictated by what the caller wants.
        returnType = colTypes;
    }

    virtual void getParameterType(ServerInterface &srvInterface,
                                  SizedColumnTypes &parameterTypes)
    {
        // Valid params.
        parameterTypes.addVarchar(1, "delimiter");
        parameterTypes.addVarchar(1, "record_terminator");
        parameterTypes.addVarchar(1, "escape");
        parameterTypes.addVarchar(1, "enclosed_by");
        parameterTypes.addBool("trailing_nullcols");
        parameterTypes.addVarchar(65000,"null");
        parameterTypes.addVarchar(65000,"format");
    }

};
RegisterFactory(CsvParserFactory);

// Initialize tss manager.
TSSManager CsvParser::TSSBuffer::TSSBuffer__manager;
