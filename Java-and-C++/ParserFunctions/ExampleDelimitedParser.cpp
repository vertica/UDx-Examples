/* Copyright (c) 2005 - 2016 Hewlett Packard Enterprise Development LP  -*- C++ -*-*/


#include "Vertica.h"
#include "ContinuousUDParser.h"
#include "StringParsers.h"
#include "ExampleDelimitedChunker.h"

using namespace Vertica;

#include <string>
#include <vector>
#include <sstream>
#include <iostream>


/**
 * DelimitedParserFramework
 *
 * A framework for writing simple delimited parsers.
 * Its design breaks the various stages of delimited parsing up into
 * clean pieces, so that it's easy to modify this parser to handle
 * special cases and scenarios.
 *
 * "StringParserImpl" is a class of objects which defines how strings
 * are parsed into columns of a given type.  See the function "parseStringToType"
 * in StringParsers.h for how this is used.
 *
 * Example uses of this framework occur later in this file.
 */
template <class StringParserImpl>
class DelimitedParserFramework : public ContinuousUDParser {
public:
    DelimitedParserFramework(char delimiter, char recordTerminator,
            const SizedColumnTypes &colInfo, StringParserImpl parseImpl,
            bool enforceNotNulls = false) :
        colInfo(colInfo), sp(parseImpl),
        currentRecordSize(0),
        delimiter(delimiter), recordTerminator(recordTerminator),
        enforceNotNulls(enforceNotNulls) {}

private:
    // Keep a copy of the information about each column.
    // Note that Vertica doesn't let us safely keep a reference to
    // the internal copy of this data structure that it shows us.
    // But keeping a copy is fine.
    const SizedColumnTypes colInfo;

    // An instance of the class containing the methods that we're
    // using to parse strings to the various relevant data types
    StringParserImpl sp;

    // Size (in bytes) of the current record (row) that we're looking at.
    size_t currentRecordSize;

    // Start-position and size of the current column, within the current row,
    // relative to getDataPtr().
    // We read in each row one column at a time.
    size_t currentColPosition;
    size_t currentColSize;

    // Configurable parsing parameters
    char delimiter;
    char recordTerminator;

    // For rejecting data
    bool enforceNotNulls;
    std::string rejectReason;


    // Start off reserving this many bytes when searching for the end of a record
    // Will reserve more as needed; but from a performance perspective it's
    // nice to not have to do so.
    static const size_t BASE_RESERVE_SIZE = 256;

    /**
     * Make sure (via reserve()) that the full upcoming row is in memory.
     * Assumes that getDataPtr() points at the start of the upcoming row.
     * (This is guaranteed by run(), prior to calling fetchNextRow().)
     *
     * Returns true if we stopped due to a record terminator;
     * false if we stopped due to EOF.
     */
    bool fetchNextRow() {
        // Amount of data we have to work with
        size_t reserved;

        // Amount of data that we've requested to work with.
        // Equal to `reserved` after calling reserve(), except in case of end-of-file.
        size_t reservationRequest = BASE_RESERVE_SIZE;

        // Pointer into the middle of our current data buffer.
        // Must always be betweeen getDataPtr() and getDataPtr() + reserved.
        const char *ptr;

        // Our current position within the stream.
        // Kept around so that we can update ptr correctly after reserve()ing more data.
        size_t position = 0;

        do {
            // Get some (more) data
            reserved = cr.reserve(reservationRequest);

            // Position counter.  Not allowed to pass getDataPtr() + reserved.
            ptr = static_cast<const char *>(cr.getDataPtr()) + position;

            // Keep reading until we hit EOF.
            // If we find the record terminator, we'll return out of the loop.
            // Very tight loop; very performance-sensitive.
            while (position < reserved && *ptr != recordTerminator) {
                ++ptr;
                ++position;
            }

            if (position < reserved && *ptr == recordTerminator) {
                currentRecordSize = position;
                return true;
            }

            reservationRequest *= 2;  // Request twice as much data next time

        // Stop if no more data can be read from the input source (we may not have seeked there yet)
        } while (!cr.noMoreData());

        currentRecordSize = position;
        return false;
    }


    /**
     * Fetch the next column.
     * Returns false if we stopped due to hitting the record terminator;
     * true if we stopped due to hitting a column delimiter.
     * Should depend on (and/or set) the values:
     *
     * - currentColPosition -- The number of bytes from getDataPtr() to
     *   the start of the current column field.  Should not be set.
     *
     * - currentColSize -- Should be set to the distance from the start
     *   of the column to the last non-record-terminator character
     */
    bool fetchNextColumn() {
        // fetchNextRow() has guaranteed that we can read until the next
        // delimiter or the record terminator, whichever comes first.
        // So this can be a very tight loop:
        // Just scan forward until we hit one of the two.
        const char *pos = static_cast<const char *>(cr.getDataPtr()) + currentColPosition;
        currentColSize = 0;

        while (currentColSize + currentColPosition < currentRecordSize
                && *pos != delimiter && *pos != recordTerminator) {
            ++pos;
            ++currentColSize;
        }
        return (currentColSize + currentColPosition < currentRecordSize
                && *pos == delimiter);
    }

    /**
     * Given a field in string form (a pointer to the first character and
     * a length), submit that field to Vertica.
     * `colNum` is the column number from the input file; how many fields
     * it is into the current record.
     *
     * Our "StringParserImpl" object will be used to transform this string
     * into a value of the right type.
     *
     * Returns true if a value was correctly parsed, and false if the
     * record should be rejected.
     */
    bool handleField(size_t colNum, char *start, size_t len, bool hasPadding = false) {
        // Empty colums are null.
        if (len == 0) {
            if (enforceNotNulls) {
                const SizedColumnTypes::Properties &colProps = colInfo.getColumnProperties(colNum);
                if (!colProps.canBeNull) {
                    rejectReason = "NULL value for NOT NULL column";
                    return false;
                }
            }
            writer->setNull(colNum);
            return true;
        } else {
            NullTerminatedString str(start, len, false, hasPadding);
            return parseStringToType(str.ptr(), str.size(), colNum, colInfo.getColumnType(colNum), writer, sp);
        }
    }

    /**
     * Advance to the next column
     */
    void advanceCol() {
        currentColPosition += currentColSize + 1;
    }

    void rejectRecord(const std::string &reason) {
        RejectedRecord rr(reason, (char *)cr.getDataPtr(), currentRecordSize,
                          std::string(1, recordTerminator));
        crej.reject(rr);
    }

public:
    virtual void initialize(ServerInterface &srvInterface, SizedColumnTypes &colTypes) {}
    virtual void deinitialize(ServerInterface &srvInterface, SizedColumnTypes &colTypes) {}

    virtual void run() {
        bool hasMoreData;

        do {
            bool rejected = false;

            // Fetch the next record
            hasMoreData = fetchNextRow();

            // Special case: ignore trailing newlines (record terminators) at
            // the end of files
            if (cr.isEof() && currentRecordSize == 0) {
                hasMoreData = false;
                break;
            }

            // Reset column positions
            currentColPosition = 0;
            currentColSize = 0;

            // Parse each column
            for (uint32_t col = 0; col < colInfo.getColumnCount(); col++) {
                // Get the data for the next column
                const bool areMoreColumns = fetchNextColumn();

                // If we are expecting another column but didn't find one, then
                // this row is invalid; reject it.
                if (areMoreColumns != (col < colInfo.getColumnCount() - 1)) {
                    rejectRecord("Wrong number of columns!");
                    rejected = true;
                    break;  // Don't bother parsing this row.
                }

                // Do something with that column's data.
                // Typically involves writing it to our StreamWriter,
                // in which case we have to know the input column number.
                if (!handleField(col, static_cast<char *>(cr.getDataPtr()) + currentColPosition,
                            currentColSize, !cr.isEof())) {
                    std::stringstream ss;
                    ss << "Parse error in column " << col + 1;  // Convert 0-indexing to 1-indexing
                    if (!rejectReason.empty()) {
                        ss << ": " << rejectReason;
                        rejectReason.clear();
                    }
                    rejectRecord(ss.str());
                    rejected = true;
                    break;
                }

                advanceCol();
            }

            // Seek past the current record.
            // currentRecordSize points to the end of the record not counting the
            // record terminator.  But we want to seek over the record terminator too.
            cr.seek(currentRecordSize + 1);

            // If we didn't reject the row, emit it
            if (!rejected) {
                writer->next();
            }
        } while (hasMoreData);
    }
};

template <class StringParserImpl>
class DelimitedParserFrameworkFactory : public ParserFactory {
public:
    virtual bool isParserApportionable() {
        // this parser does not know how to handle apportioned stream states
        return false;
    }
    virtual bool isChunkerApportionable(ServerInterface &srvInterface) {
        ParamReader params = srvInterface.getParamReader();
        if (params.containsParameter("disable_chunker") && params.getBoolRef("disable_chunker")) {
            return false;
        } else {
            return true;
        }
    }

    virtual void plan(ServerInterface &srvInterface,
            PerColumnParamReader &perColumnParamReader,
            PlanContext &planCtxt) {
        // Validate parameters
        ParamReader args(srvInterface.getParamReader());
        if (args.containsParameter("delimiter")) {
            std::string delimiter = args.getStringRef("delimiter").str();
            if (delimiter.size() != 1) {
                vt_report_error(0, "Invalid delimiter \"%s\": single character required",
                                delimiter.c_str());
            }
        }
        if (args.containsParameter("record_terminator")) {
            std::string recordTerminator = args.getStringRef("record_terminator").str();
            if (recordTerminator.size() != 1) {
                vt_report_error(1, "Invalid record_terminator \"%s\": single character required",
                        recordTerminator.c_str());
            }
        }
    }

    virtual UDChunker* prepareChunker(ServerInterface &srvInterface,
                                      PerColumnParamReader &perColumnParamReader,
                                      PlanContext &planCtxt,
                                      const SizedColumnTypes &colTypes)
    {
        ParamReader params = srvInterface.getParamReader();
        if (params.containsParameter("disable_chunker") && params.getBoolRef("disable_chunker")) {
            return NULL;
        }

        std::string recordTerminator("\n");

        ParamReader args(srvInterface.getParamReader());
        if (args.containsParameter("record_terminator")) {
            recordTerminator = args.getStringRef("record_terminator").str();
        }

        return vt_createFuncObject<ExampleDelimitedUDChunker>(srvInterface.allocator,
                recordTerminator[0]);
    }

    virtual StringParserImpl createStringParser(ServerInterface &srvInterface,
            PerColumnParamReader &perColumnParams,
            PlanContext &planCtx,
            const SizedColumnTypes &colTypes) const {
        return StringParserImpl();
    }

    virtual UDParser* prepare(ServerInterface &srvInterface,
            PerColumnParamReader &perColumnParamReader,
            PlanContext &planCtxt,
            const SizedColumnTypes &colTypes)
    {
        ParamReader args(srvInterface.getParamReader());
 
        // Defaults.
        std::string delimiter(","), record_terminator("\n");

        // Args (already validated in plan()).
        if (args.containsParameter("delimiter")) {
            delimiter = args.getStringRef("delimiter").str();
        }
        if (args.containsParameter("record_terminator")) {
            record_terminator = args.getStringRef("record_terminator").str();
        }

        bool enforceNotNulls = false;
        if (args.containsParameter("enforce_not_null_constraints")) {
            enforceNotNulls = args.getBoolRef("enforce_not_null_constraints");
        }

        return vt_createFuncObject<DelimitedParserFramework<StringParserImpl> >
               (srvInterface.allocator,
                delimiter[0],
                record_terminator[0],
                colTypes,
                createStringParser(srvInterface, perColumnParamReader, planCtxt, colTypes),
                enforceNotNulls
            );
    }

    virtual void getParserReturnType(ServerInterface &srvInterface,
            PerColumnParamReader &perColumnParamReader,
            PlanContext &planCtxt,
            const SizedColumnTypes &argTypes,
            SizedColumnTypes &returnType)
    {
        returnType = argTypes;
    }

    virtual void getParameterType(ServerInterface &srvInterface,
                                  SizedColumnTypes &parameterTypes) {
        parameterTypes.addVarchar(1, "delimiter");
        parameterTypes.addVarchar(1, "record_terminator");
        parameterTypes.addBool("enforce_not_null_constraints");
        parameterTypes.addBool("disable_chunker");
    }
};

/**
 * Basic delimited parser
 */
typedef DelimitedParserFrameworkFactory<StringParsers> DelimitedParserExampleFactory;
RegisterFactory(DelimitedParserExampleFactory);




/**
 * This delimited parser allows for specifying format strings which specify
 * the interpretation of various date/time types.
 *
 * This extends the above parser factory with an additional "format" argument.
 * The "StringParserImpl" accepts a vector of format strings (but is still
 * generic in terms of how it interprets those format strings).
 */
template <class FormattedStringParserImpl>
class FormattedDelimitedParserFrameworkFactory : public DelimitedParserFrameworkFactory<FormattedStringParserImpl> {
    virtual void getParameterType(ServerInterface &srvInterface, SizedColumnTypes &paramTypes) {
        DelimitedParserFrameworkFactory<FormattedStringParserImpl>::getParameterType(srvInterface, paramTypes);

        paramTypes.addVarchar(256, "format");
    }

    virtual FormattedStringParserImpl createStringParser(
            ServerInterface &srvInterface,
            PerColumnParamReader &perColumnParams,
            PlanContext &planCtx,
            const SizedColumnTypes &colTypes) const {
        std::vector<std::string> formatStrings;

        /*
         * Extract the "format" argument.
         * Default to the global setting.  We will override with per-column settings below
         */
        ParamReader args(srvInterface.getParamReader());
        if (args.containsParameter("format")) {
            formatStrings.resize(colTypes.getColumnCount(), args.getStringRef("format").str());
        } else {
            formatStrings.resize(colTypes.getColumnCount(), "");
        }

        /*
         * Extract per-column format specifications
         */
        for (size_t i = 0; i < colTypes.getColumnCount(); i++) {
            const std::string &cname(colTypes.getColumnName(i));
            if (perColumnParams.containsColumn(cname)) {
                ParamReader &colArgs = perColumnParams.getColumnParamReader(cname);
                if (colArgs.containsParameter("format")) {
                    formatStrings[i] = colArgs.getStringRef("format").str();
                }
            }
        }

        return FormattedStringParserImpl(formatStrings);
    }
};

/**
 * Format string parser which uses standard library functions to interpret strings
 */
typedef FormattedDelimitedParserFrameworkFactory<FormattedStringParsers> FormattedDelimitedParserExampleFactory;
RegisterFactory(FormattedDelimitedParserExampleFactory);

/**
 * Format string parser which uses Vertica library functions to interpret strings
 */
typedef FormattedDelimitedParserFrameworkFactory<VFormattedStringParsers> VFormattedDelimitedParserExampleFactory;
RegisterFactory(VFormattedDelimitedParserExampleFactory);
