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
 * DelimitedParserFrameworkExample
 *
 * An example of a simple delimited parser.
 * Designed to break the various stages of delimited parsing up into
 * clean pieces, so that it's easy to modify this parser to handle
 * special cases and scenarios.
 */
template <class StringParsersImpl>
class DelimitedParserFrameworkExample : public ContinuousUDParser {
public:
    DelimitedParserFrameworkExample(char delimiter = ',', char recordTerminator = '\n', std::vector<std::string> formatStrings = std::vector<std::string>(), bool enforceNotNulls = false) :
        currentRecordSize(0),
        delimiter(delimiter), recordTerminator(recordTerminator),
        formatStrings(formatStrings),
        enforceNotNulls(enforceNotNulls) {}

private:
    // Keep a copy of the information about each column.
    // Note that Vertica doesn't let us safely keep a reference to
    // the internal copy of this data structure that it shows us.
    // But keeping a copy is fine.
    SizedColumnTypes colInfo;

    // Size (in bytes) of the current record (row) that we're looking at.
    size_t currentRecordSize;

    // Start-position and size of the current column, within the current row,
    // relative to getDataPtr().
    // We read in each row one row at a time,
    size_t currentColPosition;
    size_t currentColSize;

    // Configurable parsing parameters
    // Set by the constructor
    char delimiter;
    char recordTerminator;

    // Format strings
    std::vector<std::string> formatStrings;

    // For rejecting data
    bool enforceNotNulls;
    std::string rejectReason;


    // Start off reserving this many bytes when searching for the end of a record
    // Will reserve more as needed; but from a performance perspective it's
    // nice to not have to do so.
    static const size_t BASE_RESERVE_SIZE = 256;

    // An instance of the class containing the methods that we're
    // using to parse strings to the various relevant data types
    StringParsersImpl sp;

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
        char *ptr;

        // Our current position within the stream.
        // Kept around so that we can update ptr correctly after reserve()ing more data.
        size_t position = 0;

        do {
            // Get some (more) data
            reserved = cr.reserve(reservationRequest);

            // Position counter.  Not allowed to pass getDataPtr() + reserved.
            ptr = (char*)cr.getDataPtr() + position;

            // Keep reading until we hit EOF.
            // If we find the record terminator, we'll return out of the loop.
            // Very tight loop; very performance-sensitive.
            while (*ptr != recordTerminator && position < reserved) {
                ++ptr;
                ++position;
            }

            if (*ptr == recordTerminator) {
                currentRecordSize = position;
                return true;
            }

            reservationRequest *= 2;  // Request twice as much data next time
        } while (!cr.noMoreData());  // Stop if we run out of data;
                             // correctly handles files that aren't newline terminated
                             // and when we reach eof but haven't seeked there yet

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
        char *pos = (char*)cr.getDataPtr() + currentColPosition;
        currentColSize = 0;

        while (*pos != delimiter && *pos != recordTerminator
                && currentColSize + currentColPosition < currentRecordSize) {
            ++pos;
            ++currentColSize;
        }
        return (currentColSize + currentColPosition != currentRecordSize
                && *pos == delimiter);
    }

    /**
     * Given a field in string form (a pointer to the first character and
     * a length), submit that field to Vertica.
     * `colNum` is the column number from the input file; how many fields
     * it is into the current record.
     *
     * Ordinarily, this method will probably want to do some parsing and
     * conversion of the input string.  For now, though, this example
     * only supports char and varchar columns, and only outputs strings,
     * so no parsing is necessary.
     */
    bool handleField(size_t colNum, char* start, size_t len, bool hasPadding = false) {
        // Empty colums are null.
        if (len == 0) {
            if (enforceNotNulls) {
                const SizedColumnTypes::Properties &colProps = colInfo.getColumnProperties(colNum);
                if (!colProps.canBeNull) {
                    /* TODO: how to reject? */
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
     * Reset the various per-column state; start over with a new row
     */
    void initCols() {
        currentColPosition = 0;
        currentColSize = 0;
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
    virtual void run() {
        bool hasMoreData;
        bool areMoreColumns;
        bool rejected;

        do {
            rejected = false;

            // Fetch the next record
            hasMoreData = fetchNextRow();

            // Special case: ignore trailing newlines (record terminators) at
            // the end of files
            if (cr.isEof() && currentRecordSize == 0) {
                hasMoreData = false;
                break;
            }

            initCols();

            // Parse each column
            for (uint32_t col = 0; col < colInfo.getColumnCount(); col++) {
                // Get the data for the next column
                areMoreColumns = fetchNextColumn();

                // "areMoreColumns" indicates whether there are more columns to read.
                // The expression "col < colInfo.getColumnCount() - 1" indicates whether we have
                // more output columns left to populate.
                // We should reach the last column at the same time we reach the
                // last input column.  If not, this row is invalid; reject it.
                if (areMoreColumns != (col < colInfo.getColumnCount() - 1)) {
                    rejectRecord("Wrong number of columns!");
                    rejected = true;
                    break;  // Don't bother parsing this row.
                }

                // Do something with that column's data.
                // Typically involves writing it to our StreamWriter,
                // in which case we have to know the input column number.
                if (!handleField(col, (char*)cr.getDataPtr() + currentColPosition, currentColSize, !cr.isEof())) {
                    std::stringstream ss;
                    ss << "Parse error in column " << col + 1;  // Convert 0-indexing to 1-indexing
                    if (!rejectReason.empty()) {
                        ss << ": " << rejectReason;
                        rejectReason.clear();
                    }
                    rejectRecord(ss.str());
                    rejected = true;
                    break;  // Don't bother parsing this row.
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

    virtual void initialize(ServerInterface &srvInterface, SizedColumnTypes &returnType);
};

template <class StringParsersImpl>
void DelimitedParserFrameworkExample<StringParsersImpl>::initialize(ServerInterface &srvInterface, SizedColumnTypes &returnType) {
    colInfo = returnType;
}

template <>
void DelimitedParserFrameworkExample<FormattedStringParsers>::initialize(ServerInterface &srvInterface, SizedColumnTypes &returnType) {
    colInfo = returnType;
    if (formatStrings.size() != returnType.getColumnCount()) {
        formatStrings.resize(returnType.getColumnCount(), "");
    }
    sp.setFormats(formatStrings);
}

template <>
void DelimitedParserFrameworkExample<VFormattedStringParsers>::initialize(ServerInterface &srvInterface, SizedColumnTypes &returnType) {
    colInfo = returnType;
    if (formatStrings.size() != returnType.getColumnCount()) {
        formatStrings.resize(returnType.getColumnCount(), "");
    }
    sp.setFormats(formatStrings);
}

template <class StringParsersImpl>
class GenericDelimitedParserFrameworkExampleFactory : public ParserFactory {
public:
    virtual void plan(ServerInterface &srvInterface,
            PerColumnParamReader &perColumnParamReader,
            PlanContext &planCtxt) {
        /* Check parameters */
        // TODO: Figure out what parameters I should have; then make sure I have them

        /* Populate planData */
        // Nothing to do here
    }

    // todo: return an appropriate udchunker
    virtual UDChunker* prepareChunker(ServerInterface &srvInterface,
                                      PerColumnParamReader &perColumnParamReader,
                                      PlanContext &planCtxt,
                                      const SizedColumnTypes &returnType)
    {
        // Defaults.
        std::string delimiter(","), record_terminator("\n");
        std::vector<std::string> formatStrings;

        //return NULL;
        return vt_createFuncObject<ExampleDelimitedUDChunker>
               (srvInterface.allocator,
                delimiter[0],
                record_terminator[0],
                formatStrings
            );
    }

    virtual UDParser* prepare(ServerInterface &srvInterface,
            PerColumnParamReader &perColumnParamReader,
            PlanContext &planCtxt,
            const SizedColumnTypes &returnType)
    {
        ParamReader args(srvInterface.getParamReader());
 
        // Defaults.
        std::string delimiter(","), record_terminator("\n");
        std::vector<std::string> formatStrings;

        // Args.
        if (args.containsParameter("delimiter"))
            delimiter = args.getStringRef("delimiter").str();
        if (args.containsParameter("record_terminator"))
            record_terminator = args.getStringRef("record_terminator").str();

        // Validate.
        if (delimiter.size()!=1) {
            vt_report_error(0, "Invalid delimiter \"%s\": single character required",
                            delimiter.c_str());
        }
        if (record_terminator.size()!=1) {
            vt_report_error(1, "Invalid record_terminator \"%s\": single character required",
                            record_terminator.c_str());
        }

        // Extract the "format" argument.
        // Default to the global setting, but let any per-column settings override for that column.
        if (args.containsParameter("format"))
            formatStrings.resize(returnType.getColumnCount(), args.getStringRef("format").str());
        else
            formatStrings.resize(returnType.getColumnCount(), "");

        bool enforceNotNulls = false;
        if (args.containsParameter("enforce_not_null_constraints")) {
            enforceNotNulls = args.getBoolRef("enforce_not_null_constraints");
        }

        for (size_t i = 0; i < returnType.getColumnCount(); i++) {
            const std::string &cname(returnType.getColumnName(i));
            if (perColumnParamReader.containsColumn(cname)) {
                ParamReader &colArgs = perColumnParamReader.getColumnParamReader(cname);
                if (colArgs.containsParameter("format")) {
                    formatStrings[i] = colArgs.getStringRef("format").str();
                }
            }
        }

        return vt_createFuncObject<DelimitedParserFrameworkExample<StringParsersImpl> >
               (srvInterface.allocator,
                delimiter[0],
                record_terminator[0],
                formatStrings,
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
        parameterTypes.addVarchar(256, "format");
        parameterTypes.addBool("enforce_not_null_constraints");
    }
};

typedef GenericDelimitedParserFrameworkExampleFactory<StringParsers> DelimitedParserFrameworkExampleFactory;
RegisterFactory(DelimitedParserFrameworkExampleFactory);

typedef GenericDelimitedParserFrameworkExampleFactory<FormattedStringParsers> FormattedDelimitedParserFrameworkExampleFactory;
RegisterFactory(FormattedDelimitedParserFrameworkExampleFactory);

typedef GenericDelimitedParserFrameworkExampleFactory<VFormattedStringParsers> VFormattedDelimitedParserFrameworkExampleFactory;
RegisterFactory(VFormattedDelimitedParserFrameworkExampleFactory);
