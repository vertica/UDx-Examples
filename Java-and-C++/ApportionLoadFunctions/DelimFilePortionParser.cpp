/* Copyright (c) 2005 - 2015 Hewlett Packard Enterprise Development LP  -*- C++ -*-*/

#include "Vertica.h"
#include "ContinuousUDParser.h"
#include "StringParsers.h"

using namespace Vertica;

#include <string>
#include <vector>
#include <sstream>
#include <iostream>
using namespace std;


/**
 * DelimFilePortionParser
 *
 * An example of a simple delimited parser.
 * Designed to break the various stages of delimited parsing up into
 * clean pieces, so that it's easy to modify this parser to handle
 * special cases and scenarios.
 */
template <class StringParsersImpl>
class DelimFilePortionParser : public ContinuousUDParser {
public:
    DelimFilePortionParser(char delimiter = ',', char recordTerminator = '\n', std::vector<std::string> formatStrings = std::vector<std::string>(), bool v = false)
        : currentRecordSize(0), row_count(0), delimiter(delimiter), recordTerminator(recordTerminator), formatStrings(formatStrings) {this->isParserApportionable = v;}

private:
    // Keep a copy of the information about each column.
    // Note that Vertica doesn't let us safely keep a reference to
    // the internal copy of this data structure that it shows us.
    // But keeping a copy is fine.
    SizedColumnTypes colInfo;

    // Size (in bytes) of the current record (row) that we're looking at.
    size_t currentRecordSize;

    size_t row_count;

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
     * false if we stopped due to EOF or end of portion.
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

        // Our current pos??ition within the stream.
        // Kept around so that we can update ptr correctly after reserve()ing more data.
        size_t position = 0;

        do {
            // Get some (more) data
            reserved = cr.reserve(reservationRequest);
            //getServerInterface().log("fetchNextRow: asking reserve() for %zu bytes, actually got %zu bytes, state = %d, stream_state = %d", reservationRequest, reserved, *cr.state, cr.stream_state);

            // Position counter.  Not allowed to pass getDataPtr() + reserved.
            ptr = (char*)cr.getDataPtr() + position;

            // Keep reading until we hit EOF.
            // If we find the record terminator, we'll return out of the loop.
            // Very tight loop; very performance-sensitive.
            while (position < reserved && *ptr != recordTerminator) {
                ++ptr;
                ++position;
            }

            if (position != reserved
                    && *ptr == recordTerminator) { // Note: don't use what you haven't reserved yet!!
                currentRecordSize = position;
                return true;
            }

            reservationRequest *= 2;  // Request twice as much data next time
        } while (!cr.noMoreData() && !cr.isPortionEnd());  // Stop if we run out of data;
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
     * Ordinarily, this method will probably want to do some parsing and
     * conversion of the input string.  For now, though, this example
     * only supports char and varchar columns, and only outputs strings,
     * so no parsing is necessary.
     */
    bool handleField(size_t colNum, char* start, size_t len, bool hasPadding = false) {
        // Empty colums are null.
        if (len==0) {
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
        getServerInterface().log("rejectRecord(): rejected len = %zu, record str= %.*s", rr.length, (int) rr.length, rr.data);
        crej.reject(rr);
    }

public:
    bool alignPortion() {
        getServerInterface().log("alignPortion():");

        bool foundFirstTerminator = fetchNextRow();
        if(foundFirstTerminator == true) {
            cr.seek(currentRecordSize+1); // currentRecordSize points at the position of terminator, go beyond it
            cr.setPortionReady();
            getServerInterface().log("alignPortion() skipped %zu bytes...", currentRecordSize+1);
            return true;
        }
        else {
            if(cr.isPortionEnd()) { // did not find a row in this portion, we should not process this portion any further (not even reject, since this portion has no ownership towards any row)
                cr.seek(currentRecordSize); // currentRecordSize points at all bytes we've looked at, seek that much
                cr.setPortionFinish();
            }
            return false;
        }
    }

    virtual void run() {
        bool hasMoreData = true;

        // If input_state started with a PORTION_START, set stream state to portion start, and try to align it.
        // One of these two would happen:
        // 1) a record terminator is found w/i this portion. In this case, we will mark stream state as PORTION_ALIGNED, 
        //    and carry on to next stage (parsing).
        // 2) a record terminator was not found w/i this portion. In this case, we will set input_state to END_OF_FILE 
        //    and return (nothing to parse, or reject);
        if(cr.isPortionStart()) {
            // now whenever you call reserve(), it's possible you've changed input_state to END_OF_PORTION;
            bool canAlign = alignPortion();
            if(!canAlign)
                return;
        }

        // Now input is already aligned, we will grab a row (via fetchNextRow) from the stream in each iteration. 
        // fetchNextRow() will return true if we found a complete row, and false if we reached EOF or end of portion.
        // Thus, it is necessary to check if it's end of portion after fetching a row. If it's end of portion, that means
        // there is some buffer in the stream but not enough for a row, and we've reached end of a portion. In this case, 
        // we will break out and go to process the last row, before exiting.
        while (hasMoreData && !cr.isPortionEnd())
        {
            //getServerInterface().log("udparser: going to fetch next row.. ");

            // Fetch the next record
            hasMoreData = fetchNextRow();
            // Important: 
            // previously, reserve(size) would return at least size bytes; otherwise, it would go grab more input by switching to server context; 
            // now in apportion load, above would still hold when not using portion, or portion not at its end yet (input_state != END_OF_PORTION). However, when input has given all bytes of this portion to stream buffer (input_state == END_OF_PORTION), reserve may return buffer size smaller than size. When a reserve() calls returned the same inadequate bytes of buffer (smaller than desired size) as the previous call, that means we dont have a row in the leftover, it would set isPortionEnd() to true, indicating you should only grab one more record from current stream. It is developer's responsibility to check isPortionEnd(), and only parse one more record.
            if(!hasMoreData && cr.isPortionEnd())
                break;

            // Special case: ignore trailing newlines (record terminators) at
            // the end of files
            if (cr.isEof() && currentRecordSize == 0) {
                hasMoreData = false;
                break;
            }

            parseOneRow();
        } 
        
        // Now we have reached end of portion, and want to fetch and parse one more row as the last row in this portion.
        // Here, we still use the same fetchNextRow function to grab a row, w/o having to think about portion end anymore 
        // (now we're not portion end anymore, we're in fact marked as NEXT_PORTION). 
        cr.setNextPortion(); // we only care about transition from PORTION_ALIGNED => PORTION_END
        getServerInterface().log("udparser: going to get the last record");

        hasMoreData = fetchNextRow();

        // Special case: ignore trailing newlines (record terminators) at
        // the end of files
        if (cr.isEof() && currentRecordSize == 0) {
            hasMoreData = false;
            // TODO: what to do for this edge case, if last row too long?
        }

        if(hasMoreData)
            parseOneRow();

        // If we have some buffer left in the stream (meaning these bytes "belong" to this portion we're working on),
        // but we could never parse it (no record terminator found) even though we've reached END_OF_FILE, we are responsible
        // for rejecting this row, instead of silently throwing it away.
        if(cr.noMoreData() && !cr.isEof()) {
            rejectRecord("Wrong number of columns!");
            cr.seek(currentRecordSize + 1);
        }
        
        // Set EOF to unwind parser and lower stream.
        cr.setPortionFinish(); 
    }

    void parseOneRow() {
        bool areMoreColumns;
        bool rejected = false;
        
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
                stringstream ss;
                ss<<"Parse error in column " <<col+1;  // Convert 0-indexing to 1-indexing
                rejectRecord(ss.str());
                rejected = true;
                break;  // Don't bother parsing this row.
            }

            advanceCol();
        }

        row_count++;
        //getServerInterface().log("parseOneRow(): row_count = %zu, currentRecordSize = %zu", row_count, currentRecordSize);

        // Seek past the current record.
        // currentRecordSize points to the end of the record not counting the
        // record terminator.  But we want to seek over the record terminator too.
        cr.seek(currentRecordSize + 1);

        // If we didn't reject the row, emit it
        if (!rejected) {
            writer->next();
            recordsAcceptedInBatch++;
        }
    }


    virtual void initialize(ServerInterface &srvInterface, SizedColumnTypes &returnType);
};

template <class StringParsersImpl>
void DelimFilePortionParser<StringParsersImpl>::initialize(ServerInterface &srvInterface, SizedColumnTypes &returnType) {
    colInfo = returnType;
}

template <>
void DelimFilePortionParser<FormattedStringParsers>::initialize(ServerInterface &srvInterface, SizedColumnTypes &returnType) {
    colInfo = returnType;
    if (formatStrings.size() != returnType.getColumnCount()) {
        formatStrings.resize(returnType.getColumnCount(), "");
    }
    sp.setFormats(formatStrings);
}

template <>
void DelimFilePortionParser<VFormattedStringParsers>::initialize(ServerInterface &srvInterface, SizedColumnTypes &returnType) {
    colInfo = returnType;
    if (formatStrings.size() != returnType.getColumnCount()) {
        formatStrings.resize(returnType.getColumnCount(), "");
    }
    sp.setFormats(formatStrings);
}

template <class StringParsersImpl>
class GenericDelimFilePortionParserFactory : public ParserFactory {
public:
    virtual void plan(ServerInterface &srvInterface,
            PerColumnParamReader &perColumnParamReader,
            PlanContext &planCtxt) {
        /* Check parameters */
        // TODO: Figure out what parameters I should have; then make sure I have them

        /* Populate planData */
        // Nothing to do here
    }


    // NOTE: this factory skipped over-write to prepareChunker(), i.e. it has no chunker

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

        for (size_t i = 0; i < returnType.getColumnCount(); i++) {
            const std::string &cname(returnType.getColumnName(i));
            if (perColumnParamReader.containsColumn(cname)) {
                ParamReader &colArgs = perColumnParamReader.getColumnParamReader(cname);
                if (colArgs.containsParameter("format")) {
                    formatStrings[i] = colArgs.getStringRef("format").str();
                }
            }
        }

        return vt_createFuncObject<DelimFilePortionParser<StringParsersImpl> >
               (srvInterface.allocator,
                delimiter[0],
                record_terminator[0],
                formatStrings,
                true // isParserApportionable
            );
    }

    virtual bool isParserApportionable() 
    { 
        return true;
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
    }
};

typedef GenericDelimFilePortionParserFactory<StringParsers> DelimFilePortionParserFactory;
RegisterFactory(DelimFilePortionParserFactory);

typedef GenericDelimFilePortionParserFactory<FormattedStringParsers> FormattedDelimFilePortionParserFactory;
RegisterFactory(FormattedDelimFilePortionParserFactory);

typedef GenericDelimFilePortionParserFactory<VFormattedStringParsers> VFormattedDelimFilePortionParserFactory;
RegisterFactory(VFormattedDelimFilePortionParserFactory);
