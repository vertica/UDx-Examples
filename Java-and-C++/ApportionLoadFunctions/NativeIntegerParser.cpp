#include "Vertica.h"

using namespace Vertica;

/**
 * Example parser which parses raw integers into Vertica integers.
 * Results depend on host platform's integer encoding (i.e. big vs. little endian).
 *
 * This example demonstrates combining "cooperative parse"
 * (which achieves parallelism in parsing data on each node with a UDChunker)
 * and "apportioned load" (which achieves parallelism by splitting an input
 * amongst multiple nodes).
 */


/**
 * NativeIntegerChunker
 * UDChunkers group records into "chunks" which can be parsed in parallel.
 * A chunk for this parse format is an integral number of raw integers -
 * a number of bytes that is some multiple of sizeof(vint).
 */
class NativeIntegerChunker : public UDChunker {
private:
    bool pastPortion;
public:
    NativeIntegerChunker() : pastPortion(false) {}

    void setup(ServerInterface &srv, SizedColumnTypes &returnType) {
        pastPortion = false;
    }
    StreamState alignPortion(ServerInterface &srv, DataBuffer &input, InputState state) {
        size_t offset = getPortion().offset;
        while (input.offset < input.size && offset % sizeof(vint) > 0) {
            input.offset++;
            offset++;
        }
        if (input.offset < input.size) {
            VIAssert(offset % sizeof(vint) == 0);
            return DONE;
        } else if (state == END_OF_PORTION || state == END_OF_FILE) {
            /* portion was too small for any records! */
            return REJECT;
        } else {
            /* we got a really really small buffer */
            return INPUT_NEEDED;
        }
    }
    StreamState process(ServerInterface &srv, DataBuffer &input, InputState state) {
        if (pastPortion) {
            /* 
             * previous state was END_OF_PORTION.
             * Chunk the record which traverses the portion boundary
             */
            if (input.offset + sizeof(vint) > input.size && state != END_OF_FILE) {
                VIAssert(state == END_OF_PORTION);
                return INPUT_NEEDED;
            } else if (input.offset + sizeof(vint) > input.size) {
                /* at END OF FILE.  Last portion was tiny! */
                input.offset = input.size;
                return DONE;
            } else  {
                input.offset += sizeof(vint);
                return DONE;
            }
        } else if (state == END_OF_FILE) {
            /* Assumption: buffer is aligned to sizeof(vint) */
            input.offset = input.size;
            return DONE;
        } else if (state == END_OF_PORTION) {
            VIAssert(input.offset % sizeof(vint) == 0);
            input.offset = input.size;
            while (input.offset % sizeof(vint) > 0) {
                input.offset--;
            }

            if (input.offset == input.size) {
                /* chunk is perfectly aligned along portion boundaries */
                return DONE;
            } else {
                /* 
                 * The portion boundary is not aligned to sizeof(vint).
                 * We need to include the record which crosses the
                 * boundary in our chunk.  So we aren't done just yet.
                 */
                pastPortion = true;
                return CHUNK_ALIGNED;
            }
        } else if (input.size - input.offset < sizeof(vint)) {
            /* not EOF, but there are no full records in the buffer */
            return INPUT_NEEDED;
        } else {
            /*
             * Vertica does not decrease buffer sizes once they are requested,
             * except at END_OF_FILE.  Since we are not at END_OF_FILE we can
             * achieve alignment to sizeof(vint) by requesting larger buffers,
             * if needed.
             */
            while (input.size % sizeof(vint) > 0) {
                return INPUT_NEEDED;
            }

            /* now the entire buffer is a chunk */
            input.offset = input.size;
            return CHUNK_ALIGNED;
        }
    }
};

/**
 * NativeIntegerParser
 * Read raw integers from the input stream and emit them into Vertica.
 *
 * This parser supports apportioned streams.  Since all records are of a fixed
 * width, the only thing required to get this is align the parser to a proper
 * record boundary, and handle records which are split across un-aligned portion
 * boundaries.
 *
 * This parser also supports parallel parsing - see the NativeIntegerChunker
 * above.  The only additional work required to do that (aside from implementing
 * the chunker) is handle the END_OF_CHUNK state.
 */
class NativeIntegerParser : public UDParser {
private:
    bool alignedPortion;
    bool pastPortion;

    void advanceInt(DataBuffer &input) {
        writer->setInt(0, *reinterpret_cast<vint *>(input.buf + input.offset));
        input.offset += sizeof(vint);
        writer->next();
    }

public:
    NativeIntegerParser() : alignedPortion(false), pastPortion(false) {}

    RejectedRecord getRejectedRecord() {
        return RejectedRecord("Truncated record at end of stream");
    }

    void setup(ServerInterface &srvInterface, SizedColumnTypes &returnType) {
        alignedPortion = false;
        pastPortion = false;
    }

    StreamState process(ServerInterface &srv, DataBuffer &input, InputState state) {
        if (state == START_OF_PORTION && !alignedPortion) {
            size_t offset = getPortion().offset;
            while (input.offset < input.size && offset % sizeof(vint) > 0) {
                input.offset++;
                offset++;
            }
            if (input.offset < input.size) {
                VIAssert(offset % sizeof(vint) == 0);
                alignedPortion = true;
            } else {
                /* no records in this portion - it is too small! */
                /* 
                 * NOTE: parsers which could have wide rows should probably return
                 * INPUT_NEEDED here.  However, this parser has fixed-size 8-byte rows.
                 * So, we only get here if the whole portion is smaller than 8 bytes.
                 */
                return DONE;
            }
        } else if (pastPortion) {
            if (input.offset + sizeof(vint) > input.size) {
                /* truncated record.  Handled below */
                VIAssert(state == END_OF_FILE);
            } else {
                advanceInt(input);
                return DONE;
            }
        }

        /* read integers into Vertica */
        while (input.offset + sizeof(vint) <= input.size) {
            advanceInt(input);
        }
        VIAssert(input.size - input.offset < sizeof(vint));

        /* get a new buffer, or we're done */
        if (state == END_OF_FILE) {
            if (input.size - input.offset > 0) {
                /* there's a truncated record */
                input.offset = input.size;
                return REJECT;
            } else {
                return DONE;
            }
        } else if (state == END_OF_PORTION) {
            if (input.size - input.offset > 0) {
                /* there is a record which crosses portion boundaries */
                pastPortion = true;
                return INPUT_NEEDED;
            } else {
                /* records are aligned with portion boundaries */
                return DONE;
            }
        } else if (state == END_OF_CHUNK) {
            /* chunker should have aligned properly */
            VIAssert(input.offset == input.size);
            return INPUT_NEEDED;
        } else {
            return INPUT_NEEDED;
        }
    }
};

class NativeIntegerParserFactory : public ParserFactory {
public:
    void getParserReturnType(ServerInterface &srvInterface,
                                     PerColumnParamReader &perColumnParamReader,
                                     PlanContext &planCtxt,
                                     const SizedColumnTypes &argTypes,
                                     SizedColumnTypes &returnType) {
        returnType.addInt();
    }
    bool isParserApportionable() {
        return true;
    }
    bool isChunkerApportionable(ServerInterface &srvInterface) {
        return true;
    }
    UDParser *prepare(ServerInterface &srv,
            PerColumnParamReader &columnParams,
            PlanContext &ctx,
            const SizedColumnTypes &returnType) {
        return vt_createFuncObject<NativeIntegerParser>(srv.allocator);
    }
    UDChunker *prepareChunker(ServerInterface &srv,
            PerColumnParamReader &columnParams,
            PlanContext &ctx,
            const SizedColumnTypes &returnType) {
        return vt_createFuncObject<NativeIntegerChunker>(srv.allocator);
    }
};
RegisterFactory(NativeIntegerParserFactory);
