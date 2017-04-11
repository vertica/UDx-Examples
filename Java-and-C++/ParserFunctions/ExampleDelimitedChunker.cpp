/* Copyright (c) 2005 - 2016 Hewlett Packard Enterprise Development LP  -*- C++ -*-*/

#include "ExampleDelimitedChunker.h"

ExampleDelimitedUDChunker::ExampleDelimitedUDChunker(char recordTerminator) :
    recordTerminator(recordTerminator), pastPortion(false) {}

/**
 * This object can be re-used between different sources, so all state pertaining to
 * handling a particular source must be reset here.
 */
void ExampleDelimitedUDChunker::setup(ServerInterface &srvInterface, SizedColumnTypes &colTypes)
{
    pastPortion = false;
}

/**
 * Align the chunker to start processing from the first complete record in the current portion of the stream.
 * Start scanning from the beginning of the buffer to find the first record terminator.
 * Return DONE to indicate that that boundary was found; the first call to process() will begin at that
 * place in the stream.
 * Return INPUT_NEEDED if no record boundary was found.
 */
StreamState ExampleDelimitedUDChunker::alignPortion(ServerInterface &srvInterface, DataBuffer &input, InputState state)
{
    /* find the first record terminator.  Its record belongs to the previous portion */
    void *buf = reinterpret_cast<void *>(input.buf + input.offset);
    void *term = memchr(buf, recordTerminator, input.size - input.offset);

    if (term) {
        /* record boundary found.  Align to the start of the next record */
        const size_t chunkSize = reinterpret_cast<size_t>(term) - reinterpret_cast<size_t>(buf);
        input.offset += chunkSize
            + sizeof(char) /* length of record terminator */;

        /* input.offset points at the start of the first complete record in the portion */
        return DONE;
    } else if (state == END_OF_FILE || state == END_OF_PORTION) {
        return REJECT;
    } else {
        VIAssert(state == START_OF_PORTION || state == OK);
        return INPUT_NEEDED;
    }
}

/**
 * There're three states that chunker could return:
 * if the input state is END_OF_FILE, the state is simply marked as DONE and return;
 * if a few rows were found in current block, move offset forward to point at the start of next (potential) row, and mark state as CHUNK_ALIGNED, indicating the chunker is ready to hand this chunk to parser;
 * if there was no row found in current block, mark the state in INPUT_NEEDED, so that the this process method will get invoked again, with a potentially larger block.
 */
StreamState ExampleDelimitedUDChunker::process(ServerInterface &srvInterface,
                                               DataBuffer &input,
                                               InputState input_state)
{
    const size_t termLen = 1;
    const char *terminator = &recordTerminator;

    if (pastPortion) {
        /*
         * Previous state was END_OF_PORTION, and the last chunk we will produce
         * extends beyond the portion we started with, into the next portion.
         * To be consistent with alignPortion() above, that means finding the first
         * record boundary, and setting the chunk to be at that boundary.
         * Fortunately, this logic is identical to aligning the portion (with
         * some slight accounting for END_OF_FILE)!
         */
        const StreamState findLastTerminator = alignPortion(srvInterface, input, input_state);
        switch (findLastTerminator) {
            case DONE:
                return DONE;
            case REJECT:
                if (input_state == END_OF_FILE) {
                    /* there is no more input where we might find a record terminator */
                    input.offset = input.size;
                    return DONE;
                }
                return INPUT_NEEDED;
            default:
                VIAssert("Invalid return state from alignPortion()");
        }
        return findLastTerminator;
    }


    size_t ret = input.offset, term_index = 0;
    for (size_t index = input.offset; index < input.size; ++index) {
        const char c = input.buf[index];
        if (c == terminator[term_index]) {
            ++term_index;
            if (term_index == termLen) {
                ret = index + 1;
                term_index = 0;
            }
            continue;
        } else if (term_index > 0) {
            index -= term_index;
        }

        term_index = 0;
    }

    if (input_state == END_OF_PORTION) {
        /* 
         * Regardless of whether or not a record was found, the next chunk will extend
         * into the next portion.
         */
        pastPortion = true;
    }

    // if we were able to find some rows, move the offset to point at the start of the next (potential) row, or end of block
    if (ret > input.offset) {
        input.offset = ret;
        return CHUNK_ALIGNED;
    }

    if (input_state == END_OF_FILE) {
        input.offset = input.size;
        return DONE;
    }

    return INPUT_NEEDED;
}

