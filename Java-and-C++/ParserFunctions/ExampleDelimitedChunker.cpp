/* Copyright (c) 2005 - 2015 Hewlett Packard Enterprise Development LP  -*- C++ -*-*/

#include "ExampleDelimitedChunker.h"
/**
 * There're three states that chunker could return:
 * if the input state is END_OF_FILE, the state is simply marked as DONE and return;
 * if a few rows were found in current block, move offset forward to point at the start of next (potential) row, and mark state as CHUNK_ALIGNED, indicating the chunker is ready to hand this chunk to parser;
 * if there was no row found in current block, mark the state in INPUT_NEEDED, so that the this process method will get invoked again, with a potentially larger block.
 */
ExampleDelimitedUDChunker::ExampleDelimitedUDChunker(char delimiter,
                                                     char recordTerminator,
                                                     std::vector<std::string> formatStrings)
  : recordTerminator(recordTerminator),
    formatStrings(formatStrings) {}

StreamState ExampleDelimitedUDChunker::process(ServerInterface &srvInterface,
                                               DataBuffer &input,
                                               InputState input_state)
{
    size_t termLen = 1;
    char* terminator = &recordTerminator;

    size_t ret = input.offset, term_index = 0;
    for (size_t index = input.offset; index < input.size; ++index) {
        char c = input.buf[index];
        if (c == terminator[term_index])
        {
            ++term_index;
            if (term_index == termLen)
            {
                ret = index + 1;
                term_index = 0;
            }
            continue;
        }
        else if (term_index > 0)
            index -= term_index;

        term_index = 0;
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

