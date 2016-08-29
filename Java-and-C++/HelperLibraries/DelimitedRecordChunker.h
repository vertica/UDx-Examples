/* Copyright (c) 2005 - 2016 Hewlett Packard Enterprise Development LP  -*- C++ -*-*/
#include "Vertica.h"

#ifndef DELIMITEDRECORDCHUNKER_H_
#define DELIMITEDRECORDCHUNKER_H_

using namespace Vertica;

/**
 * Class to find record boundaries given an input block and a single-character record terminator
 */
class DelimitedRecordChunker: public UDChunker {
private:
    char recordTerminator;
    DelimitedRecordChunker() { }

public:
    DelimitedRecordChunker(char recordTerminator) : recordTerminator(recordTerminator) { }

    StreamState process(ServerInterface &srvInterface,
                        DataBuffer &input, InputState input_state) {
        if (input_state == END_OF_FILE) {
            input.offset = input.size;
            return DONE;
        }

        size_t ret = input.size;
        // Find the last record terminator in the input block
        for (size_t i = input.size - 1; i > input.offset; --i) {
            if (input.buf[i] == recordTerminator) {
                ret = i + 1;
                break;
            }
        }

        if (ret < input.size) {
            // We found a record terminator in the input block
            // Move offset to the start of the next potential record
            LogDebugUDInfo("[DelimitedRecordChunker]: Record boundary found at [%zu]", ret);
            input.offset = ret;
            return CHUNK_ALIGNED;
        }

        return INPUT_NEEDED;
    }
};

#endif  // DELIMITEDRECORDCHUNKER_H_
