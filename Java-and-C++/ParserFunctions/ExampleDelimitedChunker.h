/* Copyright (c) 2005 - 2016 Hewlett Packard Enterprise Development LP  -*- C++ -*-*/

#include "Vertica.h"

#ifndef EXAMPLEDELIMCHUNKER_H_
#define EXAMPLEDELIMCHUNKER_H_

using namespace Vertica;

#include <string>
#include <vector>
#include <sstream>
#include <iostream>

class ExampleDelimitedUDChunker : public UDChunker
{
private:
    // Configurable parsing parameters
    // Set by the constructor
    char recordTerminator;

    // Format strings
    std::vector<std::string> formatStrings;

    // Start off reserving this many bytes when searching for the end of a record
    // Will reserve more as needed; but from a performance perspective it's
    // nice to not have to do so.
    static const size_t BASE_RESERVE_SIZE = 256;

public:
    ExampleDelimitedUDChunker(char delimiter = ',',
                              char recordTerminator = '\n',
                              std::vector<std::string> formatStrings = std::vector<std::string>());

    StreamState process(ServerInterface &srvInterface,
                        DataBuffer &input,
                        InputState input_state);
};
#endif  // EXAMPLEDELIMCHUNKER_H_
