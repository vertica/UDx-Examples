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
    const char recordTerminator;

    // Apportioned load state
    bool pastPortion;

public:
    ExampleDelimitedUDChunker(char recordTerminator = '\n');

    void setup(ServerInterface &srvInterface, SizedColumnTypes &colTypes);

    StreamState alignPortion(ServerInterface &srvInterface, DataBuffer &input, InputState state);

    StreamState process(ServerInterface &srvInterface,
                        DataBuffer &input,
                        InputState input_state);
};
#endif  // EXAMPLEDELIMCHUNKER_H_
