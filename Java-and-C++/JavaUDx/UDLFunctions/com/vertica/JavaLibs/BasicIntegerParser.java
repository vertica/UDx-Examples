/* 
 * Copyright (c) 2005 - 2017 Hewlett Packard Enterprise Development LP -*- Java -*-
 *
 * Create Date: September 10, 2013
 */
package com.vertica.JavaLibs;

import com.vertica.sdk.DataBuffer;
import com.vertica.sdk.DestroyInvocation;
import com.vertica.sdk.ServerInterface;
import com.vertica.sdk.State.InputState;
import com.vertica.sdk.State.StreamState;
import com.vertica.sdk.UDParser;
import com.vertica.sdk.UdfException;

public class BasicIntegerParser extends UDParser {
    @Override
    public StreamState process(ServerInterface srvInterface, DataBuffer input,
            InputState input_state) throws UdfException, DestroyInvocation {

        int start = input.offset;
        final int end = input.buf.length;

        do {
            boolean foundNewline = false;
            int numEnd = start;

            // assume we're at the start of an integer.  Search for a non-numeric character,
            // since that is how we're splitting the input
            for (; numEnd < end; numEnd++) {
                if (input.buf[numEnd] < '0' || input.buf[numEnd] > '9') {
                    foundNewline = true;
                    break;
                }
            }

            if (!foundNewline) {
                input.offset = start;
                if (input_state == InputState.END_OF_FILE) {
                    // If we're at end-of-file,
                    // emit the last integer (if any) and return DONE
                    if (start != end) {
                        String str = new String(input.buf, start, numEnd - start);
                        writer.setLong(0, Long.parseLong(str, 10));
                        writer.next();
                    }
                    return StreamState.DONE;
                } else {
                    // need more data
                    return StreamState.INPUT_NEEDED;
                }
            }

            String str = new String(input.buf, start, numEnd - start);
            writer.setLong(0, Long.parseLong(str, 10));
            writer.next();

            start = numEnd + 1;
        } while (true);
    }
}
