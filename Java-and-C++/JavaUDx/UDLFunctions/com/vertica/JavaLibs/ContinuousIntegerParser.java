package com.vertica.JavaLibs;

import com.vertica.sdk.ContinuousReader;
import com.vertica.sdk.StringUtils;
import com.vertica.sdk.ContinuousUDParser;
import com.vertica.sdk.DestroyInvocation;
import com.vertica.sdk.UdfException;
import com.vertica.sdk.ContinuousReader;

/**
 * Example usage of the ContinuousUDParser class.
 * "cr.reserve()" reads data from the input stream, but doesn't advance in the stream.
 * "cr.seek()" advances through the stream.
 * This parser uses reserve() to grab input data one character at a time.  When it
 * identifies a sequence of digits in the input, it parses the sequence into a "long"
 * value and advances in the stream (using seek()).
 */
public class ContinuousIntegerParser extends ContinuousUDParser {
    private String readBufferToString(int pos) {
        ContinuousReader cr = getContinuousReader();
        String s = new String(cr.getCurrentBuffer().buf,
                cr.getCurrentBuffer().offset, pos, StringUtils.getCharset());
        return s;
    }

    @Override
    public void run() throws UdfException {
        ContinuousReader cr = getContinuousReader();
        try {
            int pos = 0;

            int reserved = cr.reserve(pos + 1);
            while (!cr.isEOF() || reserved == pos + 1) {
                while (reserved == pos + 1 && Character.isDigit(cr.getDataAt(pos))) {
                    pos += 1;
                    reserved = cr.reserve(pos + 1);
                }

                try {
                    String curr = readBufferToString(pos);
                    getStreamWriter().setLong(0, Long.parseLong(curr));
                    getStreamWriter().next();
                } catch (NumberFormatException nfe) {
                    throw new UdfException(0, "Invalid Character input");
                }

                while (reserved == pos + 1 && !Character.isDigit(cr.getDataAt(pos))) {
                    pos += 1;
                    reserved = cr.reserve(pos + 1);
                }

                cr.seek(pos);
                pos = 0;
                reserved = cr.reserve(pos + 1);
            }
        } catch(DestroyInvocation di) {
            throw new UdfException(0, di.getMessage());
        }
    }
}
