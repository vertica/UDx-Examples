/* Copyright (c) 2005 - 2015 Hewlett Packard Enterprise Development LP  -*- Java -*-
 * 
 *  Create Date: September 10, 2013 
 * */

package com.vertica.JavaLibs;

import com.vertica.sdk.StringUtils;
import com.vertica.sdk.DataBuffer;
import com.vertica.sdk.ServerInterface;
import com.vertica.sdk.State.InputState;
import com.vertica.sdk.State.StreamState;
import com.vertica.sdk.UDFilter;
/**
 * OneTwoDecoder
 * A sample decoder.
 * Rather than performing a sophisticated decompression or
 * decryption or (etc) algorithm, this "decoder" simply swaps
 * all '1's for '2's, and slices off the latter half of the input.
 */
public class OneTwoDecoder extends UDFilter {
	public OneTwoDecoder(String char_to_replace, String char_to_replace_with){
	   this.char_to_replace= char_to_replace.getBytes(StringUtils.getCharset());
	   this.char_to_replace_with=char_to_replace_with.getBytes(StringUtils.getCharset());
	   this.hasRun=false;
	   this.flush=false;

	}
	@Override
	public StreamState process(ServerInterface srvInterface, DataBuffer input,
			InputState input_state, DataBuffer output) {
		if(flush){
			flush=false;
			return StreamState.OUTPUT_NEEDED;
		}
	
		assert(hasRun || input.buf.length >= 65000 || input_state == InputState.END_OF_FILE);  // VER-22560
		if (input_state == InputState.END_OF_FILE && input.buf.length == 0) {
			return StreamState.DONE;
	    }
		int offset = (int) output.offset;
		
		for (int i = input.offset; i < Math.min((input.buf.length - input.offset)/2, output.buf.length - output.offset); i++) {
			
            if (input.buf[i] == char_to_replace[0]) {
            	output.buf[i+offset] = char_to_replace_with[0];
            } else {
            	output.buf[i+offset]=input.buf[i];
            }
        }
		
		// We cut our data size in half, just 'cause.
		input.offset += Math.min(input.buf.length - input.offset, output.buf.length - output.offset);
		output.offset += input.offset/2;
		
        if (input.buf.length - input.offset < output.buf.length - output.offset) {
            flush = true;
            return input_state == InputState.END_OF_FILE ?
                StreamState.DONE : StreamState.INPUT_NEEDED;
        } else {
            return StreamState.OUTPUT_NEEDED;
        }
	}
	
	
	
	private byte[] char_to_replace;
	private byte[] char_to_replace_with;

	private boolean hasRun;
	private boolean flush;

}
