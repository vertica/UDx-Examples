/* Copyright (c) 2005 - 2015 Hewlett Packard Enterprise Development LP  -*- Java -*-
 * 
 *  Create Date: September 10, 2013
 */
package com.vertica.JavaLibs;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.RandomAccessFile;

import com.vertica.sdk.DataBuffer;
import com.vertica.sdk.ServerInterface;
import com.vertica.sdk.State.StreamState;
import com.vertica.sdk.UDSource;
import com.vertica.sdk.UdfException;

public class FileSource extends UDSource {
	
	public FileSource(String filename) {
		super();
		this.filename = filename;
	}

	@Override
	public void setup(ServerInterface srvInterface ) throws UdfException{
		try {
			reader = new RandomAccessFile(new File(filename), "r");
		} catch (FileNotFoundException e) {
			 String msg = e.getMessage();
			 throw new UdfException(0, msg);
		}
	}
	
	@Override
	public void destroy(ServerInterface srvInterface ) throws UdfException {
		if (reader != null)
			try {
				reader.close();
			} catch (IOException e) {
				String msg = e.getMessage();
				 throw new UdfException(0, msg);
			}
	}
	
	@Override
	public StreamState process(ServerInterface srvInterface, DataBuffer output) throws UdfException {
		
		//make sure we don't overwrite the already existing stuff
		long offset;
		try {
				offset = reader.read(output.buf,output.offset,output.buf.length-output.offset);							
		} catch (IOException e) {
			String msg = e.getMessage();
			throw new UdfException(0, msg);
		}
		
		output.offset +=offset;		
		if (offset == -1 || offset < output.buf.length) {
			return StreamState.DONE;
		} else {
			return StreamState.OUTPUT_NEEDED;
		}
		
	}

	private String filename;
	protected RandomAccessFile reader;
}
