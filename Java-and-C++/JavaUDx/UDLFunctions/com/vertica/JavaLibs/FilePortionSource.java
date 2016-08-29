/* Copyright (c) 2005 - 2015 Hewlett Packard Enterprise Development LP  -*- Java -*-
 * 
 *  Create Date: May 19, 2016
 */
package com.vertica.JavaLibs;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.RandomAccessFile;

import com.vertica.sdk.Portion;
import com.vertica.sdk.ServerInterface;
import com.vertica.sdk.UDSource;
import com.vertica.sdk.UdfException;


public class FilePortionSource extends FileSource {
    private final Portion portion;

    public FilePortionSource(String filename, Portion p) {
        super(filename);
        this.portion = p;
    }

    @Override
    public void setup(ServerInterface srvInterface) throws UdfException {
        super.setup(srvInterface);

        try {
            reader.seek(portion.offset);
        } catch (IOException e) {
            throw new UdfException(0, String.format("Could not seek to portion offset \"%d\"", portion.offset), e);
        }
    }

    @Override
    public Integer getSize() {
        return new Integer((int) portion.size);
    }

    @Override
    public Portion getPortion() {
        return portion;
    }
}
