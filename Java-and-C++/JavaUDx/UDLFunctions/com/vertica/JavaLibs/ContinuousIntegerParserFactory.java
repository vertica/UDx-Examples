package com.vertica.JavaLibs;

import com.vertica.sdk.ParserFactory;
import com.vertica.sdk.PerColumnParamReader;
import com.vertica.sdk.PlanContext;
import com.vertica.sdk.ServerInterface;
import com.vertica.sdk.SizedColumnTypes;
import com.vertica.sdk.UDParser;
import com.vertica.sdk.UdfException;

public class ContinuousIntegerParserFactory extends ParserFactory {
    @Override
    public UDParser prepare(ServerInterface srvInterface, PerColumnParamReader perColumnParamReader,
            PlanContext planContext, SizedColumnTypes columnTypes) throws UdfException {
        return new ContinuousIntegerParser();
    }

    @Override
    public void getParserReturnType(ServerInterface srvInterface,
            PerColumnParamReader perColumnParamReader, PlanContext planContext,
            SizedColumnTypes columnTypes, SizedColumnTypes returnTypes) throws UdfException {
        returnTypes.addInt(columnTypes.getColumnName(0));
    }
}
