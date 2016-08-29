package com.vertica.JavaLibs;

import com.vertica.sdk.ContinuousReader;
import com.vertica.sdk.StringUtils;
import com.vertica.sdk.ContinuousUDParser;
import com.vertica.sdk.DestroyInvocation;
import com.vertica.sdk.UdfException;
import com.vertica.sdk.ContinuousReader;

public class BasicIntegerParserContinuous extends ContinuousUDParser {

	public String convertToStr(int pos) {
		ContinuousReader  cr = getContinuousReader();
		String s = new String(cr.getCurrentBuffer().buf,
				cr.getCurrentBuffer().offset, pos, StringUtils.getCharset());
		return s;
	}

	@Override
	public void run() throws UdfException {
		ContinuousReader  cr = getContinuousReader();
		try {
			int pos = 0;

			int reserved = cr.reserve(pos + 1);
			while (!cr.isEOF() || reserved == pos + 1) {
				while (reserved == pos + 1 && cr.getDataAt(pos) >= '0'
						&& cr.getDataAt(pos) <= '9') {
					pos += 1;
					reserved = cr.reserve(pos + 1);
				}

				String curr = convertToStr(pos);
				getStreamWriter().setLong(0, Long.parseLong(curr));
				getStreamWriter().next();
				while (reserved == pos + 1
						&& !(cr.getDataAt(pos) >= '0' && cr.getDataAt(pos) <= '9')) {
					pos += 1;
					reserved = cr.reserve(pos + 1);
				}
				cr.seek(pos);
				pos = 0;
				reserved = cr.reserve(pos + 1);
			}
		} catch (NumberFormatException nfe){
			throw new UdfException(0, "Invalid Character input");
		} catch(DestroyInvocation di){
			throw new UdfException(0, di.getMessage());
		}
	}
}
