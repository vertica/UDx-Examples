package com.vertica.JavaLibs;

import java.util.ArrayList;

import com.vertica.sdk.DestroyInvocation;
import com.vertica.sdk.PartitionReader;
import com.vertica.sdk.PartitionWriter;
import com.vertica.sdk.ServerInterface;
import com.vertica.sdk.SizedColumnTypes;
import com.vertica.sdk.TransformFunction;
import com.vertica.sdk.UdfException;

/**
 * Phase 2: Inverted Index Builder
 *
 * Input:
 *   term_freq | PBY(term) | OBY(doc_id) |
 *   ----------+-----------+-------------+
 *       1     |     a     |      1
 *       2     |     a     |      3
 *       1     |     b     |      1
 *       1     |     b     |      2
 *       1     |     b     |      4
 *       1     |     c     |      1
 *       1     |     c     |      2
 *       1     |     d     |      3
 *       1     |     d     |      4
 *       1     |     e     |      2
 *       1     |     e     |      4
 *
 * Output:
 *   term | doc_id | term_freq | corp_freq
 *   -----+--------+-----------+----------
 *     a  |   1    |     1     |
 *     a  |   3    |     2     |
 *     a  |        |           |     2  <= Number of documents that contain "a"
 *     b  |   1    |     1     |
 *     b  |   2    |     1     |
 *     b  |   4    |     1     |
 *     b  |        |           |     3  <= Number of documents that contain "b"
 *    ...
 *  
 */
public class InvertedIndexBuilder extends TransformFunction {

	@Override
	public void processPartition(ServerInterface arg0,
			PartitionReader inputReader, PartitionWriter outputWriter)
			throws UdfException, DestroyInvocation {

        // Sanity checks on input/output we've been given.
        // Expected input: (term_freq INTEGER) OVER(PBY term VARCHAR OBY doc_id INTEGER)	
		SizedColumnTypes inTypes = inputReader.getTypeMetaData();

		ArrayList<Integer> argCols = new ArrayList<Integer>();
		inTypes.getArgumentColumns(argCols);

		ArrayList<Integer> pByCols = new ArrayList<Integer>();
		inTypes.getPartitionByColumns(pByCols);

		ArrayList<Integer> oByCols = new ArrayList<Integer>();
		inTypes.getOrderByColumns(oByCols);

		if (argCols.size() != 1 || pByCols.size() != 1 || oByCols.size() != 1
				|| !inTypes.getColumnType(argCols.get(0)).isInt()
				|| !inTypes.getColumnType(pByCols.get(0)).isVarchar()
				|| !inTypes.getColumnType(oByCols.get(0)).isInt()) {
			throw new UdfException(
					0,
					"Function expects an argument (INTEGER) with analytic clause OVER(PBY VARCHAR OBY INTEGER)");
		}
		
		SizedColumnTypes outTypes = outputWriter.getTypeMetaData();
		ArrayList<Integer> outArgCols = new ArrayList<Integer>();
		outTypes.getArgumentColumns(outArgCols);
		
		if(outArgCols.size() != 4 || 
				!outTypes.getColumnType(outArgCols.get(0)).isVarchar() ||
				!outTypes.getColumnType(outArgCols.get(1)).isInt() ||
				!outTypes.getColumnType(outArgCols.get(2)).isInt() ||
				!outTypes.getColumnType(outArgCols.get(3)).isInt()){
			throw new UdfException(0,"Function expects to emit four columns " +
					"(VARCHAR, INTEGER, INTEGER, INTEGER)");
		}

		String term = inputReader.getString(pByCols.get(0));
		int corpFreq = 0;
		
        // Count the number of documents the term appears in.
		do{
			// Output: (term VARCHAR, doc_id INTEGER, term_freq INTEGER, NULL).
			
			outputWriter.setString(outArgCols.get(0), term); // term
			outputWriter.setLong(outArgCols.get(1), inputReader.getLong(oByCols.get(0))); // doc_id
			outputWriter.setLong(outArgCols.get(2), inputReader.getLong(argCols.get(0))); // term_freq
			outputWriter.setLongNull(outArgCols.get(3)); // corp_freq
			outputWriter.next();
			corpFreq++;
		}while(inputReader.next());

		// Piggyback term's corpus frequency in the output tuple as:
        //   (term VARCHAR, NULL, NULL, corp_freq INTEGER)
		outputWriter.setString(outArgCols.get(0), term); // term
		outputWriter.setLongNull(outArgCols.get(1)); // doc_id
		outputWriter.setLongNull(outArgCols.get(2)); // term_freq
		outputWriter.setLong(outArgCols.get(3), corpFreq); // corp_freq
		outputWriter.next();

	}
}
