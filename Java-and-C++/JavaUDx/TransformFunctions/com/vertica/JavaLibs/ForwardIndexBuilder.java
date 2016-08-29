package com.vertica.JavaLibs;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.vertica.sdk.DestroyInvocation;
import com.vertica.sdk.PartitionReader;
import com.vertica.sdk.PartitionWriter;
import com.vertica.sdk.ServerInterface;
import com.vertica.sdk.SizedColumnTypes;
import com.vertica.sdk.TransformFunction;
import com.vertica.sdk.UdfException;

/**
 * Multiphase User Defined Transform: Inverted Index Example
 *
 * Documents are stored in table T:
 *   T(doc_id INTEGER, text VARCHAR(200))
 * where:
 *   doc_id - is the document identifier.
 *   text - document's text content.
 *
 * The inverted index is constructed in two phases.
 * The first phase builds a forward index -- for each document, all its terms 
 * with frequencies are emitted. During the second phase, documents
 * that contain a particular term are received in sorted order by doc_id, 
 * which form the term's posting list, and are emitted to the final output.
 *
 * The inverted index is persisted in the table:
 *   T_II(term VARCHAR(200), doc_id INTEGER, term_freq INTEGER, corp_freq INTEGER)
 * where:
 *   term - a term in the corpus vocabulary.
 *   doc_id - identifier of a document containing term.
 *   term_freq - number of times term appears in document identified by doc_id.
 *   corp_freq - number of times term appears in the document corpus.
 */

/**
 * Phase 1: Forward Index Builder
 *
 * Input:
 *   doc_id | text
 *   -------+------
 *      1   | a b c
 *      2   | c b e
 *      3   | a d a
 *      4   | b d e
 *
 * Output:
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
 */
public class ForwardIndexBuilder extends TransformFunction {

	@Override
	public void processPartition(ServerInterface arg0,
			PartitionReader inputReader, PartitionWriter outputWriter)
			throws UdfException, DestroyInvocation {
		
        // Sanity checks on input/output we've been given.
        // Expected input: (doc_id INTEGER, text VARCHAR)		
		SizedColumnTypes inTypes = inputReader.getTypeMetaData();
		ArrayList<Integer> argCols = new ArrayList<Integer>();
		inTypes.getArgumentColumns(argCols);

		if (argCols.size() < 2
				|| !inTypes.getColumnType(argCols.get(0)).isInt()
				|| !inTypes.getColumnType(argCols.get(1)).isVarchar()) {
			throw new UdfException(0,
					"Function Expects two arguments(Integer, Varchar)");
		}

		SizedColumnTypes outTypes = outputWriter.getTypeMetaData();
		ArrayList<Integer> outArgsCols = new ArrayList<Integer>();
		outTypes.getArgumentColumns(outArgsCols);

		ArrayList<Integer> outPbyCols = new ArrayList<Integer>();
		outTypes.getPartitionByColumns(outPbyCols);

		ArrayList<Integer> outObyCols = new ArrayList<Integer>();
		outTypes.getOrderByColumns(outObyCols);

		if (outArgsCols.size() != 1
				|| !outTypes.getColumnType(outArgsCols.get(0)).isInt()
				|| outPbyCols.size() != 1
				|| !outTypes.getColumnType(outPbyCols.get(0)).isVarchar()
				|| outObyCols.size() != 1
				|| !outTypes.getColumnType(outObyCols.get(0)).isInt()) {
			throw new UdfException(0,
					"Function expects to emit an (INTEGER) argument"
							+ " with OVER(PBY (VARCHAR) OBY (INTEGER)) clause.");
		}

		Map<String, Long> docTerms = new HashMap<String,Long>();

		//Extract terms from Documents
		do {
			docTerms.clear();
			long docID = inputReader.getLong(argCols.get(0));

			String text = inputReader.getString(argCols.get(1)).toLowerCase();

			if (text != null && !text.isEmpty()) {
				String[] terms = text.split(" ");
				int i = 0;
				while (i < terms.length) {
					if (!docTerms.containsKey(terms[i])) {
						docTerms.put(terms[i], 1L); // new document term
					} else {
						docTerms.put(terms[i], docTerms.get(terms[i]) + 1); // term was already seen. Increment its count.
					}
					i++;
				}
			}
			
			// output: (term_freq) OVER(PBY term OBY doc_id).
			for(Entry<String,Long> docTerm : docTerms.entrySet()){
				outputWriter.setLong(outArgsCols.get(0), docTerm.getValue()); //term_freq
				outputWriter.setString(outPbyCols.get(0), docTerm.getKey()); // term
				outputWriter.setLong(outObyCols.get(0),docID); // doc_id
				outputWriter.next();
			}

		} while (inputReader.next());
	}

}
