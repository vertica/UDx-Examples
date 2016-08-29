package com.vertica.JavaLibs;

import java.util.ArrayList;
import java.util.Vector;

import com.vertica.sdk.ColumnTypes;
import com.vertica.sdk.MultiPhaseTransformFunctionFactory;
import com.vertica.sdk.ServerInterface;
import com.vertica.sdk.SizedColumnTypes;
import com.vertica.sdk.TransformFunction;
import com.vertica.sdk.TransformFunctionPhase;
import com.vertica.sdk.UdfException;

public class InvertedIndexFactory extends MultiPhaseTransformFunctionFactory {

	/**
	 * Extracts terms from documents.
	 */
	public class ForwardIndexPhase extends TransformFunctionPhase {

		@Override
		public TransformFunction createTransformFunction(ServerInterface arg0) {
			return new ForwardIndexBuilder();
		}

		@Override
		public void getReturnType(ServerInterface arg0,
				SizedColumnTypes inputTypes, SizedColumnTypes outputTypes) {

			// Sanity checks on input we've been given.
			// Expected input: (doc_id INTEGER, text VARCHAR)
			ArrayList<Integer> argCols = new ArrayList<Integer>();

			inputTypes.getArgumentColumns(argCols);

			if (argCols.size() < 2
					|| !inputTypes.getColumnType(argCols.get(0)).isInt()
					|| !inputTypes.getColumnType(argCols.get(1)).isVarchar()) {
				throw new UdfException(0,
						"Funciton only accepts two arguments (INTEGER, VARCHAR)");
			}

			// Output of this phase is:
			// (term_freq INTEGER) OVER(PBY term VARCHAR OBY doc_id INTEGER)

			// Number of times term appears within a document.
			outputTypes.addInt("term_freq");

			// Add analytic clause columns: (PARTITION BY term ORDER BY doc_id).

			// The length of any term is at most the size of the entire
			// document.
			outputTypes.addVarcharPartitionColumn(
					inputTypes.getColumnType(argCols.get(1)).getStringLength(),
					"term");

			// Add order column on the basis of the document id's data type.
			outputTypes.addOrderColumn(
					inputTypes.getColumnType(argCols.get(0)), "doc_id");
		}

	}

	public class InvertedIndexPhase extends TransformFunctionPhase {

		@Override
		public TransformFunction createTransformFunction(
				ServerInterface srvInterface) {
			return new InvertedIndexBuilder();
		}

		@Override
		public void getReturnType(ServerInterface srvInterface,
				SizedColumnTypes inputTypes, SizedColumnTypes outputTypes) {
			// Sanity checks on input we've been given.
			// Expected input:
			// (term_freq INTEGER) OVER(PBY term VARCHAR OBY doc_id INTEGER)
			ArrayList<Integer> argCols = new ArrayList<Integer>();
			inputTypes.getArgumentColumns(argCols);

			ArrayList<Integer> pByCols = new ArrayList<Integer>();
			inputTypes.getPartitionByColumns(pByCols);

			ArrayList<Integer> oByCols = new ArrayList<Integer>();
			inputTypes.getOrderByColumns(oByCols);

			if (argCols.size() != 1 || pByCols.size() != 1
					|| oByCols.size() != 1
					|| !inputTypes.getColumnType(argCols.get(0)).isInt()
					|| !inputTypes.getColumnType(pByCols.get(0)).isVarchar()
					|| !inputTypes.getColumnType(oByCols.get(0)).isInt()) {
				throw new UdfException(
						0,
						"Function expects an argument (INTEGER) with "
								+ "analytic clause OVER(PBY VARCHAR OBY INTEGER)");
			}

			// Output of this phase is:
			// (term VARCHAR, doc_id INTEGER, term_freq INTEGER, corp_freq
			// INTEGER).
			outputTypes.addVarchar(inputTypes.getColumnType(pByCols.get(0))
					.getStringLength(), "term");
			outputTypes.addInt("doc_id");

			// Number of times term appears within the document.
			outputTypes.addInt("term_freq");

			// Number of documents where the term appears in.
			outputTypes.addInt("corp_freq");

		}
	}

	@Override
	public void getPhases(ServerInterface srvInterface,
			Vector<TransformFunctionPhase> phases) {
		ForwardIndexPhase fwardIdxPh;
		InvertedIndexPhase invIdxPh;
		
		fwardIdxPh = new ForwardIndexPhase();
		invIdxPh = new InvertedIndexPhase();
		
		fwardIdxPh.setPrepass();
		phases.add(fwardIdxPh);
		phases.add(invIdxPh);
	}

	@Override
	public void getPrototype(ServerInterface srvInterface,
			ColumnTypes argTypes, ColumnTypes returnTypes) {

		// Expected input: (doc_id INTEGER, text VARCHAR).
		argTypes.addInt();
		argTypes.addVarchar();

		// Output is: (term VARCHAR, doc_id INTEGER, term_freq INTEGER,
		// corp_freq INTEGER)
		returnTypes.addVarchar();
		returnTypes.addInt();
		returnTypes.addInt();
		returnTypes.addInt();
	}

}
