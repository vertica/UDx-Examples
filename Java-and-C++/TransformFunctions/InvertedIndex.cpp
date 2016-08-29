/* Copyright (c) 2005 - 2015 Hewlett Packard Enterprise Development LP  -*- C++ -*-*/
/* 
 * Description: Example User Defined Transform Function: Build inverted index
 *
 * Create Date: March 22, 2014
 */
#include "Vertica.h"
#include <time.h> 
#include <sstream>
#include <map>

using namespace Vertica;
using namespace std;

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
class ForwardIndexBuilder : public TransformFunction
{
    virtual void processPartition(ServerInterface &srvInterface, PartitionReader &inputReader, PartitionWriter &outputWriter)
    {
        try {
            // Sanity checks on input/output we've been given.
            // Expected input: (doc_id INTEGER, text VARCHAR)
            const SizedColumnTypes &inTypes = inputReader.getTypeMetaData();
            vector<size_t> argCols;
            inTypes.getArgumentColumns(argCols);

            if (argCols.size() < 2 || !inTypes.getColumnType(argCols.at(0)).isInt() ||
                !inTypes.getColumnType(argCols.at(1)).isVarchar())
                vt_report_error(0, "Function expects two arguments (INTEGER, VARCHAR).");

            const SizedColumnTypes &outTypes = outputWriter.getTypeMetaData();
            vector<size_t> outArgCols;
            outTypes.getArgumentColumns(outArgCols);
            vector<size_t> outPbyCols;
            outTypes.getPartitionByColumns(outPbyCols);
            vector<size_t> outObyCols;
            outTypes.getOrderByColumns(outObyCols);

            if (outArgCols.size() != 1 || !outTypes.getColumnType(outArgCols.at(0)).isInt() ||
                outPbyCols.size() != 1 || !outTypes.getColumnType(outPbyCols.at(0)).isVarchar() ||
                outObyCols.size() != 1 || !outTypes.getColumnType(outObyCols.at(0)).isInt())
                vt_report_error(0, "Function expects to emit an (INTEGER) argument "
                                "with OVER(PBY (VARCHAR) OBY (INTEGER)) clause.");

            // Extract terms from documents.
            do {
                vint docId = inputReader.getIntRef(argCols.at(0));
                VString text = inputReader.getStringRef(argCols.at(1));
                map<string, int> docTerms; // term counts.

                if (!text.isNull()) {
                    std::string terms = text.str();
                    istringstream iss(terms);

                    do {
                        std::string term;
                        iss >> term;

                        if (!term.empty()) {
                            // Standardize terms to lowercase.
                            std::transform(term.begin(), term.end(), term.begin(), ::tolower);

                            if (docTerms.count(term) == 0)
                                docTerms[term] = 1; // New document term.
                            else
                                docTerms[term]++; // Term was already seen. Increment its count.
                        }
                    } while (iss);

                    // Output: (term_freq) OVER(PBY term OBY doc_id).
                    for (map<string, int>::iterator it = docTerms.begin();
                         it != docTerms.end(); ++it) {
                        outputWriter.setInt(outArgCols.at(0), it->second); // term_freq
                        VString &termRef = outputWriter.getStringRef(outPbyCols.at(0)); // term
                        termRef.copy(it->first);
                        outputWriter.setInt(outObyCols.at(0), docId); // doc_id
                        outputWriter.next();
                    }

                    docTerms.clear();
                }
            } while (inputReader.next());
       } catch(exception& e) {
           // Standard exception. Quit.
           vt_report_error(0, "Exception while processing partition: [%s]", e.what());
       }
   }
};

class InvertedIndexFactory;


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
class InvertedIndexBuilder : public TransformFunction
{
   virtual void processPartition(ServerInterface &srvInterface, PartitionReader &inputReader, PartitionWriter &outputWriter)
   {
       try {
           // Sanity checks on input/output we've been given.
           // Expected input: (term_freq INTEGER) OVER(PBY term VARCHAR OBY doc_id INTEGER)
           const SizedColumnTypes &inTypes = inputReader.getTypeMetaData();

           vector<size_t> argCols;
           inTypes.getArgumentColumns(argCols);
           vector<size_t> pByCols;
           inTypes.getPartitionByColumns(pByCols);
           vector<size_t> oByCols;
           inTypes.getOrderByColumns(oByCols);

           if (argCols.size() != 1 || pByCols.size() != 1 || oByCols.size() != 1 ||
               !inTypes.getColumnType(argCols.at(0)).isInt() ||
               !inTypes.getColumnType(pByCols.at(0)).isVarchar() ||
               !inTypes.getColumnType(oByCols.at(0)).isInt())
               vt_report_error(0, "Function expects an argument (INTEGER) with analytic clause OVER(PBY VARCHAR OBY INTEGER)");

           const SizedColumnTypes &outTypes = outputWriter.getTypeMetaData();
           vector<size_t> outArgCols;
           outTypes.getArgumentColumns(outArgCols);

           if (outArgCols.size() != 4 || 
               !outTypes.getColumnType(outArgCols.at(0)).isVarchar() ||
               !outTypes.getColumnType(outArgCols.at(1)).isInt() ||
               !outTypes.getColumnType(outArgCols.at(2)).isInt() ||
               !outTypes.getColumnType(outArgCols.at(3)).isInt())
               vt_report_error(0, "Function expects to emit four columns "
                               "(VARCHAR, INTEGER, INTEGER, INTEGER)");

           VString term = inputReader.getStringRef(pByCols.at(0));
           ostringstream oss;
           vint corpFreq = 0;

           // Count the number of documents the term appears in.
           do {
               // Output: (term VARCHAR, doc_id INTEGER, term_freq INTEGER, NULL).
               VString &outTerm = outputWriter.getStringRef(outArgCols.at(0)); // term
               outTerm.copy(term);
               outputWriter.setInt(outArgCols.at(1), inputReader.getIntRef(oByCols.at(0))); // doc_id
               outputWriter.setInt(outArgCols.at(2), inputReader.getIntRef(argCols.at(0))); // term_freq
               outputWriter.setNull(outArgCols.at(3)); // corp_freq
               outputWriter.next();
               corpFreq++;
           } while (inputReader.next());

           // Piggyback term's corpus frequency in the output tuple as:
           //   (term VARCHAR, NULL, NULL, corp_freq INTEGER)
           VString &outTerm = outputWriter.getStringRef(outArgCols.at(0)); // term
           outTerm.copy(term);
           outputWriter.setNull(outArgCols.at(1)); // doc_id
           outputWriter.setNull(outArgCols.at(2)); // term_freq
           outputWriter.setInt(outArgCols.at(3), corpFreq); // corp_freq
           outputWriter.next();
       } catch(exception& e) {
           // Standard exception. Quit.
           vt_report_error(0, "Exception while processing partition: [%s]", e.what());
       }
   }
};


class InvertedIndexFactory : public MultiPhaseTransformFunctionFactory
{
public:
   /**
    * Extracts terms from documents.
    */
   class ForwardIndexPhase : public TransformFunctionPhase
   {
       virtual void getReturnType(ServerInterface &srvInterface,
                                  const SizedColumnTypes &inputTypes,
                                  SizedColumnTypes &outputTypes)
       {
           // Sanity checks on input we've been given.
           // Expected input: (doc_id INTEGER, text VARCHAR)
           vector<size_t> argCols;
           inputTypes.getArgumentColumns(argCols);

           if (argCols.size() < 2 ||
               !inputTypes.getColumnType(argCols.at(0)).isInt() ||
               !inputTypes.getColumnType(argCols.at(1)).isVarchar())
               vt_report_error(0, "Function only accepts two arguments (INTEGER, VARCHAR))");

           // Output of this phase is:
           //   (term_freq INTEGER) OVER(PBY term VARCHAR OBY doc_id INTEGER)

           // Number of times term appears within a document.
           outputTypes.addInt("term_freq");

           // Add analytic clause columns: (PARTITION BY term ORDER BY doc_id).

           // The length of any term is at most the size of the entire document.
           outputTypes.addVarcharPartitionColumn(inputTypes.getColumnType(argCols.at(1)).getStringLength(), "term");

           // Add order column on the basis of the document id's data type.
           outputTypes.addOrderColumn(inputTypes.getColumnType(argCols.at(0)), "doc_id");
       }

       virtual TransformFunction *createTransformFunction(ServerInterface &srvInterface)
       { return vt_createFuncObject<ForwardIndexBuilder>(srvInterface.allocator); }
   };

   /**
    * Constructs terms' posting lists.
    */
   class InvertedIndexPhase : public TransformFunctionPhase
   {
       virtual void getReturnType(ServerInterface &srvInterface,
                                  const SizedColumnTypes &inputTypes,
                                  SizedColumnTypes &outputTypes)
       {
           // Sanity checks on input we've been given.
           // Expected input:
           //   (term_freq INTEGER) OVER(PBY term VARCHAR OBY doc_id INTEGER)
           vector<size_t> argCols;
           inputTypes.getArgumentColumns(argCols);
           vector<size_t> pByCols;
           inputTypes.getPartitionByColumns(pByCols);
           vector<size_t> oByCols;
           inputTypes.getOrderByColumns(oByCols);

           if (argCols.size() != 1 || pByCols.size() != 1 || oByCols.size() != 1 ||
               !inputTypes.getColumnType(argCols.at(0)).isInt() ||
               !inputTypes.getColumnType(pByCols.at(0)).isVarchar() ||
               !inputTypes.getColumnType(oByCols.at(0)).isInt())
               vt_report_error(0, "Function expects an argument (INTEGER) with "
                               "analytic clause OVER(PBY VARCHAR OBY INTEGER)");

           // Output of this phase is:
           //   (term VARCHAR, doc_id INTEGER, term_freq INTEGER, corp_freq INTEGER).

           outputTypes.addVarchar(inputTypes.getColumnType(pByCols.at(0)).getStringLength(),
                                   "term");
           outputTypes.addInt("doc_id");

           // Number of times term appears within the document.
           outputTypes.addInt("term_freq");

           // Number of documents where the term appears in.
           outputTypes.addInt("corp_freq");
       }
       
       virtual TransformFunction *createTransformFunction(ServerInterface &srvInterface)
       { return vt_createFuncObject<InvertedIndexBuilder>(srvInterface.allocator); }
   };

   ForwardIndexPhase fwardIdxPh;
   InvertedIndexPhase invIdxPh;

   virtual void getPhases(ServerInterface &srvInterface, std::vector<TransformFunctionPhase *> &phases)
   {
       fwardIdxPh.setPrepass(); // Process documents wherever they're originally stored.
       phases.push_back(&fwardIdxPh);
       phases.push_back(&invIdxPh);
   }

   virtual void getPrototype(ServerInterface &srvInterface,
                             ColumnTypes &argTypes,
                             ColumnTypes &returnType)
   {
       // Expected input: (doc_id INTEGER, text VARCHAR).
       argTypes.addInt();
       argTypes.addVarchar();

       // Output is: (term VARCHAR, doc_id INTEGER, term_freq INTEGER, corp_freq INTEGER)
       returnType.addVarchar();
       returnType.addInt();
       returnType.addInt();
       returnType.addInt();
   }
};

RegisterFactory(InvertedIndexFactory);

