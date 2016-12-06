/* Copyright (c) 2005 - 2016 Hewlett Packard Enterprise Development LP  -*- C++ -*-*/

/* 
 *
 * Description: Example Vertica Distributed File System with User Defined Scalar Functions. Add 2 ints
 * 		and write to the given file, if it does not exist. Otherwise read from file and write contents
 * 		to the output.
 *
 * Create Date: Apr 29, 2013
 */
#include "Vertica.h"
#include "VerticaUDx.h"
#include "VerticaDFS.h"

class exception;
using namespace Vertica;


const std::string FILE_PATH = "file_path";
/*
 * This is a simple function that adds two integers, save it to a file and return the result or read
 * previous results from a file.
 */
class DFSWriteRead : public ScalarFunction
{
public:

    DFSFile file;
    DFSFileWriter fileWriter;
    DFSFileReader fileReader;

    virtual void setup (ServerInterface &srvInterface, const SizedColumnTypes &argTypes) 
    {
        
        // Get file name from the input parameters
        std::string filePath = srvInterface.getParamReader().containsParameter(FILE_PATH)? srvInterface.getParamReader().getStringRef(FILE_PATH).str():"/vdfs/dfswriteread/results1";
        // Initiate a file.
        file = DFSFile(srvInterface, filePath);
	// If file already exists, open for read
        if (file.exists())
	{
	  fileReader = DFSFileReader(file);
	  fileReader.open();
        }
	// Otherwise create and open for write.
	else
	{
	  file.create(NS_GLOBAL, HINT_REPLICATE);
          fileWriter = DFSFileWriter(file);
	  fileWriter.open();
	}
    }
 
   virtual void destroy (ServerInterface &srvInterface, const SizedColumnTypes &argTypes) 
   {
       // Close if file is opened for reading or writing. 
        if (fileReader.isOpen())
	{
	  fileReader.close();
	}
	if (fileWriter.isOpen())
	{
	  fileWriter.close();
   	} 
   }

    /*
     * This method processes a block of rows in a single invocation.
     *
     * The inputs are retrieved via argReader
     * The outputs are returned via resWriter
     */
    virtual void processBlock(ServerInterface &srvInterface,
                              BlockReader &argReader,
                              BlockWriter &resWriter)
    {
        try {
            // Basic error checking
            if (argReader.getNumCols() != 2)
                vt_report_error(0, "Function only accept 2 arguments, but %zu provided", 
                                argReader.getNumCols());

        char buff[8];
        // If file is opened for read, read resulsts from previous run and write to the log file.
	if (fileReader.isOpen())
	{
	  size_t result;
	  do
	  {
	    result = fileReader.read(buff, sizeof(buff));
            srvInterface.log("Contents of the file read is  %s", buff);
	  } while (result > 0);
	}
 	    // While we have inputs to process
            do {
                const vint a = argReader.getIntRef(0);
                const vint b = argReader.getIntRef(1);
		resWriter.setInt(a+b);
		sprintf(buff,"%llu\n",a+b);
	        // If file is open for write, write resulsts to the file as well.
		if (fileWriter.isOpen())
		{  
		   fileWriter.write(buff, sizeof(buff));
                }
		resWriter.next();
            } while (argReader.next());


        } catch(std::exception& e) {
            // Standard exception. Quit.
            vt_report_error(0, "Exception while processing block: [%s]", e.what());
        }
    }
};

class DFSWriteReadFactory : public ScalarFunctionFactory
{
    // return an instance of DFSWriteRead to perform the actual operations.
    virtual ScalarFunction *createScalarFunction(ServerInterface &interface)
    { 
	return vt_createFuncObj(interface.allocator, DFSWriteRead); 
    }

    virtual void getPrototype(ServerInterface &interface,
                              ColumnTypes &argTypes,
                              ColumnTypes &returnType)
    {
        argTypes.addInt();
        argTypes.addInt();
        returnType.addInt();
    }
    
    virtual void getParameterType(ServerInterface &srvInterface,
                                  SizedColumnTypes &parameterTypes)
    {
        parameterTypes.addVarchar(256, FILE_PATH);
    }

};

RegisterFactory(DFSWriteReadFactory);
