/* Copyright (c) 2005 - 2016 Hewlett Packard Enterprise Development LP  -*- C++ -*-*/

/* 
 *
 * Description: Example User Defined Scalar Function: TextConvert
 *
 * Create Date: Jul 30, 2014
 */
#include "Vertica.h"

using namespace Vertica;
#include <string>
#include <stdlib.h>
#include <iconv.h>

/*
 * This is a simple function that converts a string from one encoding to another
 */
class TextConvert : public ScalarFunction
{

private:
    std::string fromEncoding, toEncoding;
    iconv_t cd; // the conversion descriptor opened
    char * output_buf;    

public:
    const static int size_of_out_buff=65000; //Giving max size to output buffer

    // Add setup method , and use alloc to allocate the buffer 
    virtual void setup (ServerInterface &srvInterface, const SizedColumnTypes &argTypes) 
    {
       output_buf=(char *)srvInterface.allocator->alloc(size_of_out_buff);
       // note "to encoding" is first argument to iconv...
        cd = iconv_open(toEncoding.c_str(), fromEncoding.c_str());
        if (cd == (iconv_t)(-1))
        {
            // error when creating converters.
            vt_report_error(0, "Error initializing iconv: %m");
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
            if (argReader.getNumCols() != 1)
                vt_report_error(0, "Function only accept 1 arguments, but %zu provided", 
                                argReader.getNumCols());

            // While we have inputs to process
            do {
                // Get a copy of the input string
                VString inStr = argReader.getStringRef(0);
                char *input_buf=inStr.data();
                size_t inBytesLeft = inStr.length() , outBytesLeft = size_of_out_buff;
                //Create a temp_buff because when iconv is called it takes the pointer to the end. 
                char *temp_buf=output_buf;
                size_t ret = iconv(cd, &input_buf, &inBytesLeft, &temp_buf, &outBytesLeft);
                
                if (ret == (size_t)(-1)){
                    // seen an error
                    switch (errno)
                    {
                        case E2BIG:
                            // input size too big, not a problem, ask for more output.
                            vt_report_error(1, "Input size too big");
                            break;
                        case EINVAL:
                            // input stops in the middle of a byte sequence 
                            vt_report_error(1, "Input stops in the middle of byte sequence");
                            break;
                        case EILSEQ:
                            // invalid sequence seen, throw
                            // TODO: reporting the wrong byte position
                            vt_report_error(1, "Invalid byte sequence when doing conversion");
                        case EBADF:
                            // something wrong with descriptor, throw
                            vt_report_error(0, "Invalid descriptor");
                        default:
                            vt_report_error(1,"Uncommon error");
                            break; 
                     }
                } else { 
                    // Copy string into results
                    resWriter.getStringRef().copy(output_buf,size_of_out_buff-outBytesLeft);
                    resWriter.next();
                    ret = iconv(cd,NULL,0,NULL,0);
                }
           } while (argReader.next());
        } catch(std::exception& e) {
            // Standard exception. Quit.
            vt_report_error(0, "Exception while processing block: [%s]", e.what());
        }
    }

    //Constructor needed to get from and to encodings
    TextConvert(const std::string &from, const std::string &to)
    : fromEncoding(from), toEncoding(to)    
    {
         
    }

    virtual void destroy(ServerInterface &srvInterface,const SizedColumnTypes &argTypes)
    {
        // free iconv resources;
        iconv_close(cd);
    }


};

class TextConvertFactory : public ScalarFunctionFactory
{
    // return an instance of TextConvert to do the actual text conversion.
    virtual ScalarFunction *createScalarFunction(ServerInterface &srvInterface)
    { 
       std::vector<std::string> args = srvInterface.getParamReader().getParamNames();

        /* Check parameters */
        if (!(args.size() == 0 ||
                (args.size() == 1 && find(args.begin(), args.end(), "from_encoding") != args.end()) ||
                (args.size() == 2
                        && find(args.begin(), args.end(), "from_encoding") != args.end()
                        && find(args.begin(), args.end(), "to_encoding") != args.end()))) {
            vt_report_error(0, "Invalid arguments.  Must specify either no arguments, or 'from_encoding' alone, or 'from_encoding' and 'to_encoding'.");
        }
   
        std::string from_encoding="UTF-16";
        std::string to_encoding="UTF-8";
         
        if (args.size() == 2)
        {
            from_encoding=srvInterface.getParamReader().getStringRef("from_encoding").str();
            to_encoding=srvInterface.getParamReader().getStringRef("to_encoding").str();
        }
        else if (args.size() == 1)
        {
            from_encoding=srvInterface.getParamReader().getStringRef("from_encoding").str();
        }

        if (from_encoding.empty()) {
            vt_report_error(0, "The empty string is not a valid from_encoding value");
        }
        if (to_encoding.empty()) {
            vt_report_error(0, "The empty string is not a valid to_encoding value");
        }
      
      
        return vt_createFuncObject<TextConvert>(srvInterface.allocator,from_encoding,to_encoding); 
    }

    virtual void getPrototype(ServerInterface &interface,
                              ColumnTypes &argTypes,
                              ColumnTypes &returnType)
    {
        argTypes.addVarchar();
        returnType.addVarchar();
    }

    virtual void getReturnType(ServerInterface &srvInterface,
                               const SizedColumnTypes &argTypes,
                               SizedColumnTypes &returnType)
    {
        returnType.addVarchar(TextConvert::size_of_out_buff);
    }

    virtual void getParameterType(ServerInterface &srvInterface,
                                  SizedColumnTypes &parameterTypes) {
        parameterTypes.addVarchar(32, "from_encoding");
        parameterTypes.addVarchar(32, "to_encoding");
    }
    
};

RegisterFactory(TextConvertFactory);

