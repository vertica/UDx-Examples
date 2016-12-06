/* Copyright (c) 2005 - 2016 Hewlett Packard Enterprise Development LP  -*- C++ -*-*/

#ifndef VPDISTLIB
#define VPDISTLIB

#include <vector>
#include <string>
#include "CoroutineStream.h"

#include <curl/curl.h>

struct VDistStream;

/**
 * VP Dist library functions -- manages interactions between the client and the vpdist server
 */
struct VDistLib
{

    // Example URL: http://localhost:8000
    VDistLib(const std::string &url);
    ~VDistLib();

    void open();
    void close();

    // List all files available on the server
    std::vector<std::string> getFiles(std::string suffix = "/list");

    // Open a URL for reading. Callers responsibility to free the returned stream.
    VDistStream* getFile(const std::string &filename);

private:
    std::string _url;

    VDistStream* openURL(const std::string &url);
};

// RAII for cURL instance; will always call curl_easy_cleanup when it goes
//    out of scope
struct CurlHandle
{
    CURL *curl;
    CurlHandle() : curl(curl_easy_init()) {}
    ~CurlHandle() { curl_easy_cleanup(curl);}
    operator CURL *(void) const { return curl; }
};


typedef size_t (*CurlCallbackFn)(void *contents, size_t size, size_t nmemb, void *userp);
// Adapter that invokes curl and feeds data as necessary
class VDistStreamProducer : public StreamProducer 
{
public:
    VDistStreamProducer(const std::string &url) : _url(url) {}
    virtual void produce(CoroutineStream &str);

    template <class StreamType>
    void produce(StreamType &vp_str, CurlCallbackFn fn) {
        /* init the curl session */
        CurlHandle curl_handle;
        
        curl_easy_setopt(curl_handle, CURLOPT_VERBOSE, 1);

        /* specify URL to get */
        curl_easy_setopt(curl_handle, CURLOPT_URL, _url.c_str());

        /* send all data to this function  */
        curl_easy_setopt(curl_handle, CURLOPT_WRITEFUNCTION, fn);

        /* we pass our 'chunk' struct to the callback function */
        curl_easy_setopt(curl_handle, CURLOPT_WRITEDATA, (void *)&vp_str);

        /* some servers don't like requests that are made without a user-agent
           field, so we provide one */
        curl_easy_setopt(curl_handle, CURLOPT_USERAGENT, "libcurl-agent/1.0");

        /* Set error bufferer */
        curl_easy_setopt(curl_handle, CURLOPT_ERRORBUFFER, vp_str._errorMessage);

        /* get it! */
        vp_str._errorCode = curl_easy_perform(curl_handle);
        
        // CurlHandle will clean itself up when it goes out of scope
    }

private:
    static size_t
    WriteMemoryCallback(void *contents, size_t size, size_t nmemb, void *userp);

    std::string _url;
};

struct VDistStreamException : public std::exception 
{
    VDistStreamException(const std::string &msg) : _msg(msg) {}
    virtual ~VDistStreamException() throw () {}
    const std::string getMessage() const { return _msg; }
    std::string _msg;
};

/** 
 * Interface that gets called as data is retrieved from the network.
 */
struct VDistStream : public CoroutineStream
{
    VDistStream(VDistStreamProducer *p) : 
        CoroutineStream(*p), _producer(p), _errorCode(0) {}
    ~VDistStream() { delete _producer; }
    // Example of use:
    // while (const StreamChunk *c = s.peekChunk()) {
    //     std::cout << "main stream read "
    //               << c->size() << " bytes of data: " 
    //               << c->str() << std::endl;
    //     s.consumeChunk();
    // }

    // Throws exception if stream error
    virtual const StreamChunk *peekChunk();
    

private:
    VDistStreamProducer *_producer;
    int                   _errorCode;
    char                  _errorMessage[1024];
    friend class VDistStreamProducer;
};



#endif /* #define VPDISTLIB */
