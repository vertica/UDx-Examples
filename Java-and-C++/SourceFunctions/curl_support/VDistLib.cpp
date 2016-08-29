/* Copyright (c) 2005 - 2015 Hewlett Packard Enterprise Development LP  -*- C++ -*-*/

#include "VDistLib.h"
#include <sstream>

#include "Vertica.h"
#include <curl/curl.h>







//////////////////////////////////////////////

VDistLib::VDistLib(const std::string &url) : _url(url) 
{
}


VDistLib::~VDistLib()
{
}

    
void VDistLib::open()
{
}


void VDistLib::close()
{
}


std::vector<std::string> VDistLib::getFiles(std::string suffix)
{
    const std::string request_url = _url + suffix;
    VDistStream *str = openURL(request_url);

    try {
        // copy the entire thing into a stringstream
        std::stringstream ss;
        while (const StreamChunk *c = str->peekChunk()) {
            ss.write(c->data(), c->size());
            str->consumeChunk();
        }


        std::vector<std::string> ret;
        while (!ss.eof()) {
            std::string t;
            ss >> t;
            if (!t.empty()) {
                ret.push_back(t);
            }
        }
        delete str;
        return ret;

    } catch (...) {
        delete str;
        throw;
    }
}

VDistStream* VDistLib::getFile(const std::string &filename)
{
    std::stringstream ss;
    ss << _url << "/get?fname=" << filename;

    return openURL(ss.str());
}

// Global structure to initialize/cleanup curl library
struct CurlHolder 
{
    CurlHolder() {
        curl_global_init(CURL_GLOBAL_ALL);
    }
    ~CurlHolder() {
        /* we're done with libcurl, so clean it up */ 
        curl_global_cleanup();
    }

} gCurl;

void VDistStreamProducer::produce(CoroutineStream &str) {
    produce(static_cast<VDistStream &>(str), WriteMemoryCallback);
}

size_t
VDistStreamProducer::WriteMemoryCallback(void *contents, size_t size, size_t nmemb, void *userp)
{
    CoroutineStream *str = (CoroutineStream *)userp;
    const size_t realsize = size * nmemb;
    const char*  realdata = static_cast<char*>(contents);
    str->write(realdata, realsize);
    return realsize;
}



VDistStream* VDistLib::openURL(const std::string &url)
{
    return new VDistStream(new VDistStreamProducer(url));
}

const StreamChunk *VDistStream::peekChunk()
{
    if (_errorCode != 0) {
        throw VDistStreamException(_errorMessage);
    }
    return CoroutineStream::peekChunk();
}
