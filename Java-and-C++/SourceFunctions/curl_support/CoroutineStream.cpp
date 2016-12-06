/* Copyright (c) 2005 - 2016 Hewlett Packard Enterprise Development LP  -*- C++ -*-*/

#include "CoroutineStream.h"
#include <string.h>

#ifdef VERTICA_INTERNAL
#include "Session/ThreadDebugContext.h"
#endif

void BaseStream::write(const char *data, size_t sz)
{
    const char *p = data;
    size_t remain = sz;

    // while we have data remaining...
    while (remain > 0) {
        // allocate new chunk if necessary
        if (chunks.empty() || chunks.back().avail() == 0) {
            chunks.push_back(StreamChunk());
        }
        
        // Copy
        StreamChunk &c = chunks.back();
        size_t cpySz =  c.avail() < remain ? c.avail() : remain;
        char *writep = c.buf+c.sz;
        memcpy(writep, p, cpySz);
        

        // update our pointers and remaining counts
        c.sz   += cpySz;
        remain -= cpySz;
        p      += cpySz;
    }
}

const StreamChunk *BaseStream::peekChunk()
{
    return (chunks.empty()) ? NULL : &chunks.front();
}

// Consumes the current chunk (or no op if stream is empty);
void BaseStream::consumeChunk()
{
    if (!chunks.empty()) chunks.pop_front(); 
}



void CoroutineStream::producerShim(int a0, int a1) 
{
    // get the arguments back into a pointer
    ContextArgHacker hacker(a0,a1);
    CoroutineStream *_this = hacker.getPtr();

    _this->_producer.produce(*_this);
    _this->_producerDone = true;
}

CoroutineStream::CoroutineStream(StreamProducer &prod) : 
    _producer(prod), 
    _producerDone(false), 
    _producerStarted(false)
{
}


void CoroutineStream::write(const char *data, size_t sz)
{
    // This is getting called from the producer. We only write if there is no
    // more data in the stream (or maybe eventually we'll limit the size)
    if (!empty()) {
        // swap back to consumer context to consume data until more data is needed
        swapcontext(&producer_context, &consumer_context);
    }
    // now pass it along to the underlying stream
    BaseStream::write(data, sz);
}

const StreamChunk *CoroutineStream::peekChunk()
{
    // If stream is empty or we only have a single (potentially half full
    // buffer), get more by running the producer
    if (!BaseStream::peekChunk() || this->size() == 1) {
        if (!_producerStarted) {
            ContextArgHacker hacker(this);

            getcontext(&producer_context);
            producer_context.uc_stack.ss_sp   = _pstack;
            producer_context.uc_stack.ss_size = sizeof(_pstack);
            producer_context.uc_link = &consumer_context;
            makecontext(&producer_context, 
                        reinterpret_cast<void (*)()>(producerShim), 
                        2, hacker.getA0(), hacker.getA1());
            _producerStarted = true;
        } 

        // if the producer isn't done, call it to give a chance to make more
        if (!_producerDone) {
#ifdef VERTICA_INTERNAL
            Session::ThreadDebugContext::StackSetter ss((char *)stack, stacksize);
#endif

            swapcontext(&consumer_context, &producer_context);
        } 
        // producer is done, no more data is coming
    }
    

    return BaseStream::peekChunk();
}
