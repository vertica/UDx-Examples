/*Copyright (c) 2005 - 2016 Hewlett Packard Enterprise Development LP  -*- C++ -*-*/
/****************************
 * Vertica Analytic Database
 *
 * UDL helper/wrapper; allows continuous reading and writing of data,
 * rather than the state-machine-based approach in the stock API.
 * Helper methods and objects.
 *
 ****************************/
#ifdef VALGRIND_BUILD
#include <valgrind/memcheck.h>
#endif

#include <ucontext.h>
#include <memory>
#include <exception>

#include "Vertica.h"
#include "UdfException.h"

#ifndef COROUTINEHELPERS_H_
#define COROUTINEHELPERS_H_

#ifndef VERTICA_INTERNAL
using Vertica::uint8;
using Vertica::int64;
#endif

#ifdef VERTICA_INTERNAL
#include "Session/ThreadDebugContext.h"
#endif

// Can only pass integer args to makecontext. This code wrapes a pointer
// into two integer args in a very non portable way.
// @cond INTERNAL
struct ContextArgHacker {
    ContextArgHacker(void *ptr) : _ptr(ptr) {}
    ContextArgHacker(int a0, int a1) { getArr()[0] = a0; getArr()[1] = a1; }

    int getA0()              { return getArr()[0]; }
    int getA1()              { return getArr()[1]; }
    int *getArr()            { return reinterpret_cast<int*>(&_ptr); }
    void* getPtr() { return _ptr; }

private:
    void *_ptr;
};

// @cond INTERNAL
typedef int (*CoroutineMethod)(void *);

/**
 * Implement the Coroutine design pattern.
 * An instance of this class allows you to switch back
 * and forth between execution contexts at any time.
 */
class Coroutine {
private:
    // Disable the copy constructor -- must pass by reference
    Coroutine(Coroutine &c) { VAssert(false); }
#ifdef VALGRIND_BUILD
    int64 stackid;
#endif

public:
    Coroutine() : stack(NULL), coroutine_exception(NULL) {}

    /**
     * Initialize a new context that will return to the current
     * context once it finishes executing.
     * Give it a DEFAULT_STACK_SIZE initial starting stack.
     */
    void initializeNewContext(Vertica::ServerInterface *srvInterface) {
        // Allocate a secondary stack to run run() on
        stacksize = DEFAULT_STACK_SIZE;
        if (stack == NULL) stack = (uint8*)srvInterface->allocator->alloc(stacksize);

#ifdef VALGRIND_BUILD
    stackid = VALGRIND_STACK_REGISTER(stack, stack+stacksize);
#endif
        // Set up a context that uses the newly-allocated stack
        int stat = getcontext(&pcontext);
        VIAssert(stat == 0);
        pcontext.uc_stack.ss_sp = stack;
        pcontext.uc_stack.ss_size = stacksize;
        pcontext.uc_link = &ccontext;
    }

    /**
     * Kick off the internal coroutine.
     *
     * Note that makecontext() (particularly as provided by some of
     * the older libc versions supported by Vertica) is a very
     * old-school C x86 function.  No OO functions; arguments must
     * all be int32's; etc.  Hence the hackish workaround.
     */
    void start(CoroutineMethod main, void *context) {
        ContextArgHacker fn((void*)main);
        ContextArgHacker args(context);
        makecontext(&pcontext, (void(*)())callFnPtr, 4,
                fn.getA0(), fn.getA1(),
                args.getA0(), args.getA1());
    }

    /**
     * To be called from the UDParser side.
     * Switches back to the run() side, causing switchBack() to return.
     */
    void switchIntoCoroutine() {
#ifdef VERTICA_INTERNAL
        Session::ThreadDebugContext::StackSetter ss((char *)stack, stacksize);
#endif
        int stat = swapcontext(&ccontext, &pcontext);
        VIAssert(stat == 0);
    }

    /**
     * To be called from the run() side.
     * Switches back to the UDParser side, causing switchIntoCoroutine()
     * to return.
     *
     * Require newState as an argument so we don't forget to
     * tell the original context what we want it to do for us
     */
    void switchBack() {
        int stat = swapcontext(&pcontext, &ccontext);
        VIAssert(stat == 0);


        // If we've been instructed to throw an exception, do it now
        if (coroutine_exception != NULL) {
            // Clear out the old exception in case it is caught and handled
            //   by the user code, and we need to do this again later.
        std::exception * exception_to_throw = coroutine_exception;
            coroutine_exception = NULL;

            throw *exception_to_throw;
        }
    }

    /**
     * To be called from the UDParser side.
     * Throws the indicated exception the next time we switch back into the
     * coroutine.
     *
     * If set multiple times before switchIntoCoroutine() is called, only
     * the last such exception will be thrown.
     *
     * Returns the prior-set exception if one was set, or NULL if no
     * exception was queued for throwing.
     */
    std::exception * throw_in_coroutine(std::exception * exception) {
        std::exception * old_exception = coroutine_exception;
        coroutine_exception = exception;
        return old_exception;
    }

    // @cond INTERNAL
    const static size_t DEFAULT_STACK_SIZE = 1 * 1024 * 1024;  // Default to 1mb stack for the run() method

    /** Contexts and stack */
    // WARNING:  If using this example with Valgrind, it may be
    // necessary to invoke the VALGRIND_STACK_REGISTER macro
    // during setup.
    // @cond INTERNAL
    ucontext_t ccontext, pcontext;
    // @cond INTERNAL
    uint8 *stack;
    // @cond INTERNAL
    size_t stacksize;

    // Next time we swap into the coroutine, throw this exception in its context
    // @cond: INTERNAL
    std::exception * coroutine_exception;

    // Part of an x86_64-specific hack used by Coroutine to work around
    // makecontext() being limited to 32-bit pointers by taking two pairs
    // of 32-bit pointer parts and convert them into a pair of 64-bit pointers.
    // @cond INTERNAL
    static int callFnPtr(int fn0, int fn1, int a0, int a1) {
        ContextArgHacker fn(fn0, fn1);
        ContextArgHacker arg(a0, a1);
        return (*(CoroutineMethod)fn.getPtr())(arg.getPtr());
    }
};

class ContinuousStreamer {
private:
    // Disable the copy constructor -- must pass by reference
    ContinuousStreamer(ContinuousStreamer &cs) : bytesConsumed(0), c(cs.c) { VAssert(false); }
    size_t bytesConsumed;

public:
    /**
     * Instantiate a ContinuousStreamer.
     * ContinuousStreamer always need a Coroutine object to work with,
     * to tell when to go get more data, etc.
     */
    ContinuousStreamer(Coroutine &c)
        : bytesConsumed(0), currentBuffer(NULL), state(NULL),
          needInput(false), lastReservationSize(0), stream_state(PORTION_ALIGNED), prev_reserved(0), c(c) {}

    /**
     * @return true iff we have reached the end of the input
     * stream, and no data can be reserved.
     */
    bool isEof() {
                // Are we at the last block, and used up all the data in it? 
        return ((*state == Vertica::END_OF_FILE && currentBuffer->offset == currentBuffer->size) 
                // Or, we know we finished our portion, and should go no further
                || stream_state == DONE);
    }

    /**
     * @return true iff no more data can be reserved.
     * The current reservation can still be valid and of nonzero size, however.
     */
    bool noMoreData() {
                // Are we at the last block?
        return (*state == Vertica::END_OF_FILE
                // Have we reserved all available data in it?
                && lastReservationSize == capacity());
    }

    /**
     * Return a pointer to the currently-available data.
     *
     * This call returns a pointer to the start of a buffer
     * containing the next N bytes of the input stream, where
     * N is equal to the value returned by the last call to
     * reserve().
     *
     * The position in the input stream is determined by
     * the cumulative calls to seek().  Note that calling
     * seek() invalidates any reserved data, and reserve()
     * must be called again.
     */
    void* getDataPtr() {
        return (void*)((uint8_t*)(currentBuffer->buf) + currentBuffer->offset);
    }

    /**
     * If size is less than or equal to capacity(), this call has no effect.
     * Otherwise, it is a request to allocate more memory.
     *
     * Returns `size` if at least `size` bytes could be reserved, or some value
     * smaller than `size`, indicating how many bytes were successfully reserved,
     * if end-of-file was reached and fewer than `size` bytes are available.
     * Check capacity to see how many bytes were reserved.
     */
    size_t reserve(size_t size) {
        while (1) {
            lastReservationSize = size < capacity() ? size : capacity();
            // Break at EOF or if we have enough.
            if (noMoreData() || lastReservationSize >= size) break;
            // Nearing end of a portion, give whatever we have
            if (*state == Vertica::END_OF_PORTION && (stream_state == PORTION_START || stream_state == PORTION_ALIGNED) ) {
                // Note: here isEndOfPortion, as far as udparser is concerned, is stream has seeked past last terminator IN THIS PORTION, and is ready to go into the final one.
                if(prev_reserved == lastReservationSize)  // can't make progress, must have seeked to last record
                    stream_state = PORTION_END;
                prev_reserved = lastReservationSize; 
                break;
            }
            // Read past end-of-chunk as last resort.
            if (*state == Vertica::END_OF_CHUNK && lastReservationSize > 0) break;
            // Switch context to get more input.
            needInput = true;
            c.switchBack();
        }
        return lastReservationSize;
    }

    /**
     * Number of bytes reserved.
     */
    size_t capacity() const {
        return currentBuffer->size - currentBuffer->offset;
    }

    /**
     * Seek forward in the input stream.
     *
     * Attempt to seek forward `distance` bytes.  If this is
     * successful, return `distance`.
     *
     * If this is unsuccessful due to reaching the end of the
     * input stream, return a value less than `distance` that
     * is equal to the number of bytes that have been consumed.
     *
     * Calling seek() invalidates any reserved data; to guarantee
     * that data is still available, it is necessary to call
     * reserve() immediately after calling seek().
     */
    size_t seek(size_t distance) {
        size_t remaining_distance = distance;
        while (*state != Vertica::END_OF_FILE &&
               currentBuffer->size - currentBuffer->offset < remaining_distance)
        {
            // reserve() keeps asking for more data, which
            // results in reserving larger and larger blocks.
            // We do something similar, but we pretend that we
            // fully consume each block as we see it, so that
            // the underlying implementation thinks it can just
            // keep feeding us blocks of this size.
            remaining_distance -= currentBuffer->size - currentBuffer->offset;
            currentBuffer->offset = currentBuffer->size;
            needInput = true;
            if (*state == Vertica::END_OF_PORTION && (stream_state == PORTION_START || stream_state == PORTION_ALIGNED) ) {
                // if this is all we have in this portion and reserve() didn't get to set EndOfPortion(), do it here, otherwise we'd forever lose that info after switching context for more data
                stream_state = PORTION_END;
                bytesConsumed += distance - remaining_distance;
                return distance - remaining_distance; // TODO: does seek ever use return value??
            }
            c.switchBack();
        }

        if (currentBuffer->size - currentBuffer->offset >= remaining_distance) {
            currentBuffer->offset += remaining_distance;
            bytesConsumed += distance;
            return distance;
        } else {
            remaining_distance -= currentBuffer->size - currentBuffer->offset;
            currentBuffer->offset = currentBuffer->size;
            bytesConsumed += distance - remaining_distance;
            return distance - remaining_distance;
        }
    }

    size_t getBytesConsumed() {
        return bytesConsumed;
    }

    bool isPortionStart() {
        if(*state == Vertica::START_OF_PORTION){ // SOP is not allowed to change to EOP before value cached here
            stream_state = PORTION_START; // cache this value, since input state may change to EOP while not yet aligned
            return true;
        }
        else {
            return false;
        }
    }

    void setPortionReady() {
        stream_state = PORTION_ALIGNED;
    }

    bool isPortionEnd() {
        return stream_state == PORTION_END;
    }

    void setNextPortion() {
        stream_state = NEXT_PORTION;
    }

    // fail-fast, although there might be more data to be read -- it doesn't belong to us
    void setPortionFinish() {
        //*state = Vertica::END_OF_FILE; // not useful, as this points to a local var on ContinuousUDParser, that never got passed back to the wrapper
        stream_state = DONE;
        //currentBuffer->offset = currentBuffer->size; // NOTE: dont modify this w/o careful thinking, underlying buffer may be shared with other parsers (e.g. in coop parse)
        c.switchBack();
    }

    // re-set stream_state to default (same as in c'tor)
    void resetStreamState() {
        bytesConsumed = 0;
        currentBuffer = NULL;
        state = NULL;
        needInput = false;
        lastReservationSize = 0;
        stream_state = PORTION_ALIGNED;
        prev_reserved = 0;
    }

    /// @cond INTERNAL
    Vertica::DataBuffer *currentBuffer;

    /// @cond INTERNAL
    Vertica::InputState *state;

    /// @cond INTERNAL
    bool needInput;

    /// @cond INTERNAL
    size_t lastReservationSize;

    /*
     * PORTION_START 
     *    Used when a load stack is apportioned. 
     *    This mode indicates this stream is at the start of a portion, which is not yet aligned at a record begin.
     *
     * PORTION_ALIGNED
     *    Used when a load stack is apportioned. 
     *    This mode indicates this stream is aligned from a portion (including the case when the first portion is already aligned).
     *
     * PORTION_END
     *    Used when a load stack is apportioned. 
     *    This mode indicates this stream is at the end of a portion.
     *
     * NEXT_PORTION
     *    Used when a load stack is apportioned. 
     *    This mode indicates this stream is pointing at content from the next portion.
     *
     * Note: the 4 portion-related stream states above are mostly used to mark internal stream state in coroutine. They vaguely map 
     * to input states, but has subtle difference in its meaning. Coroutine never modifies input_state; 
     * instead, the state machine is captured in stream state.
 */

    typedef enum { DONE = 0, PORTION_START = 1, PORTION_ALIGNED = 2, PORTION_END = 3, NEXT_PORTION = 4 } CoroutineStreamState;
    /// @cond INTERNAL
    CoroutineStreamState stream_state;  // keep internal stream state, aside from input_state (which might get modified in parserwrapper)

    /// @cond INTERNAL
    size_t prev_reserved;

    /// @cond INTERNAL
    Coroutine &c;
};

/**
 * ContinuousReader
 * A ContinuousStreamer intended for reading data
 */
class ContinuousReader : public ContinuousStreamer {
public:
    ContinuousReader(Coroutine& c) : ContinuousStreamer(c) {}
    /**
     * Reads up to `n` bytes from the input stream,
     * and copies them into the input buffer.
     *
     * A wrapper on top of reserve() and seek().
     *
     * Because read() calls seek(), it also invalidates getDataPtr().
     * See the documentation for that function call if using
     * read() and getDataPtr() in the same UDParser implementation.
     *
     * Note that read() is incompatible with row rejection at this time
     * because the row to be rejected is no longer available to Vertica
     * once it has been seek()'ed past.  See the reserve()/seek() API
     * for an alternative that doesn't have this limitation.
     */
    size_t read(void *buf, size_t n) {
        size_t reserved = reserve(n);
        memcpy(buf, getDataPtr(), reserved);
        return seek(reserved);
    }

    std::string getErrorHeader(){
        return std::string("COPY:");
    }
};

/**
 * ContinuousWriter
 * A ContinuousStreamer intended for writing data
 */
class ContinuousWriter : public ContinuousStreamer {
public:
    ContinuousWriter(Coroutine& c) : ContinuousStreamer(c) {}
    /**
     * Writes up to `n` bytes from buf to the output stream.
     *
     * Returns the number of bytes successfully written.
     *
     * Will write exactly `n` bytes unless the output stream closes
     * before the last byte has been written.
     *
     * write() is a wrapper on top of reserve() and seek().
     * Because write() calls seek(), it also invalidates getDataPtr().
     * See the documentation for that function call if using
     * write() and getDataPtr() in the same UDParser implementation.
     *
     * Note that write() is incompatible with row rejection at this time
     * because the row to be rejected is no longer available to Vertica
     * once it has been seek()'ed past.  See the reserve()/seek() API
     * for an alternative that doesn't have this limitation.
     */
    size_t write(const void *buf, size_t n) {
        size_t reserved = reserve(n);
        memcpy(getDataPtr(), buf, reserved);
        return seek(reserved);
    }
};

class ContinuousRejecter {
private:
    // Disable the copy constructor -- must pass by reference
    ContinuousRejecter(ContinuousRejecter &cs) : c(cs.c) { VAssert(false); }

public:
    /**
     * CoroutineRejecter constructor
     * Capture a Coroutine instance to let us communicate with the server
     */
    ContinuousRejecter(Coroutine &c) : haveRejectedRecord(false), c(c) {}

    /**
     * Request that Vertica process the current raw data as a rejected row.
     * Emit the rejected row via the rejected-rows mechanism specified in
     * the COPY statement, with the error message specified by `reason`,
     * and delimited by the record terminator specified by `recordTerminator`.
     */
    void reject(const Vertica::RejectedRecord &rr) {
        rejectedRecord = rr;
        haveRejectedRecord = true;
        c.switchBack();
    }

protected:
    Vertica::RejectedRecord rejectedRecord;
    bool haveRejectedRecord;

    Coroutine &c;

    friend class ContinuousUDParser;
};

#endif // COROUTINE_HELPERS_H_
