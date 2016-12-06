/* Copyright (c) 2005 - 2016 Hewlett Packard Enterprise Development LP  -*- C++ -*-*/
/****************************
 * Vertica Analytic Database
 *
 * UDL helper/wrapper; allows continuous reading and writing of data,
 * rather than the state-machine-based approach in the stock API.
 * Parser implementation.
 *
 ****************************/

#include "CoroutineHelpers.h"

#ifndef CONTINUOUSUDSOURCE_H_
#define CONTINUOUSUDSOURCE_H_

/**
 * ContinuousUDSource
 *
 * This is a wrapper on top of the UDSource class.  It provides an abstraction
 * that allows user code to treat the output as a continuous stream of data,
 * that they can write to at will and process in a continuous manner
 * rather than having an iterator-like design where a user function is called
 * repeatedly to get more data.
 */
class ContinuousUDSource : public Vertica::UDSource {
using Vertica::UDSource::destroy;
public:
    // Functions to implement

    /**
     * ContinuousUDSource::initialize()
     *
     * Will be invoked during query execution, prior to run() being called.
     *
     * May optionally be overridden to perform setup/initialzation.
     */
    virtual void initialize(Vertica::ServerInterface &srvInterface) {}

    /**
     * ContinuousUDSource::run()
     *
     * User-implemented method that processes data.
     * Called exactly once per ContinuousUDParser instance.
     * It should write data using the reserve() and seek()
     * methods on the 'cw' field.
     * It should return once it has finished emitting data.
     *
     * run() should be very careful about keeping pointers or references
     * to internal values returned by methods on the ContinuousUDParser
     * class.  Unless documentation explicitly indicates that it is
     * safe to do so, it is not safe to keep such pointers or references,
     * even for objects (such as the server interface) that one might
     * otherwise expect to be persistent.  Instead, call the accessor
     * function for each use.
     */
    virtual void run() {}

    /**
     * ContinuousUDSource::deinitialize()
     *
     * Will be invoked during query execution, after run() has returned.
     *
     * May optionally be overridden to perform tear-down/destruction.
     */
    virtual void deinitialize(Vertica::ServerInterface &srvInterface) {}

protected:
    // Functions to use

    /**
     * Get the current ServerInterface.
     *
     * Do not store the return value of this function.  Other
     * function calls on ContinuousUDParser may change
     * the server interface that it returns.
     */
    Vertica::ServerInterface& getServerInterface() { return *srvInterface; }

    /**
     * Return control to the server.
     * Use this method in idle or busy loops, to allow the server to
     * check for status changes or query cancellations.
     */
    void yield() {
        state = RUN_START;
        c.switchBack();
    }

    /**
     * ContinuousWriter
     * Houses methods relevant to writing raw binary buffers.
     * 
     * NOTE: The methods on ContinuousWriter will throw an exception if the
     * operation has been externally canceled.  Ensure that you clean up
     * your resources appropriately.
     */
    ContinuousWriter cw;

private:

    /****************************************************
     ****************************************************
     ****************************************************
     *
     * All code below this point is a part of the internal implementation of
     * this class.  If you are simply trying to write a ContinuousUDParser,
     * it is unnecessary to read beyond this point.
     *
     * However, the following is also presented as example code, and can be
     * taken and modified if/as needed.
     *
     ***************************************************
     ***************************************************
     ***************************************************/

    Vertica::ServerInterface *srvInterface;

    std::auto_ptr<udf_exception> exception;

    Coroutine c;

    /** State of the internal worker context */
    enum State
    {
        NOT_INITIALIZED, // Coroutine context not in existence
        RUN_START,    // Coroutine running
        FINISHED,     // Coroutine has declared that it's done processing data
        CLOSED,       // Coroutine cleanup finished
        ABORTED,       // Coroutine threw an exception
    } state;

protected:
    static int _ContinuousUDSourceRunner(ContinuousUDSource *source) {
        // Internal error, haven't set a parser but are trying to run it
        VIAssert(source != NULL);

        try {
            source->runHelper();
        } catch (udf_exception &e) {
            source->exception.reset(new udf_exception(e));
            source->state = ABORTED;
        } catch (...) {
            source->exception.reset(new udf_exception(0, "Unknown exception caught", __FILE__, __LINE__));
            source->state = ABORTED;
        }

        return 0;
    }

    /**
     * Called by _ContinuousUDParserRunner() to handle immediate
     * set-up and tear-down for run()
     */
    void runHelper() {
        run();
        state = FINISHED;
    }

public:
    // Constructor.  Initialize stuff properly.
    // In particular, various members need access to our Coroutine.
    // Also, initialize POD types to
    ContinuousUDSource() : cw(c), srvInterface(NULL) {}

    // Wrap UDParser::setup(); we have some initialization of our own to do
    void setup(Vertica::ServerInterface &srvInterface) {
        state = NOT_INITIALIZED;

        // Set as many of (srvInterface, currentBuffer, inputState)
        // to NULL for as much of the time as possible.
        // Makes things more fail-fast, if someone tries to use
        // one of them and it's not currently defined/valid,
        // since they are invalidated frequently.
        this->srvInterface = &srvInterface;

        c.initializeNewContext(&getServerInterface());

        initialize(srvInterface);

        // Initialize the coroutine stack
        c.start((int(*)(void*))_ContinuousUDSourceRunner, (void*)this);
        state = RUN_START;

        this->srvInterface = NULL;
    }

    // Wrap UDParser::destroy(); we have some tear-down of our own to do
    void destroy(Vertica::ServerInterface &srvInterface) {

        this->srvInterface = &srvInterface;
        // If run() hasn't finished, it is currently suspended.  To ensure
        //    that everything allocated on its stack is freed (including any
        //    resources, such as TCP sockets), we will explicitly throw an
        //    exception in its current frame.
        //
        // The client code should either propagate the error up and unwind
        //    its stack (in which case the state will end up ABORTED), or it
        //    will catch the exception and try to continue, in which case it is
        //    ill-behaved, at which point we will error out this UDx
        if (state == RUN_START) {
            this->srvInterface->log("UDx canceled.  Throwing cancellation in UDx");
            c.throw_in_coroutine(new udf_exception(0, "Operation canceled", __FILE__, __LINE__));
            c.switchIntoCoroutine();
        }
        
        // If the run() method didn't return after we threw the exception, 
        //   shut down the UDx with a failed assertion
	VIAssert(state == FINISHED || state == ABORTED);
        deinitialize(srvInterface);
        this->srvInterface = NULL;
        state = CLOSED;
    }

    /**
     * Override the built-in process() method with our own logic.
     * Abstract away the state-machine interface by stuffing run() into a
     * coroutine.
     * Whenever run() wants more data than we have right now,
     * context-switch out of the coroutine and back into process(), and
     * go get whatever run() needs.
     */
    Vertica::StreamState process(Vertica::ServerInterface &srvInterface,
            Vertica::DataBuffer &output) {
        // Capture the new state for this run
        // IMPORTANT:  It is unsafe to access any of these values outside
        // of this function call!
        // So be careful that run() is never running while we're not inside
        // a process() call.
        this->srvInterface = &srvInterface;
        this->cw.currentBuffer = &output;
        // Fake output state
        Vertica::InputState output_state = Vertica::OK;
        this->cw.state = &output_state;

        // setup() is supposed to take care of getting us out of this state
        VIAssert(state != NOT_INITIALIZED && state != FINISHED);

        // Pass control to the user.
        c.switchIntoCoroutine();

        // Don't need these any more; clear them to make sure they
        // don't get used improperly
        this->srvInterface = NULL;
        this->cw.currentBuffer = NULL;
        this->cw.state = NULL;

        // Propagate exception.
        if (exception.get()) {
            Vertica::vt_throw_exception(exception->errorcode, exception->what(), exception->filename, exception->lineno);
        }

        if (this->cw.needInput) {
            this->cw.needInput = false;
            return Vertica::OUTPUT_NEEDED;
        }

        if (state == RUN_START) {
            return Vertica::KEEP_GOING;
        }

        // We should only be able to get here if we're finished
        VIAssert(state == FINISHED);
        return Vertica::DONE;
    }
};


#endif  // CONTINUOUSUDSOURCE_H_
