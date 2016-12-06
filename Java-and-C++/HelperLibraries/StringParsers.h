/* Copyright (c) 2005 - 2016 Hewlett Packard Enterprise Development LP  -*- C++ -*-*/
/****************************
 * Vertica Analytic Database
 *
 * Helper functions and objects to parse strings into the various internal Vertica formats
 *
 ****************************/

#include <errno.h>
#include <strings.h>
#include <alloca.h>
#include <time.h>

#include "Vertica.h"


using namespace Vertica;

#ifndef STRINGPARSERS_H_
#define STRINGPARSERS_H_

// Internal struct; used in parseInterval() and parseIntervalYM()
struct IntervalIdentifier {
    const uint64_t count_per;
    const size_t label_length;
    const char *label;
};

// Given a char* and a length, provide a null-terminated string.
// Sometimes, when parsing an input file, this can be achieved by swapping the
// character following the string with a null byte.  Sometimes not.
// Do something reasonable either way.
class NullTerminatedString {
private:
    char *str;
    size_t len;
    bool canAppendNull;
    char replacedChar;

public:
    NullTerminatedString(char *str, size_t len, bool stripWhitespace = false,
            bool canAppendNull = false) :
        str(str), len(len), canAppendNull(canAppendNull) {
        if (stripWhitespace) {
            while (isspace(*str) && len > 0) {
                ++str;
                --len;
            }
            while (isspace(str[len - 1]) && len > 0) {
                --len;
            }
            this->str = str;
            this->len = len;
        }

        if (canAppendNull) {

        } else {
            // If we can't stick a null character after the string,
            // just copy it.
            this->str = new char[len + 1];
            memcpy(this->str, str, len);
        }
        replacedChar = this->str[len];
        this->str[len] = '\0';
    }

    ~NullTerminatedString() {
        this->str[len] = replacedChar;
        if (!canAppendNull) {
            delete[] this->str;
        }
    }

    char *ptr() {
        return str;
    }
    size_t size() {
        return len;
    }
};

/**
 * Parse the given string data into the given
 * column number of the specified type.
 *
 * Templatized on the parser class to use.
 * The parser class in question should be either
 * the StringParsers class or a subclass of it,
 * with a valid default constructor.
 *
 * This design allows creating subclasses of
 * StringParsers that override non-virtual methods,
 * allowing for easy customization without
 * paying the performance penalty of using
 * virtual methods.  For small fields, this penalty
 * can be large as compared to the total cost
 * of parsing a field.
 */
template<class StringParsersImpl>
bool parseStringToType(char *str, size_t len, size_t colNum,
        const Vertica::VerticaType &type, Vertica::StreamWriter *writer,
        StringParsersImpl &sp) {
    bool retVal;

    switch (type.getTypeOid()) {
        case BoolOID: {
            Vertica::vbool val(false);
            retVal = sp.parseBool(str, len, colNum, val, type);
            writer->setBool(colNum, val);
            break;
        }
        case Int8OID: {
            Vertica::vint val(0);
            retVal = sp.parseInt(str, len, colNum, val, type);
            writer->setInt(colNum, val);
            break;
        }
        case Float8OID: {
            Vertica::vfloat val(0);
            retVal = sp.parseFloat(str, len, colNum, val, type);
            writer->setFloat(colNum, val);
            break;
        }
        case CharOID: {
            Vertica::VString val = writer->getStringRef(colNum);
            retVal = sp.parseChar(str, len, colNum, val, type);
            break;
        }
        case VarcharOID: case LongVarcharOID: {
            Vertica::VString val = writer->getStringRef(colNum);
            retVal = sp.parseVarchar(str, len, colNum, val, type);
            break;
        }
        case DateOID: {
            Vertica::DateADT val(0);
            retVal = sp.parseDate(str, len, colNum, val, type);
            writer->setDate(colNum, val);
            break;
        }
        case TimeOID: {
           TimeADT val(0);
           retVal = sp.parseTime(str, len, colNum, val, type);
           writer->setTime(colNum, val);
           break;
        }
        case TimestampOID: {
            Vertica::Timestamp val(0);
            retVal = sp.parseTimestamp(str, len, colNum, val, type);
            writer->setTimestamp(colNum, val);
            break;
        }
        case TimestampTzOID: {
            Vertica::TimestampTz val(0);
            retVal = sp.parseTimestampTz(str, len, colNum, val, type);
            writer->setTimestampTz(colNum, val);
            break;
        }
        case IntervalOID: {
            Vertica::Interval val(0);
            retVal = sp.parseInterval(str, len, colNum, val, type);
            writer->setInterval(colNum, val);
            break;
        }
        case IntervalYMOID: {
            Vertica::IntervalYM val(0);
            retVal = sp.parseIntervalYM(str, len, colNum, val, type);
            writer->setInterval(colNum, val);
            break;
        }
        case TimeTzOID: {
            Vertica::TimeTzADT val(0);
            retVal = sp.parseTimeTz(str, len, colNum, val, type);
            writer->setTimeTz(colNum, val);
            break;
        }
        case NumericOID: {
            Vertica::VNumeric val = writer->getNumericRef(colNum);
            retVal = sp.parseNumeric(str, len, colNum, val, type);
            break;
        }
        case VarbinaryOID: case LongVarbinaryOID: {
            Vertica::VString val = writer->getStringRef(colNum);
            retVal = sp.parseVarbinary(str, len, colNum, val, type);
            break;
        }
        case BinaryOID: {
            Vertica::VString val = writer->getStringRef(colNum);
            retVal = sp.parseBinary(str, len, colNum, val, type);
            break;
        }
        default:
            vt_report_error(0, "Error, unrecognized type: '%s'", type.getTypeStr());
            retVal = false;
    }

    return retVal;
}

/**
 * StringParsers
 *
 * Sample parsers; convert from strings to the specified types.
 *
 * Note that these parser functions are intended as examples!
 * They may require modification for any particular use case.
 *
 * Also, while they bear a lot of resemblance to the internal
 * Vertica string-conversion routines, they are not guaranteed
 * to behave in an identical manner.  If exact behavior is
 * required, the internal converters can be invoked directly via
 * a COPY expression, casting the parser output to the desired
 * data type.  Or, just use the built-in Vertica parsers directly,
 * if possible for the file format in question.
 *
 * Note that these functions take null-terminated C strings
 * rather than std::strings for performance reasons:  This allows
 * using the data directly out of the input-stream block rather
 * than making a copy.  See the NullTerminatedString class for
 * an example of how to handle null-termination of data that
 * isn't naturally null-terminated.
 */
class StringParsers {

public:

    /**
     * Parse a string to a boolean
     */
    bool parseBool(char *str, size_t len, size_t colNum, Vertica::vbool &target, const Vertica::VerticaType &type) {
        if (isNull(str, len)) {
            target = Vertica::vbool_null;
            return Vertica::vbool_true;
        }

        if (len == 1) {
            switch (str[0]) {
            case 'T':
            case 't':
            case 'Y':
            case 'y':
            case '1':
                target = Vertica::vbool_true;
                return true;
            case 'F':
            case 'f':
            case 'N':
            case 'n':
            case '0':
                target = Vertica::vbool_false;
                return true;
            default:
                return false;
            }
        } else {
            if (len == 2 && strncasecmp(str, "no", 2) == 0) {
                target = Vertica::vbool_false;
                return true;
            } else if (len == 3 && strncasecmp(str, "yes", 3) == 0) {
                target = Vertica::vbool_true;
                return true;
            } else if (len == 4 && strncasecmp(str, "true", 4) == 0) {
                target = Vertica::vbool_true;
                return true;
            } else if (len == 5 && strncasecmp(str, "false", 5) == 0) {
                target = Vertica::vbool_false;
                return true;
            } else {
                return false;
            }
        }
    }

    /**
     * Parse a string to an integer, in the specified base
     */
    bool parseInt(char *str, size_t len, size_t colNum, Vertica::vint &target, const Vertica::VerticaType &type, int base = 10) {
        if (isNull(str, len)) {
            target = Vertica::vint_null;
            return true;
        }

        char* endval = str;
        errno = 0;
        target = strtoll(str, &endval, base);

        // Check all the various error conditions of strtoll
        return errno == 0 && target != Vertica::vint_null
                && str + len == endval;
    }

    /**
     * Parse a string to a floating-point number
     */
    bool parseFloat(char *str, size_t len, size_t colNum, Vertica::vfloat &target, const Vertica::VerticaType &type) {
        if (isNull(str, len)) {
            target = Vertica::vfloat_null;
            return true;
        }

        if (len == 0)
            return false; // Must actually have data in a float

        if (len >= 3 && strncasecmp(str, "nan", 3) == 0) {
            target = Vertica::vfloat_NaN;
            return true;
        }

        char *end = str;

        errno = 0;
        target = strtod(str, &end) + 0;

        return !((errno == ERANGE
                  && (target == HUGE_VAL || target == -HUGE_VAL))
                 || (errno != 0) || (target == Vertica::vfloat_null)
                 || (end != str+len));
    }

    /**
     * Parse a string to a binary field
     */
    bool parseBinary(char *str, size_t len, size_t colNum, Vertica::VString &target, const Vertica::VerticaType &type) {
        // Don't attempt to parse binary.
        // Just copy it.
        return parseVarchar(str, len, colNum, target, type);
    }

    /**
     * Parse a string to a varbinary field
     */
    bool parseVarbinary(char *str, size_t len, size_t colNum, Vertica::VString &target, const Vertica::VerticaType &type) {
        // Don't attempt to parse binary.
        // Just copy it.
        return parseVarchar(str, len, colNum, target, type);
    }

    /**
     * Parse a string to a CHAR field
     */
    bool parseChar(char *str, size_t len, size_t colNum, Vertica::VString &target, const Vertica::VerticaType &type) {
        // Some SQL implementations give CHAR and VARCHAR different
        // rules regarding parsing of leading whitespace.
        // We don't bother here / for now.
        return parseVarchar(str, len, colNum, target, type);
    }

    /**
     * Parse a string to a VARCHAR field
     */
    bool parseVarchar(char *str, size_t len, size_t colNum, Vertica::VString &target, const Vertica::VerticaType &type) {
        if (isNull(str, len)) {
            target.setNull();
            return true;
        }

        // It's a string!
        if ((size_t)type.getStringLength() < len) {
            return false;
        }

        target.copy(str, len);
        return true;
    }

    /**
     * Parse a string to an arbitrary-precision Numeric
     */
    bool parseNumeric(char *str, size_t len, size_t colNum, Vertica::VNumeric &target, const Vertica::VerticaType &type) {
        if (isNull(str, len)) {
            target.setNull();
            return true;
        }

        return Vertica::VNumeric::charToNumeric(str, type, target);
    }

    /**
     * Parse a string to a Vertica Date, according to the specified format.
     * `format` is a format-string as passed to the default Vertica
     * string->date parsing function.
     */
    bool parseDate(char *str, size_t len, size_t colNum, Vertica::DateADT &target, const Vertica::VerticaType &type) {
        if (isNull(str, len)) {
            target = Vertica::vint_null;
            return true;
        }

        try {
            target = Vertica::dateIn(str, false);
            if (target == vint_null) {
                return false;
            }
            return true;
        } catch (...) {
            return false;
        }
    }

    /**
     * Parse a string to a Vertica Time, according to the specified format.
     * `format` is a format-string as passed to the default Vertica
     * string->date parsing function.
     */
    bool parseTime(char *str, size_t len, size_t colNum, Vertica::TimeADT &target, const Vertica::VerticaType &type) {
        if (isNull(str, len)) {
            target = Vertica::vint_null;
            return true;
        }

        try {
            target = Vertica::timeIn(str, type.getTypeMod(), false);
            if (target == vint_null) {
                return false;
            }
            return true;
        } catch (...) {
            return false;
        }
    }

    /**
     * Parse a string to a Vertica Timestamp, according to the specified format.
     * `format` is a format-string as passed to the default Vertica
     * string->date parsing function.
     */
    bool parseTimestamp(char *str, size_t len, size_t colNum, Vertica::Timestamp &target, const Vertica::VerticaType &type) {
        if (isNull(str, len)) {
            target = Vertica::vint_null;
            return true;
        }

        try {
            target = Vertica::timestampIn(str, type.getTypeMod(), false);
            if (target == vint_null) {
                return false;
            }
            return true;
        } catch (...) {
            return false;
        }
    }

    /**
     * Parse a string to a Vertica TimeTz, according to the specified format.
     * `format` is a format-string as passed to the default Vertica
     * string->date parsing function.
     */
    bool parseTimeTz(char *str, size_t len, size_t colNum, Vertica::TimeTzADT &target, const Vertica::VerticaType &type) {
        if (isNull(str, len)) {
            target = Vertica::vint_null;
            return true;
        }

        try {
            target = Vertica::timetzIn(str, type.getTypeMod(), false);
            if (target == vint_null) {
                return false;
            }
            return true;
        } catch (...) {
            return false;
        }
    }

    /**
     * Parse a string to a Vertica TimestampTz, according to the specified format.
     * `format` is a format-string as passed to the default Vertica
     * string->date parsing function.
     */
    bool parseTimestampTz(char *str, size_t len, size_t colNum, Vertica::TimestampTz &target, const Vertica::VerticaType &type) {
        if (isNull(str, len)) {
            target = Vertica::vint_null;
            return true;
        }

        try {
            target = Vertica::timestamptzIn(str, type.getTypeMod(), false);
            if (target == vint_null) {
                return false;
            }
            return true;
        } catch (...) {
            return false;
        }
    }

    /**
     * Parse an interval expression to a Vertica Interval
     */
    bool parseInterval(char *str, size_t len, size_t colNum, Vertica::Interval &target, const Vertica::VerticaType &type) {
        try {
            target = Vertica::intervalIn(str, type.getTypeMod(), false);
            if (target == vint_null) {
                return false;
            }
            return true;
        } catch (...) {
            return false;
        }
    }

    /**
     * Parse an interval expression to a Vertica IntervalYM
     */
    bool parseIntervalYM(char *str, size_t len, size_t colNum, Vertica::IntervalYM &target, const Vertica::VerticaType &type) {
        return false;  // not supported here
    }

protected:
    /**
     * Helper function to determine whether the specified string-value
     * represents the null value.
     * Assumes that the null string is independent of data type.
     * Alternative implementations may of course not make this assumption.
     */
    bool isNull(char *str, size_t len) {
        // The empty string is NULL, 'cause we said so
        return (len == 0);
    }
};

/**
 * Implementation of StringParsers that allows the use of format-strings to
 * specify the interpretation of various date/time types.
 * Requires that the relevant format-strings be set at construction time.
 */
class VFormattedStringParsers : public StringParsers {
public:
    // Ctor.
    VFormattedStringParsers()
    {}
    // Ctor.
    VFormattedStringParsers(const std::vector<std::string> &formats) :
        formats(formats)
    {}

    /**
     * Specify the string formatters used by this implementation
     */
    void setFormats(const std::vector<std::string> &formats) { this->formats = formats; }

    /**
     * Parse a string to a Vertica Date, according to the specified format.
     * `format` is a format-string as passed to the default Vertica
     * string->date parsing function.
     */
    bool parseDate(char *str, size_t len, size_t colNum, Vertica::DateADT &target, const Vertica::VerticaType &type) {
        if (isNull(str, len)) {
            target = Vertica::vint_null;
            return true;
        }

        try {
            target = Vertica::dateInFormatted(str, formats[colNum], true);
            return true;
        } catch (...) {
            return false;
        }
    }

    /**
     * Parse a string to a Vertica Time, according to the specified format.
     * `format` is a format-string as passed to the default Vertica
     * string->date parsing function.
     */
    bool parseTime(char *str, size_t len, size_t colNum, Vertica::TimeADT &target, const Vertica::VerticaType &type) {
        if (isNull(str, len)) {
            target = Vertica::vint_null;
            return true;
        }

        try {
            target = Vertica::timeInFormatted(str, type.getTypeMod(), formats[colNum], true);
            return true;
        } catch (...) {
            return false;
        }
    }

    /**
     * Parse a string to a Vertica Timestamp, according to the specified format.
     * `format` is a format-string as passed to the default Vertica
     * string->date parsing function.
     */
    bool parseTimestamp(char *str, size_t len, size_t colNum, Vertica::Timestamp &target, const Vertica::VerticaType &type) {
        if (isNull(str, len)) {
            target = Vertica::vint_null;
            return true;
        }

        try {
            target = Vertica::timestampInFormatted(str, type.getTypeMod(), formats[colNum], true);
            return true;
        } catch (...) {
            return false;
        }
    }

    /**
     * Parse a string to a Vertica TimeTz, according to the specified format.
     * `format` is a format-string as passed to the default Vertica
     * string->date parsing function.
     */
    bool parseTimeTz(char *str, size_t len, size_t colNum, Vertica::TimeTzADT &target, const Vertica::VerticaType &type) {
        if (isNull(str, len)) {
            target = Vertica::vint_null;
            return true;
        }

        try {
            target = Vertica::timetzInFormatted(str, type.getTypeMod(), formats[colNum], true);
            return true;
        } catch (...) {
            return false;
        }
    }

    /**
     * Parse a string to a Vertica TimestampTz, according to the specified format.
     * `format` is a format-string as passed to the default Vertica
     * string->date parsing function.
     */
    bool parseTimestampTz(char *str, size_t len, size_t colNum, Vertica::TimestampTz &target, const Vertica::VerticaType &type) {
        if (isNull(str, len)) {
            target = Vertica::vint_null;
            return true;
        }

        // handle _internal type
        if (formats[colNum] == "_internal") {
            char* endval = str;
            errno = 0;
            target = strtoll(str, &endval, 10);

            // Check all the various error conditions of strtoll
            return errno == 0 && target != Vertica::vint_null
                && str + len == endval;
        }

        try {
            target = Vertica::timestamptzInFormatted(str, type.getTypeMod(), formats[colNum], true);
            return true;
        } catch (...) {
            return false;
        }
    }

private:
    std::vector<std::string> formats;
};


/**
 * Implementation of StringParsers that allows the use of format-strings to
 * specify the interpretation of various date/time types.
 * Requires that the relevant format-strings be set at construction time.
 */
class FormattedStringParsers : public StringParsers {
public:
    // Ctor.
    FormattedStringParsers()
    {}
    // Ctor.
    FormattedStringParsers(const std::vector<std::string> &formats) :
        formats(formats)
    {}

    /**
     * Specify the string formatters used by this implementation
     */
    void setFormats(const std::vector<std::string> &formats) { this->formats = formats; }

    /**
     * Parse a string to a Vertica Date, according to the specified format.
     * `format` is a format-string as passed to the libc "strptime" function.
     *
     * Note that all example date/time functions here use GNU-specific libc
     * extensions to handle timezones.
     * Libraries using this code must therefore be linked against a
     * GNU-compatible libc.
     * The default libc on all Vertica-supported platforms is GNU libc.
     */
    bool parseDate(char *str, size_t len, size_t colNum, Vertica::DateADT &target, const Vertica::VerticaType &type) {
        if (isNull(str, len)) {
            target = Vertica::vint_null;
            return true;
        }

        const std::string &format = formats.at(colNum);

        struct tm tm;
        memset(&tm, 0, sizeof(struct tm));
        if (strptime(str, format.empty() ? "%Y-%m-%d" : format.c_str(), &tm) == NULL) {
            if (!format.empty() || strptime(str, "%Y/%m/%d", &tm) == NULL)
                return false;
        }
        time_t time = timegm(&tm); // Assumes time is in GMT; for local time use mktime()
        target = Vertica::getDateFromUnixTime(time);
        return true;
    }

    /**
     * Parse a string to a Vertica Time, according to the specified format.
     * `format` is a format-string as passed to the libc "strptime" function.
     *
     * Note that all example date/time functions here use GNU-specific libc
     * extensions to handle timezones.
     * Libraries using this code must therefore be linked against a
     * GNU-compatible libc.
     * The default libc on all Vertica-supported platforms is GNU libc.
     */
    bool parseTime(char *str, size_t len, size_t colNum, Vertica::TimeADT &target, const Vertica::VerticaType &type) {
        if (isNull(str, len)) {
            target = Vertica::vint_null;
            return true;
        }

        const std::string &format = formats.at(colNum);

        struct tm tm;
        memset(&tm, 0, sizeof(struct tm));
        if (strptime(str, format.empty() ? "%H:%M:%S" : format.c_str(), &tm) == NULL)
            return false;
        time_t time = tm.tm_sec + tm.tm_min*60 + tm.tm_hour*3600;
        target = Vertica::getTimeFromUnixTime(time);
        return true;
    }

    /**
     * Parse a string to a Vertica Timestamp, according to the specified format.
     * `format` is a format-string as passed to the libc "strptime" function.
     *
     * Note that all example date/time functions here use GNU-specific libc
     * extensions to handle timezones.
     * Libraries using this code must therefore be linked against a
     * GNU-compatible libc.
     * The default libc on all Vertica-supported platforms is GNU libc.
     */
    bool parseTimestamp(char *str, size_t len, size_t colNum, Vertica::Timestamp &target, const Vertica::VerticaType &type) {
        if (isNull(str, len)) {
            target = Vertica::vint_null;
            return true;
        }

        const std::string &format = formats.at(colNum);

        struct tm tm;
        memset(&tm, 0, sizeof(struct tm));
        if (strptime(str, format.empty() ? "%Y-%m-%d %H:%M:%S" : format.c_str(), &tm) == NULL) {
            if (!format.empty() || strptime(str, "%Y/%m/%d %H:%M:%S", &tm) == NULL)
                return false;
        }
        time_t time = timegm(&tm); // Assumes time is in GMT; for local time use mktime()
        target = Vertica::getTimestampFromUnixTime(time);
        return true;
    }

    /**
     * Parse a string to a Vertica TimeTz, according to the specified format.
     * `format` is a format-string as passed to the libc "strptime" function.
     *
     * Note that all example date/time functions here use GNU-specific libc
     * extensions to handle timezones.
     * Libraries using this code must therefore be linked against a
     * GNU-compatible libc.
     * The default libc on all Vertica-supported platforms is GNU libc.
     */
    bool parseTimeTz(char *str, size_t len, size_t colNum, Vertica::TimeTzADT &target, const Vertica::VerticaType &type) {
        if (isNull(str, len)) {
            target = Vertica::vint_null;
            return true;
        }

        const std::string &format = formats.at(colNum);

        struct tm tm;
        memset(&tm, 0, sizeof(struct tm));
        if (strptime(str, format.empty() ? "%H:%M:%S %Z" : format.c_str(), &tm) == NULL)
            return false;
        time_t time = tm.tm_sec + tm.tm_min*60 + tm.tm_hour*3600;
        target = Vertica::setTimeTz(Vertica::getTimeFromUnixTime(time), tm.tm_gmtoff);
        return true;
    }

    /**
     * Parse a string to a Vertica TimestampTz, according to the specified format.
     * `format` is a format-string as passed to the libc "strptime" function.
     *
     * Note that all example date/time functions here use GNU-specific libc
     * extensions to handle timezones.
     * Libraries using this code must therefore be linked against a
     * GNU-compatible libc.
     * The default libc on all Vertica-supported platforms is GNU libc.
     */
    bool parseTimestampTz(char *str, size_t len, size_t colNum, Vertica::TimestampTz &target, const Vertica::VerticaType &type) {
        if (isNull(str, len)) {
            target = Vertica::vint_null;
            return true;
        }

        const std::string &format = formats.at(colNum);

        struct tm tm;
        memset(&tm, 0, sizeof(struct tm));
        if (strptime(str, format.empty() ? "%Y-%m-%d %H:%M:%S%Z" : format.c_str(), &tm) == NULL) {
            if (!format.empty() || strptime(str, "%Y/%m/%d %H:%M:%S%Z", &tm) == NULL)
                return false;
        }
        time_t time = timegm(&tm); // Assumes time is in GMT; for local time use mktime()
        target = Vertica::getTimestampTzFromUnixTime(time);
        return true;
    }

    /**
     * Parse an interval expression to a Vertica Interval
     */
    bool parseInterval(char *str, size_t len, size_t colNum, Vertica::Interval &target, const Vertica::VerticaType &type) {
        return parseIntervalHelper(str, len, target, false);
    }

    /**
     * Parse an interval expression to a Vertica IntervalYM
     */
    bool parseIntervalYM(char *str, size_t len, size_t colNum, Vertica::IntervalYM &target, const Vertica::VerticaType &type) {
        return parseIntervalHelper(str, len, (Vertica::Interval&) target, true);
    }

private:

    /**
     * Helper that points at the next token in a string
     */
    void token_next(char*& str) {
        // Walk over the token
        // Special case; kinda ugly...
        if (*str == ':' && (str[1] >= '0' && str[1] <= '9')) {
            ++str;
            return;
        }
        if (*str != ' ' && *str != '\0')
            ++str;
        while (true) {
            if ((*str >= '0' && *str <= '9') || (*str >= 'A' && *str <= 'Z')
                    || (*str >= 'a' && *str <= 'z'))
                ++str;
            else
                break;
        }
        // Walk over the separator
        while (true) {
            if (*str == ' ')
                ++str;
            else
                break;
        }
    }

    Vertica::vint identifier_convert(Vertica::vint val, const char *id, Vertica::vint scale_factor = 1) {
        // The valid identifiers are
        // SECOND, MINUTE, HOUR, DAY, WEEK, MONTH, YEAR, DECADE, CENTURY, MILLENIUM
        // and their valid pluralizations, lowercase variants, etc
        // with "number" corresponding to how many of the given unit
        // we should have in this interval.

#define _C_STR(str) sizeof(str),str
        const static IntervalIdentifier IDENTIFIERS[] = {
            { 1000000LL, _C_STR("SECOND")},
            {   1000000LL, _C_STR("SECONDS")},
            {   1000000LL * 60LL, _C_STR("MINUTE")},
            {   1000000LL * 60LL, _C_STR("MINUTES")},
            {   1000000LL * 60LL * 60LL, _C_STR("HOUR")},
            {   1000000LL * 60LL * 60LL, _C_STR("HOURS")},
            {   1000000LL * 60LL * 60LL * 24LL, _C_STR("DAY")},
            {   1000000LL * 60LL * 60LL * 24LL, _C_STR("DAYS")},
            {   1000000LL * 60LL * 60LL * 24LL * 7LL, _C_STR("WEEK")},
            {   1000000LL * 60LL * 60LL * 24LL * 7LL, _C_STR("WEEKS")},
            {   1000000LL * 60LL * 60LL * 24LL * 30LL, _C_STR("MONTH")},
            {   1000000LL * 60LL * 60LL * 24LL * 30LL, _C_STR("MONTHS")},
            {   1000000LL * 60LL * 60LL * 24LL * 365LL, _C_STR("YEAR")},
            {   1000000LL * 60LL * 60LL * 24LL * 365LL, _C_STR("YEARS")},
            {   1000000LL * 60LL * 60LL * 24LL * 365LL * 10LL, _C_STR("DECADE")},
            {   1000000LL * 60LL * 60LL * 24LL * 365LL * 10LL, _C_STR("DECADES")},
            {   1000000LL * 60LL * 60LL * 24LL * 365LL * 100LL, _C_STR("CENTURY")},
            {   1000000LL * 60LL * 60LL * 24LL * 365LL * 100LL, _C_STR("CENTURIES")},
            {   1000000LL * 60LL * 60LL * 24LL * 365LL * 1000LL, _C_STR("MILLENIUM")},
            {   1000000LL * 60LL * 60LL * 24LL * 365LL * 1000LL, _C_STR("MILLENIA")},
        {   0LL, 0, NULL}
        };
#undef _C_STR

        for (int i = 0; IDENTIFIERS[i].label != NULL; i++) {
            if (strncasecmp(id, IDENTIFIERS[i].label, IDENTIFIERS[i].label_length-1) == 0) {
                if (IDENTIFIERS[i].count_per % scale_factor != 0) {
                    return -1;  // Not a valid type at this scale
                }
                return val * (IDENTIFIERS[i].count_per / scale_factor);
            }
        }

        return -1;
    }

    Vertica::vint identifier_convert_YM(Vertica::vint val, const char *id, Vertica::vint scale_factor = 1) {
        // The valid identifiers are
        // SECOND, MINUTE, HOUR, DAY, WEEK, MONTH, YEAR, DECADE, CENTURY, MILLENIUM
        // and their valid pluralizations, lowercase variants, etc
        // with "number" corresponding to how many of the given unit
        // we should have in this interval.

#define _C_STR(str) sizeof(str),str
        const static IntervalIdentifier IDENTIFIERS[] = {
            {   1LL, _C_STR("MONTH")},
            {   1LL, _C_STR("MONTHS")},
            {   12LL, _C_STR("YEAR")},
            {   12LL, _C_STR("YEARS")},
            {   12LL * 10LL, _C_STR("DECADE")},
            {   12LL * 10LL, _C_STR("DECADES")},
            {   12LL * 100LL, _C_STR("CENTURY")},
            {   12LL * 100LL, _C_STR("CENTURIES")},
            {   12LL * 1000LL, _C_STR("MILLENIUM")},
            {   12LL * 1000LL, _C_STR("MILLENIA")},
            {   0LL, 0, NULL}
        };
#undef _C_STR

        for (int i = 0; IDENTIFIERS[i].label != NULL; i++) {
            if (strncasecmp(id, IDENTIFIERS[i].label, IDENTIFIERS[i].label_length-1) == 0) {
                if (IDENTIFIERS[i].count_per % scale_factor != 0) {
                    return -1;  // Not a valid type at this scale
                }
                return val * (IDENTIFIERS[i].count_per / scale_factor);
            }
        }

        return -1;
    }

    bool parseIntervalHelper(char *str, size_t len, Vertica::Interval &target, bool YearMonth = false) {
        // The interval format looks something like
        // [number identifier]* [days] [hh:mm[:ss]]

        if (isNull(str, len)) {
            target = Vertica::vint_null;
            return true;
        }

        if (len == 0 || *str == '\0') {
            return false; // Must actually have data
        }

        target = 0;

        // First, do the [number identifier]* part
        {

            char *ptr = str;
            char *number;
            char *identifier;
            char *comma;
            char *endptr;
            Vertica::vint val;
            while (true) {
                number = ptr;
                token_next(ptr);
                identifier = ptr;
                token_next(ptr);
                comma = ptr;
                token_next(ptr);

                if (*identifier == ':' || *comma == ':') {
                    // Oops, we just gobbled up a [day] [hh:mm[:ss]].
                    // Don't do that.
                    str = number;
                    break;
                }

                if (*number == '\0') {
                    return true; // Reached end-of-string
                }
                if (*identifier == '\0') {
                    return false; // Invalid format:  Number but no identifier
                }

                endptr = number;
                val = strtoll(number, &endptr, 10);
                if (*endptr != ' ') {
                    return false; // Must parse through to a space
                }
                val = YearMonth ? identifier_convert_YM(val, identifier) : identifier_convert(val, identifier);
                if (val == -1) { return false; } // Conversion error of some sort
                target += val;

                switch (*comma) {
                    case ',': continue; // Should have another token coming; keep looping
                    case '\0': return true; // All done!
                    case '0': case '1': case '2': case '3': case '4':
                    case '5': case '6': case '7': case '8': case '9':
                    // Whoops, we gobbled up a [day].  Back that out, then parse it.
                    str = comma;
                    break;
                    default: return false; // Invalid format
                }
                break;  // We only get here by explicitly 'break'ing in the above 'switch' statement
            }
        }

        // Ok, now we've found "[number identifier]*".
        // Time to find "[day]", then "[hh:mm[:ss]]"
        {
            int hhmmss = 0; // When iterating through the components of "HH:MM:SS", count how far in we are
            const static char* units[] = {"DAY","HOUR","MINUTE","SECOND"};

            char *ptr = str;
            char *number;
            char *next;
            char *endptr;
            Vertica::vint val;

            while (*ptr != '\0') {
                number = ptr;
                token_next(ptr);
                next = ptr;
                token_next(ptr);

                switch (*next) {
                    case '0': case '1': case '2': case '3': case '4':
                    case '5': case '6': case '7': case '8': case '9':
                        // Deal with having spaces
                        ptr = next;
                        // Deliberately fall through
                    case ':': case '\0':
                        if (hhmmss > 3) { return false;} // aa:bb:cc: -- invalid format
                        endptr = number;
                        val = strtoll(number, &endptr, 10);
                        if (*endptr != ' ' && *endptr != ':' && *endptr != '\0') { return false; } // Must parse through to a space
                        val = YearMonth ? identifier_convert_YM(val, units[hhmmss]) : identifier_convert(val, units[hhmmss]);
                        if (val == -1) { return false; } // Conversion error of some sort
                        target += val;
                        hhmmss++;
                        continue;
                    default:
                        // Some data that we weren't expecting
                        // Presumably invalid
                        return false;
                }
            }
        }

        return true;
    }

    std::vector<std::string> formats;
};
#endif // STRINGPARSERS_H_
