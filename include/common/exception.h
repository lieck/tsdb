#pragma once

#include <stdexcept>
#include <iostream>

namespace ljdb {

enum class ExceptionType {
    // Invalid exception type
    INVALID = 0,
    // IO exception type
    IO = 1,
};



class Exception : public std::runtime_error {
public:
    explicit Exception(const std::string &msg) : std::runtime_error(msg), type_(ExceptionType::INVALID) {
#ifndef NDEBUG
      std::cerr << "Message: " << msg << std::endl;
#endif
    }

    Exception(ExceptionType exception_type, const std::string &msg) : std::runtime_error(msg), type_(exception_type) {
#ifndef NDEBUG
        std::cerr << "\nException Type:" << ExceptionTypeToString(type_)  << "\nMessage: " << msg << std::endl;
#endif
    }


    static auto ExceptionTypeToString(ExceptionType type) -> std::string {
        switch (type) {
        case ExceptionType::INVALID:
            return "Invalid";
        case ExceptionType::IO:
            return "Io";
        default:
            return "Unknown";
        }
    }

    auto Type() -> ExceptionType {
        return type_;
    }

private:

    ExceptionType type_;
};

}  // namespace ljdb
