#pragma once

#define DISABLE_COPY(Class) \
    Class(const Class&) = delete; \
    Class& operator=(const Class&) = delete; \
    Class(const Class&&) = delete
