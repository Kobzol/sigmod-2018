#pragma once

#define DISABLE_COPY(Class) \
    Class(const Class&) = delete; \
    Class& operator=(const Class&) = delete

#define ENABLE_MOVE(Class) \
    Class(Class&&) = default

#define EXPECT(expr) __builtin_expect(expr, 1)
#define NOEXPECT(expr) __builtin_expect(expr, 0)
