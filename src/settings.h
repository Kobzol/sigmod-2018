#pragma once

#define STATISTICS
#define CHECK_ERRORS

#define EXPECT(expr) __builtin_expect(expr, 1)
#define NOEXPECT(expr) __builtin_expect(expr, 0)
