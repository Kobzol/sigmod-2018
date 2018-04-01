#pragma once

#include <string>


#include <sys/stat.h>
#include <iostream>
#include <fcntl.h>
#include <fstream>

#include <fcntl.h>
#ifdef __linux__
#include <sys/mman.h>
#include <unistd.h>
#else
#include <Windows.h>
// TODO
#define __builtin_expect(expr1, expr2) expr1
#endif

#include <cstring>
#include <unordered_map>
#include <unordered_set>
#include <algorithm>

#include "settings.h"
#include "stats.h"
#include "query.h"
#include "database.h"

void loadDatabase(Database& database);
uint64_t readInt(std::string& str, int& index);
void loadQuery(Query& query, std::string& line);
