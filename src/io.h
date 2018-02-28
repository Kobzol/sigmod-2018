#pragma once

#include <string>

#include "query.h"
#include "database.h"

void loadDatabase(Database& database);
uint64_t readInt(std::string& str, int& index);
void loadQuery(Query& query, std::string& line);
