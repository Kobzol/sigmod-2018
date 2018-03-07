#pragma once

#include <string>
#include <memory>
#include <unordered_map>

#include "database.h"
#include "query.h"

class Executor
{
public:
    void executeQuery(Database& database, Query& query);

private:
    void createViews(Database& database,
                     const Query& query,
                     std::unordered_map<uint32_t, Iterator*>& views,
                     std::vector<std::unique_ptr<Iterator>>& container);

    Iterator* createRootView(Database& database, Query& query,
                             std::unordered_map<uint32_t, Iterator*>& views,
                             std::vector<std::unique_ptr<Iterator>>& container);
};
