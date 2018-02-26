#pragma once

#include <string>
#include <memory>
#include <unordered_map>

#include "database.h"
#include "query.h"

class Executor
{
public:
    std::string executeQuery(Database& database, Query& query);

private:
    void createFilterViews(Database& database,
                           const Query& query,
                           std::unordered_map<uint32_t, View*>& views,
                           std::vector<std::unique_ptr<View>>& container);

    std::string aggregate(Database& database, const Query& query,
                          std::unordered_map<uint32_t, View*>& views);

    void join(Query& query, std::unordered_map<uint32_t, View*>& views,
              std::vector<std::unique_ptr<View>>& container);
};
