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

//private:
    /**
     * Prepares iterators for all bindings of a query.
     * After this method every binding has a corresponding iterator in the views hashmap.
     */
    void createViews(Database& database,
                     const Query& query,
                     std::unordered_map<uint32_t, Iterator*>& views,
                     std::vector<std::unique_ptr<Iterator>>& container);

    /**
     * Combines all joins in a query to create a single root iterator.
     */
    Iterator* createRootView(Database& database, Query& query,
                             std::unordered_map<uint32_t, Iterator*>& views,
                             std::vector<std::unique_ptr<Iterator>>& container);

	// Sestaveni planu dotazu.
	Iterator* buildQueryPlan1(Database& database, Query& query,
		std::unordered_map<uint32_t, Iterator*>& views,
		std::vector<std::unique_ptr<Iterator>>& container);
};
