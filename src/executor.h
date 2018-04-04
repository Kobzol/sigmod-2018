#pragma once

#include <string>
#include <memory>
#include <unordered_map>

#include "database.h"
#include "query.h"
#include "relation/wrapper-iterator.h"

class Executor
{
public:
    explicit Executor(Query& query): query(query)
    {

    }

    void buildPlan(Database& database, int split);
    void finalizeQuery();

    std::vector<MultiWrapperIterator*> roots;

private:
    /**
     * Prepares iterators for all bindings of a query.
     * After this method every binding has a corresponding iterator in the views hashmap.
     */
    void createViews(Database& database, const Query& query, std::unordered_map<uint32_t, Iterator*>& views,
                     std::vector<std::unique_ptr<Iterator>>& container, bool& aggregable);

    /**
     * Creates AggregatedIterators for all bindings.
     */
    void createAggregatedViews(const Query& query, std::unordered_map<uint32_t, Iterator*>& views,
                               std::vector<std::unique_ptr<Iterator>>& container);

    /**
     * Combines all joins in a query to create a single root iterator.
     */
    Iterator* createRootView(Database& database, Query& query,
                             std::unordered_map<uint32_t, Iterator*>& views,
                             std::vector<std::unique_ptr<Iterator>>& container,
                             bool aggregable);

    void prepareRoots(Database& database, Query& query, Iterator* root, bool aggregable, int split);

    Query& query;
    std::unordered_map<uint32_t, Iterator*> views;
    std::vector<std::unique_ptr<Iterator>> container;

    std::unordered_map<SelectionId, Selection> selectionMap;
    std::vector<uint32_t> columnIds;
    std::vector<Selection> selections;
    std::unordered_map<uint32_t, uint32_t> backMap;
};
