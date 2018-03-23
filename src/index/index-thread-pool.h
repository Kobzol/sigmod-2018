#pragma once

#include <thread>

#include "../settings.h"
#include "../database.h"

class IndexThreadPool
{
public:
    void start()
    {
        for (int i = 0; i < INDEX_THREADS; i++)
        {
            this->workers[i] = std::thread(&IndexThreadPool::work, this);
            this->workers[i].detach();
        }
    }

    bool buildPrimaryIndices(int column)
    {
        for (int i = 0; i < static_cast<int32_t>(database.relations.size()); i++)
        {
            if (column < static_cast<int32_t>(database.relations[i].columnCount))
            {
                auto& primary = database.primaryIndices[database.getGlobalColumnId(static_cast<uint32_t>(i),
                                                                                   static_cast<uint32_t>(column))];
                if (primary->take())
                {
                    primary->build();
                    return true;
                }
            }
        }

        return false;
    }

    bool buildSecondaryIndices()
    {
        for (int r = 0; r < static_cast<int32_t>(database.relations.size()); r++)
        {
            for (int c = 1; c < static_cast<int32_t>(database.relations[r].columnCount); c++)
            {
#ifdef USE_SORT_INDEX
                auto& sort = database.sortIndices[database.getGlobalColumnId(static_cast<uint32_t>(r),
                                                                             static_cast<uint32_t>(c))];
                if (sort->take())
                {
                    sort->build();

#ifdef USE_AGGREGATE_INDEX
                    auto& aggregate = database.aggregateIndices[database.getGlobalColumnId(static_cast<uint32_t>(r),
                    static_cast<uint32_t>(c))];
                    if (aggregate->take())
                    {
                        aggregate->build();
                    }
#endif
                    return true;
                }
#endif
            }
        }

        return false;
    }

    void work()
    {
#ifdef USE_PRIMARY_INDEX
        while (!this->stopped && this->buildPrimaryIndices(0));
        while (!this->stopped && this->buildPrimaryIndices(1));
        while (!this->stopped && this->buildPrimaryIndices(2));
#endif

        while (!this->stopped && this->buildSecondaryIndices());
    }

    void stop()
    {
        this->stopped = true;
    }

    std::thread workers[INDEX_THREADS];
    std::atomic<bool> stopped{false};
};

extern IndexThreadPool threadIndexPool;
