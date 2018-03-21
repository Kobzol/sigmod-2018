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

    bool doWork()
    {
        for (int i = 0; i < static_cast<int32_t>(database.sortIndices.size()); i++)
        {
            bool found = false;
#ifdef USE_PRIMARY_INDEX
            auto& primary = database.primaryIndices[i];
            if (primary->take())
            {
                primary->build();
                found = true;
            }
#endif
#ifdef USE_SORT_INDEX
            auto& sort = database.sortIndices[i];
            if (sort->take())
            {
                sort->build();
                found = true;

#ifdef USE_AGGREGATE_INDEX
                auto& aggregate = database.aggregateIndices[i];
                if (aggregate->take())
                {
                    aggregate->build();
                }
#endif
            }
#endif
            if (found) return true;
        }

        return false;
    }

    void work()
    {
        while (!this->stopped && this->doWork());
    }

    void stop()
    {
        this->stopped = true;
    }

    std::thread workers[INDEX_THREADS];
    std::atomic<bool> stopped{false};
};

extern IndexThreadPool threadIndexPool;
