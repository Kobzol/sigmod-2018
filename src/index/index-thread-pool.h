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
        }
    }

    bool doWork()
    {
        for (int i = 0; i < static_cast<int32_t>(database.sortIndices.size()); i++)
        {
#ifdef USE_PRIMARY_INDEX
            auto& index = database.primaryIndices[i];
            if (index->take())
            {
                index->build();
                return true;
            }
#endif
#ifdef USE_SORT_INDEX
            auto& index = database.sortIndices[i];
            if (index->take())
            {
                index->build();
                return true;
            }
#endif
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
