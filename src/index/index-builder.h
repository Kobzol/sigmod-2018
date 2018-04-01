#pragma once

#include <thread>
#include "primary-index.h"
#include "../database.h"

class IndexBuilder
{
public:
    void buildIndices(const std::vector<uint32_t>& indices)
    {
        if (indices.empty()) return;

        auto count = static_cast<int>(indices.size());
        /*using Column = std::pair<uint32_t, uint32_t>;
        std::vector<Column> columns; // index, row count
        for (auto index: indices)
        {
            columns.emplace_back(index, database.primaryIndices[index]->relation.getRowCount());
        }

        uint32_t maxRows = 0;
        uint32_t minRows = std::numeric_limits<uint32_t>::max();

        for (auto& col: columns)
        {
            maxRows = std::max(maxRows, col.second);
            minRows = std::min(minRows, col.second);
        }

        std::sort(columns.begin(), columns.end(), [](const Column& lhs, const Column& rhs) {
            return lhs.second > rhs.second;
        });*/

        uint32_t threads = std::max(static_cast<uint32_t>(PRIMARY_THREADS_LAZY),
                               std::thread::hardware_concurrency() / count);

#pragma omp parallel for schedule(dynamic)
        for (int i = 0; i < count; i++)
        {
            if (database.primaryIndices[indices[i]]->take())
            {
                database.primaryIndices[indices[i]]->build(threads);
            }
        }
    }
};
