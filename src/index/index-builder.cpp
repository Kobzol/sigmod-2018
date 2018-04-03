#include "index-builder.h"

#include <thread>
#include "primary-index.h"
#include "../database.h"
#include "../timer.h"
#include "../stats.h"

void IndexBuilder::buildIndices(const std::vector<uint32_t>& indices)
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

#pragma omp parallel for schedule(dynamic)
    for (int i = 0; i < count; i++)
    {
        if (database.primaryIndices[indices[i]]->take())
        {
            database.primaryIndices[indices[i]]->initMemory();
        }
    }

#pragma omp parallel for schedule(dynamic) num_threads(8)
    for (int i = 0; i < count; i++)
    {
        database.primaryIndices[indices[i]]->build(PRIMARY_INDEX_PREBUILD_THREADS);
    }
}
