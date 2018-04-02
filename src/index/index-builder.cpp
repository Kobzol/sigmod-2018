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

#ifdef STATISTICS
    Timer indexGroupTimer;
#endif

#pragma omp parallel for schedule(dynamic)
    for (int i = 0; i < count; i++)
    {
        if (database.primaryIndices[indices[i]]->take())
        {
            database.primaryIndices[indices[i]]->prepare();
        }
    }

#ifdef STATISTICS
    indexGroupCountTime += indexGroupTimer.get();
#endif

    std::vector<std::function<void()>> bucketJobs;
    for (int i = 0; i < count; i++)
    {
        bucketJobs.insert(bucketJobs.end(),
                          database.primaryIndices[indices[i]]->bucketJobs.begin(),
                          database.primaryIndices[indices[i]]->bucketJobs.end()
        );
    }

#ifdef STATISTICS
    Timer bucketTimer;
#endif

#pragma omp parallel for schedule(dynamic)
    for (int i = 0; i < static_cast<int32_t>(bucketJobs.size()); i++)
    {
        bucketJobs[i]();
    }

#ifdef STATISTICS
    indexCopyToBucketsTime += bucketTimer.get();
    Timer sortTimer;
#endif

    std::vector<std::function<void()>> sortJobs;
    for (int i = 0; i < count; i++)
    {
        sortJobs.insert(sortJobs.end(),
                        database.primaryIndices[indices[i]]->sortJobs.begin(),
                        database.primaryIndices[indices[i]]->sortJobs.end()
        );
    }

#pragma omp parallel for schedule(dynamic)
    for (int i = 0; i < static_cast<int32_t>(sortJobs.size()); i++)
    {
        sortJobs[i]();
    }

#ifdef STATISTICS
    indexSortTime += sortTimer.get();
#endif

#pragma omp parallel for schedule(dynamic)
    for (int i = 0; i < count; i++)
    {
        database.primaryIndices[indices[i]]->finalize();
    }
}
