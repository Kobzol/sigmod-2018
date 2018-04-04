#include "index-builder.h"

#include <thread>
#include "primary-index.h"
#include "../database.h"
#include "../timer.h"
#include "../stats.h"

void IndexBuilder::buildIndices(const std::vector<uint32_t>& indices, int outerThreads, int innerThreads)
{
    if (indices.empty()) return;

    std::vector<uint32_t> primaryIndices;
    std::vector<uint32_t> secondaryIndices;

    for (auto index: indices)
    {
        if (PrimaryIndex::canBuild(database.primaryIndices[index]->relation))
        {
            primaryIndices.push_back(index);
        }
        else secondaryIndices.push_back(index);
    }

    auto primaryCount = static_cast<int>(primaryIndices.size());
    auto secondaryCount = static_cast<int>(secondaryIndices.size());

#pragma omp parallel for schedule(dynamic)
    for (int i = 0; i < primaryCount; i++)
    {
        if (database.primaryIndices[primaryIndices[i]]->take())
        {
            database.primaryIndices[primaryIndices[i]]->initMemory();
        }
    }

#pragma omp parallel for schedule(dynamic) num_threads(outerThreads)
    for (int i = 0; i < primaryCount; i++)
    {
        database.primaryIndices[primaryIndices[i]]->build(static_cast<uint32_t>(innerThreads));
    }

#pragma omp parallel for schedule(dynamic) num_threads(outerThreads)
    for (int i = 0; i < secondaryCount; i++)
    {
        if (database.sortIndices[secondaryIndices[i]]->take())
        {
            database.sortIndices[secondaryIndices[i]]->build(static_cast<uint32_t>(innerThreads));
        }
    }
}
