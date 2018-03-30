#pragma once

#include "primary-index.h"

#include <cmath>
#include <cstdint>
#include <cstring>
#include <numeric>

struct IndexRecord
{
    uint64_t value;
    uint64_t row;
};

template <int N>
inline uint64_t* countingSort(uint64_t* mem, uint64_t* init, int rows, uint32_t column,
                              uint64_t minValue, uint64_t maxValue)
{
    std::vector<std::vector<uint32_t>> buckets(maxValue - minValue + 1);
    auto* ptr = reinterpret_cast<Row<N>*>(mem);
    auto* src = reinterpret_cast<Row<N>*>(init);

    for (int i = 0; i < rows; i++)
    {
        buckets[ptr[i].row[column] - minValue].push_back(i);
    }

    uint32_t target = 0;
    for (auto& bucket: buckets)
    {
        for (auto row: bucket)
        {
            ptr[target] = src[row];
            target++;
        }
    }

    return mem;
}

template <int N>
inline void copyResult(Row<N>* __restrict__ dest, const Row<N>* __restrict__ src,
                       const IndexRecord* __restrict__ offsets,
                       int rows)
{
    for (int i = 0; i < rows; i++)
    {
        dest[i] = src[offsets[i].row];
    }
}

inline bool canSkip(const int* counts, int rows)
{
    for (int i = 0; i < 256; i++)
    {
        if (counts[i] == rows) return true;
    }
    return false;
}

template <int N, int BITS=8>
inline void lsdRadixSort(Row<N>* __restrict__ dest,
                         Row<N>* __restrict__ src,
                         const uint64_t* __restrict__ columnData,
                         int rows,
                         int start,
                         uint64_t minValue,
                         uint64_t maxValue)
{
    std::vector<IndexRecord> indices[2];
    for (auto& index: indices)
    {
        index.resize(static_cast<size_t>(rows));
    }

    int activeIndex = 0;

    auto bits = static_cast<int>(std::ceil(log2(maxValue - minValue + 1)));
    auto passes = static_cast<uint32_t>(std::ceil(bits / (double) BITS));

    auto buckets = static_cast<int>(1U << BITS);

    std::vector<int> counts(passes * buckets);
    int offsets[buckets];

    for (int i = 0; i < rows; i++)
    {
        auto value = columnData[start + i] - minValue;
        indices[0][i].value = value;
        indices[0][i].row = static_cast<uint64_t>(start + i);
        for (uint32_t pass = 0; pass < passes; pass++)
        {
            unsigned char radix = (value >> (pass * BITS)) & 0xFF;
            counts[pass * buckets + radix]++;
        }
    }

    for (uint32_t pass = 0; pass < passes; pass++)
    {
        auto* __restrict__ count = counts.data() + buckets * pass;
        if (!canSkip(count, rows))
        {
            auto* __restrict__ currentIndices = indices[activeIndex].data();
            auto* __restrict__ nextIndices = indices[1 - activeIndex].data();
            offsets[0] = 0;
            for (int i = 1; i < buckets; i++)
            {
                offsets[i] = offsets[i - 1] + count[i - 1];
            }

            for (int i = 0 ; i < rows; i++)
            {
                auto& index = currentIndices[i];
                auto value = index.value;
                unsigned char radix = (value >> (pass * BITS)) & 0xFF;
                auto& nextIndex = nextIndices[offsets[radix]++];
                nextIndex.value = index.value;
                nextIndex.row = index.row;
            }

            activeIndex = 1 - activeIndex;
        }
    }

    copyResult(dest, src, indices[activeIndex].data(), rows);
}
