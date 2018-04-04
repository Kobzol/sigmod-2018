#pragma once

#include <vector>
#include <cstdint>

class IndexBuilder
{
public:
    void buildIndices(const std::vector<uint32_t>& indices, int outerThreads, int innerThreads);
};
