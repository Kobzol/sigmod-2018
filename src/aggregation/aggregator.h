#pragma once

#include <string>
#include <unordered_map>
#include <sstream>
#include <cassert>
#include <algorithm>
#include <xmmintrin.h>

#include "../query.h"
#include "../database.h"
#include "../settings.h"
#include "../relation/wrapper-iterator.h"

class Aggregator: public WrapperIterator
{
public:
    explicit Aggregator(Iterator* inner, const Query& query);

    void sumRows(std::vector<uint64_t>& results, const std::vector<uint32_t>& columnIds,
                 const std::vector<Selection>& selections, size_t& count) override;

    void split(std::vector<std::unique_ptr<Iterator>>& container, std::vector<Iterator*>& groups, size_t count) final;

    const Query& query;
};
