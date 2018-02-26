#include "hash-joiner.h"

#include <unordered_map>

std::unique_ptr<View> HashJoiner::join(View& r1, View& r2, const std::vector<Join>& joins)
{
    return std::unique_ptr<View>();
}
