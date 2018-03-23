#pragma once

#include <functional>   // std::greater
#include <algorithm>
#include <map>
#include <iostream>
#ifndef __linux__
#include <crtdbg.h>
#endif

#include "../settings.h"
#include "iterator.h"
#include "column-relation.h"

struct hist_bucket
{
	uint32_t frequency; // frequency of distinct values
	uint32_t distinct_values;
	uint64_t max_value;
	uint32_t max_value_frequency;
};

struct column_statistics
{
	uint32_t cardinality;
	uint32_t min;
	uint32_t max;
	bool isUnique;
	bool growByOne;
};

/**
* Histogram for each column in the relation
* TODO: create histogram from smaller sample
*/
class MaxdiffHistogram
{

	std::vector<hist_bucket*> histogram;
	std::vector<uint32_t> histogramCount;


public:
	
	std::vector<column_statistics> columnStats;

	void loadRelation(ColumnRelation& relation);

	uint32_t estimateResult(const Filter& filter);

	void print(int relation);
	
};
