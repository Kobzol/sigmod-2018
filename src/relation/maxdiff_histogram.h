#pragma once

#include <functional>   // std::greater
#include <algorithm>
#include <map>
#include <iostream>

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
};

/**
* Histogram for each column in the relation
* TODO: create histogram from smaller sample
*/
class MaxdiffHistogram
{
	std::vector<bool> isUnique;
	std::vector<hist_bucket*> histogram_old;
	std::vector<uint32_t> histogramCount_old;

	
	std::vector<hist_bucket*> histogram;
	std::vector<uint32_t> histogramCount;


public:
	
	std::vector<column_statistics> columnStats;
	std::vector<column_statistics> columnStats2;

	void loadRelation(ColumnRelation& relation);

	void loadRelation_old(ColumnRelation& relation);
	bool compareBuckets();

	uint32_t estimateResult(const Filter& filter);
	uint64_t maxValue(uint32_t colId) { return histogram_old[colId][histogramCount_old[colId] - 1].max_value; }

	//void print();
	
};
