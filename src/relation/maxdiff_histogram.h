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
	uint64_t* other_columns_max;
	uint64_t* other_columns_min;
};

struct column_statistics
{
	uint32_t cardinality;
	uint32_t min;
	uint32_t max;
	bool isUnique;
	bool growByOne;
};

#define CHUNK_SIZE 5000
#define HIST_MASK 0x1ffff
#define BUCKET_SHIFT 17

/**
* Histogram for each column in the relation
* TODO: create histogram from smaller sample
*/
class MaxdiffHistogram
{

	std::vector<hist_bucket*> histogram;
	std::vector<uint32_t> histogramCount;
	uint64_t tupleCount;


public:	


	std::vector<column_statistics> columnStats;

	void loadRelation(ColumnRelation& relation, std::vector<uint32_t*>& fullhistograms2, bool sampling = true, int sampleMax = 100000);

	uint32_t estimateResult(const Filter& filter);
	void findTresholds(int col_order, int count, uint64_t* treshold);

	void printStatistics(int relation);
	void print(int colOrder);
};
