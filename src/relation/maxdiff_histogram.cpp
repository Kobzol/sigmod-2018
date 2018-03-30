#include "maxdiff_histogram.h"
#include <cstring>

void MaxdiffHistogram::loadRelation(ColumnRelation& relation, std::vector<uint32_t*>& fullhistograms2, bool sampling, int sampleMax)
{
	std::vector<std::vector<uint64_t>> preSort; // sort of the input column
	//std::vector<uint32_t*> fullhistograms2;

	size_t fullHistSize = 1 << BUCKET_SHIFT;

	std::vector<std::pair<uint64_t, uint32_t>> sorted_values;
	std::vector<uint32_t> unordered_values;
	bool* isBorder = new bool[relation.getRowCount()];

	std::vector<uint64_t> buckets_diff;
	std::vector<uint64_t> buckets_order;

	tupleCount = relation.getRowCount();
	int skipSize = 0;
	double multiplyKoeficient = 1;
	if (sampling)
	{
		if (relation.getRowCount() < sampleMax)
		{
			sampling = false;
		} else
		{
			skipSize = (relation.getRowCount() - sampleMax) * CHUNK_SIZE / sampleMax;
			multiplyKoeficient = relation.getRowCount() / sampleMax;
		}
	}

	for (int i = 0; i < static_cast<int32_t>(relation.getColumnCount()); i++)
	{
		//fullhistograms.emplace_back();
		columnStats.emplace_back();
		columnStats[i].cardinality = 0;
		columnStats[i].min = -1;
		columnStats[i].max = 0;
	}

	for (auto h: fullhistograms2) preSort.emplace_back();

	int chunkCount = 0;
	for (int i = 0; i < static_cast<int32_t>(relation.getColumnCount()); i++)
	{
		for (int j = 0; j < static_cast<int32_t>(relation.getRowCount()); j++)
		{
			if (sampling && chunkCount++ == CHUNK_SIZE)
			{
				chunkCount = 0;
				j += skipSize;
				if (j > static_cast<int32_t>(relation.getRowCount())) break;
			}

			auto value = relation.getValue(j, i);
			int bucket = value >> BUCKET_SHIFT;
			while (fullhistograms2.size() <= bucket)
			{
				preSort.emplace_back();
				fullhistograms2.push_back(new uint32_t[fullHistSize]);
				std::memset(fullhistograms2[fullhistograms2.size() - 1], 0, fullHistSize * sizeof(uint32_t));
			}
			preSort[bucket].push_back(value);

			columnStats[i].min = value < columnStats[i].min ? value : columnStats[i].min;
			columnStats[i].max = value > columnStats[i].max ? value : columnStats[i].max;
		}

		int b = 0;
		uint64_t segment = 0;
		bool unique = true;
		sorted_values.clear();
		for (auto& sarray : preSort)
		{
			if (sarray.size() < 10000)
			{
				// for small sarray
				unordered_values.clear();
				for (auto value : sarray)
				{
					fullhistograms2[b][value & HIST_MASK]++;
					if (fullhistograms2[b][value & HIST_MASK] == 1) unordered_values.push_back(value & HIST_MASK);
				}
				if (sarray.size() > 0)
				{
					std::sort(unordered_values.begin(), unordered_values.end());
					for (auto k : unordered_values)
					{
						sorted_values.push_back(std::make_pair(segment + k, fullhistograms2[b][k]));
						unique &= (fullhistograms2[b][k] == 1);
						fullhistograms2[b][k] = 0;
					}
				}
			}
			else
			{
				// for large sarray
				for (auto value : sarray)
				{
					fullhistograms2[b][value & HIST_MASK]++;
				}
				if (sarray.size() > 0)
				{
					for (int k = 0; k < fullHistSize; k++)
					if (fullhistograms2[b][k] > 0) 
					{
						sorted_values.push_back(std::make_pair(segment + k, fullhistograms2[b][k]));
						unique &= (fullhistograms2[b][k] == 1);
						fullhistograms2[b][k] = 0;
					}
				}
			}
			sarray.clear();
			b++;
			segment += fullHistSize;
		}

		columnStats[i].cardinality = sorted_values.size();

		columnStats[i].isUnique = unique;
		buckets_diff.clear();
		buckets_order.clear();


		if (!unique)
		{
			// computing the diffs between neighbors and searching for the greatest BUCKET_N diffs
			int c = 0;
			auto last = sorted_values.begin()->second;
			for (auto& element : sorted_values)
			{
				int diff = (int)last - element.second;
				last = element.second;
				isBorder[c] = false;

				if (buckets_diff.size() == 0 || diff >= (int)buckets_diff.back())
				{
					auto it = std::lower_bound(buckets_diff.cbegin(), buckets_diff.cend(), diff,
						std::greater<uint32_t>());
					int pos = it - buckets_diff.cbegin();

					buckets_diff.insert(it, diff);
					buckets_order.insert(buckets_order.cbegin() + pos, c);
				}

				if (buckets_diff.size() > BUCKET_N)
				{
					buckets_diff.pop_back();
					buckets_order.pop_back();
				}


				c++;
			}

			// marking the border values
			for (auto& it : buckets_order)
			{
				isBorder[it] = true;
			}

			// creating the histogram buckets
			histogramCount.push_back(buckets_order.size());
			hist_bucket* hist = new hist_bucket[buckets_order.size()];
			histogram.push_back(hist);

			// computing the bucket values
			int bucket_count = 0;
			c = 0;
			int distinct_values = 0;
			uint64_t frequency = 0;
			for (auto& element : sorted_values)
			{
				if (isBorder[c])
				{
					hist[bucket_count].distinct_values = distinct_values;
					hist[bucket_count].frequency = frequency * multiplyKoeficient;
					hist[bucket_count].max_value = element.first;
					hist[bucket_count].max_value_frequency = element.second * multiplyKoeficient;

					// reset of counters
					distinct_values = 0;
					frequency = 0;
					bucket_count++;
				}
				else
				{
					distinct_values++;
					frequency += element.second;
				}
				c++;
			}

		}
		else
		{
			// creating the histogram buckets for unique attribute
			auto hist_size = sorted_values.size() < BUCKET_N ? sorted_values.size() : BUCKET_N;
			histogramCount.push_back(hist_size);
			hist_bucket* hist = new hist_bucket[hist_size];
			histogram.push_back(hist);
			int step_count = sorted_values.size() / BUCKET_N;
			int c = 0, bucket_c = 0;
			for (auto& element : sorted_values)
			{
				// TODO: rewrite with direct access to sorted_values
				if (c == step_count)
				{
					hist[bucket_c].distinct_values = step_count - 1;
					hist[bucket_c].frequency = (step_count - 1) * multiplyKoeficient;
					hist[bucket_c].max_value = element.first;
					hist[bucket_c].max_value_frequency = 1;
					hist[bucket_c].other_columns_max = new uint64_t[relation.getColumnCount()];
					hist[bucket_c].other_columns_min = new uint64_t[relation.getColumnCount()];
					bucket_c++;
					c = 0;
				}
				if (bucket_c == step_count) break;
				c++;
			}
		}
	}

	//std::unordered_map<uint64_t, uint16_t>* bucketmap = new std::unordered_map<uint64_t, uint16_t>[relation.getColumnCount()];
	//for (int i = 0; i < relation.getColumnCount(); i++)
	//{
	//	for (int j = 0; j < histogramCount[i]; j++)
	//	{
	//		//bucketmap[i].insert(histogram[i][j], )
	//	}
	//}
	//for (int j = 0; j < static_cast<int32_t>(relation.getRowCount()); j++)
	//{
	//	for (int i = 0; i < static_cast<int32_t>(relation.getColumnCount()); i++)
	//	{
	//		auto value = relation.getValue(j, i);
	//		for (int i = 0; i < static_cast<int32_t>(relation.getColumnCount()); i++)
	//		{
	//			double pos = value / 53;
	//		}
	//	}
	//}
	
	delete[] isBorder;

}


void MaxdiffHistogram::findTresholds(int col_order, int count, uint64_t* tresholds)
{
	int c = 1;
	int countSum = 0;
	int tresholdCount = tupleCount / count;
	for (int i = 0; i < static_cast<int32_t>(histogramCount[col_order]); i++)
	{
		countSum += histogram[col_order][i].frequency + histogram[col_order][i].max_value_frequency;
		if (countSum > tresholdCount * c)
		{
			tresholds[c - 1] = histogram[col_order][i].max_value;
			c++;
		}
		if (count == c) break;
	}
}

uint32_t MaxdiffHistogram::estimateResult(const Filter& filter)
{
	auto colId = filter.selection.column;
	auto value = filter.value;


	if (filter.oper == '=')
	{
		auto last = histogram[colId][0].max_value;
		if (value < last)
		{
			return histogram[colId][0].frequency / histogram[colId][0].distinct_values;
		}
		if (value == last)
		{
			return histogram[colId][0].max_value_frequency;
		}
		for (int i = 1; i < static_cast<int32_t>(histogramCount[colId]); i++)
		{
			if (value < histogram[colId][i].max_value)
			{
				return histogram[colId][0].frequency / histogram[colId][0].distinct_values;
			}
			if (value == histogram[colId][i].max_value)
			{
				return histogram[colId][i].max_value_frequency;
			}
		}
		return 0;
	}
	if (filter.oper == '<')
	{
		auto last = histogram[colId][0].max_value;
		uint32_t sum = 0;
		if (value < last)
		{
			return histogram[colId][0].frequency; // TODO: compute fraction
		}
		if (value == last)
		{
			return histogram[colId][0].max_value_frequency + histogram[colId][0].frequency;
		}
		sum += histogram[colId][0].max_value_frequency + histogram[colId][0].frequency;
		for (int i = 1; i < static_cast<int32_t>(histogramCount[colId]); i++)
		{
			if (value < histogram[colId][i].max_value)
			{
				return sum +  (float)(value - last) / (histogram[colId][i].max_value - last) * histogram[colId][0].frequency;
			}
			if (value == histogram[colId][i].max_value)
			{
				return sum + histogram[colId][i].max_value_frequency;
			}
			sum += histogram[colId][i].max_value_frequency + histogram[colId][i].frequency;
			last = histogram[colId][i].max_value;
		}
		return sum;
	}
	if (filter.oper == '>')
	{
		int pos = histogramCount[colId] - 1;
		auto last = histogram[colId][pos].max_value;
		auto lastfrequency = histogram[colId][pos].frequency;
		uint32_t sum = 0;
		if (value >= last)
		{
			return  histogram[colId][pos].max_value_frequency;
		}
		sum += histogram[colId][pos].max_value_frequency + histogram[colId][pos].frequency;
		for (; pos > 0; pos--)
		{
			if (value > histogram[colId][pos].max_value)
			{
				return sum - (float)(value - histogram[colId][pos].max_value) / (last - histogram[colId][pos].max_value) * lastfrequency;
			}
			if (value == histogram[colId][pos].max_value)
			{
				return sum + histogram[colId][pos].max_value_frequency;
			}
			sum += histogram[colId][pos].max_value_frequency + histogram[colId][pos].frequency;
			last = histogram[colId][pos].max_value;
			lastfrequency = histogram[colId][pos].frequency;
		}
		return sum;
	}
	return 0;
}


void MaxdiffHistogram::printStatistics(int relation)
{
	int i = 0;
	for (auto s : columnStats)
	{
		std::cout << relation << "." << i << ": " << s.cardinality << ", (" << s.min << ", " << s.max << "), " << (s.isUnique ? "u " : " ") << (s.growByOne ? "u " : " ") << std::endl;
		i++;
	}
}

void MaxdiffHistogram::print(int colOrder)
{
	std::cout << "histogram count " << histogramCount[colOrder] << std::endl;
	for (int i = 0; i < static_cast<int32_t>(histogramCount[colOrder]); i++)
	{
		std::cout << i << ": " << histogram[colOrder][i].distinct_values <<
			", " << histogram[colOrder][i].frequency <<
			", " << histogram[colOrder][i].max_value <<
			", " << histogram[colOrder][i].max_value_frequency << std::endl;
	}
	std::cout << std::endl;
	for (int i = 0; i < 50; i++)
	{
		Filter f;
		f.oper = '<';
		f.value = i * 1000;
		f.selection.column = colOrder;
		std::cout << "estimate " << i * 1000 << ": " << estimateResult(f) << std::endl;
	}
}
