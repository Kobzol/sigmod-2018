#include "maxdiff_histogram.h"

#include <map>
#include <algorithm>

void MaxdiffHistogram::loadRelation(ColumnRelation& relation)
{
	std::map<uint64_t, uint32_t> sorted_values;

	std::vector<uint32_t> diffs(static_cast<size_t>(relation.getRowCount()));
	std::vector<bool> isBorder(static_cast<size_t>(relation.getRowCount()));

    std::vector<uint64_t> buckets_diff;
	std::vector<uint64_t> buckets_order;

	for (int i = 0; i < static_cast<int32_t>(relation.getColumnCount()); i++)
	{
		fullhistograms.emplace_back();
		columnStats.emplace_back();
		columnStats[i].cardinality = 0;
		columnStats[i].min = -1;
		columnStats[i].max = 0;
	}

#ifdef TRANSPOSE_RELATIONS
	for (int i = 0; i < static_cast<int32_t>(relation.getColumnCount()); i++)
		for (int j = 0; j < static_cast<int32_t>(relation.getRowCount()); j++)
		{
#else
	for (int j = 0; j < static_cast<int32_t>(relation.getRowCount()); j++)
		for (int i = 0; i < static_cast<int32_t>(relation.getColumnCount()); i++)
		{
#endif
			auto value = relation.getValue(j, i);
			fullhistograms[i][value]++;
			columnStats[i].min = value < columnStats[i].min ? value : columnStats[i].min;
			columnStats[i].max = value > columnStats[i].max ? value : columnStats[i].max;
		}



	for (int i = 0; i < static_cast<int32_t>(relation.getColumnCount()); i++)
    {
		columnStats[i].cardinality = fullhistograms[i].size();

        sorted_values.clear();
        bool unique = true;

        for (auto& element : fullhistograms[i])
        {
            sorted_values[element.first] = element.second;
            unique &= element.second == 1;
        }

        isUnique.push_back(unique);
        buckets_diff.clear();
        buckets_order.clear();

        if (!unique)
        {
            // computing the diffs between neighbors and searching for the greatest BUCKET_N diffs
            int c = 0;
            auto last = sorted_values.begin()->second;
            for (auto& element : sorted_values)
            {
                int diff = (int) last - element.second;
                last = element.second;
				diffs[c] = diff;
                isBorder[c] = false;

                if (buckets_diff.size() == 0 || diff >= (int) buckets_diff.back())
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
                    hist[bucket_count].frequency = frequency;
                    hist[bucket_count].max_value = element.first;
                    hist[bucket_count].max_value_frequency = element.second;

                    // reset of counters
                    distinct_values = 0;
                    frequency = 0;
                    bucket_count++;
                } else
                {
                    distinct_values++;
                    frequency += element.second;
                }
                c++;
            }
        } else
        {
            // creating the histogram buckets for unique attribute
            auto hist_size = fullhistograms[i].size() < BUCKET_N ? fullhistograms[i].size() : BUCKET_N;
            histogramCount.push_back(hist_size);
            hist_bucket* hist = new hist_bucket[hist_size];
            histogram.push_back(hist);
            int step_count = fullhistograms[i].size() / BUCKET_N;
            int c = 0, bucket_c = 0;
            for (auto& element : sorted_values)
            {
                // TODO: rewrite with direct access to sorted_values
                if (c == step_count)
                {
                    hist[bucket_c].distinct_values = step_count - 1;
                    hist[bucket_c].frequency = step_count - 1;
                    hist[bucket_c].max_value = element.first;
                    hist[bucket_c].max_value_frequency = 1;
                    bucket_c++;
                    c = 0;
                }
                c++;
            }
        }
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


void MaxdiffHistogram::print()
{
	int i = 0;
	for (auto s : columnStats)
	{
		std::cout << i++ << ": " << s.cardinality << ", (" << s.min << ", " << s.max << ");  ";
	}
}