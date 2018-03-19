#pragma once

#include <assert.h>
#include <array>
#include <cstdint>

#include "../relation/iterator.h"


class AggregateAbstract : public Iterator
{
public:

	virtual uint32_t getCountColumnIndex() = 0;

};