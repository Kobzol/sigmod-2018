#pragma once

#include "../query.h"

class ForeignKeyChecker
{
public:
    bool isForeignKey(const Selection& primary, const Selection& foreign);
};
