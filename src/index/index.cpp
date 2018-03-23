#include "index.h"
#include "../relation/column-relation.h"

Index::Index(ColumnRelation& relation, uint32_t column)
        : relation(relation), column(column), columns(relation.getColumnCount())
{

}
