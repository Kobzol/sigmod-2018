#include "self-join.h"

SelfJoin::SelfJoin(Iterator* inner, std::vector<Selection> selections):
    WrapperIterator(inner), selections(std::move(selections))
{
    assert(this->selections.size() > 1);
    this->selectionSize = static_cast<int32_t>(this->selections.size() - 1);
}

bool SelfJoin::getNext()
{
    while (this->inner->getNext())
    {
        uint64_t value = this->inner->getValue(this->selections[0]);
        bool ok = true;
        for (int i = 1; i < this->selectionSize; i++)
        {
            if (this->inner->getValue(this->selections[i]) != value)
            {
                ok = false;
                break;
            }
        }
        if (ok) return true;
    }

    return false;
}

void SelfJoin::dumpPlan(std::ostream& ss)
{
    ss << "SJ(";
    this->inner->dumpPlan(ss);
    ss << ")";
}

std::unique_ptr<Iterator> SelfJoin::createIndexedIterator(std::vector<std::unique_ptr<Iterator>>& container,
                                                          const Selection& selection)
{
    container.push_back(this->inner->createIndexedIterator(container, selection));
    return std::make_unique<SelfJoin>(container.back().get(), this->selections);
}

void SelfJoin::splitToBounds(std::vector<std::unique_ptr<Iterator>>& container, std::vector<Iterator*>& groups,
                             std::vector<uint64_t>& bounds, size_t count)
{
    std::vector<Iterator*> subGroups;
    this->inner->splitToBounds(container, subGroups, bounds, count);

    for (auto& group: subGroups)
    {
        container.push_back(std::make_unique<SelfJoin>(group, this->selections));
        groups.push_back(container.back().get());
    }
}

void SelfJoin::splitUsingBounds(std::vector<std::unique_ptr<Iterator>>& container, std::vector<Iterator*>& groups,
                                const std::vector<uint64_t>& bounds)
{
    std::vector<Iterator*> subGroups;
    this->inner->splitUsingBounds(container, subGroups, bounds);

    for (auto& group: subGroups)
    {
        container.push_back(std::make_unique<SelfJoin>(group, this->selections));
        groups.push_back(container.back().get());
    }
}

void SelfJoin::split(std::vector<std::unique_ptr<Iterator>>& container, std::vector<Iterator*>& groups, size_t count)
{
    std::vector<Iterator*> subGroups;
    this->inner->split(container, subGroups, count);

    for (auto& group: subGroups)
    {
        container.push_back(std::make_unique<SelfJoin>(group, this->selections));
        groups.push_back(container.back().get());
    }
}
