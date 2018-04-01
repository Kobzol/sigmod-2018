#include "filter-compiler.h"

#include <algorithm>
#include <cassert>
#include <cstddef>

#ifdef __linux__
#include <sys/mman.h>
#endif

static unsigned char getCmpOpcode(char oper)
{
    switch (oper)
    {
        case '=': return 0x75; // JNE
        case '<': return 0x73; // JAE
        case '>': return 0x76; // JBE
        default: assert(false);
    }

    return 0x00;
}

FilterFn FilterCompiler::compile(const Filter& filter)
{
    const size_t allocSize = 4096;
    const size_t predicateSize = 15; // MOVABS + CMP + Jcc

#ifdef __linux__
    void* mem = mmap(nullptr, allocSize, PROT_READ | PROT_WRITE | PROT_EXEC, MAP_ANONYMOUS | MAP_PRIVATE, 0, 0);
#else
	void* mem = nullptr;
#endif
    auto* eip = reinterpret_cast<unsigned char*>(mem);

    // MOV al, 0x1
    *eip++ = 0xB0; *eip++ = 0x01;

    std::vector<Filter> filters;
    if (filter.oper == 'r')
    {
        filters.emplace_back(filter.selection, filter.value, nullptr, '>');
        filters.emplace_back(filter.selection, filter.valueMax, nullptr, '<');
    }
    else filters.push_back(filter);

    size_t left = filters.size();
    for (auto& filter : filters)
    {
        left--;
        uint64_t value = filter.value;

        // MOVABS RCX, value
        *eip++ = 0x48; *eip++ = 0xB9;
        for (int b = 0; b < 8; b++)
        {
            *eip++ = static_cast<unsigned char>((value >> (b * 8)) & 0xFF);
        }

        // CMP RDI, RCX
        *eip++ = 0x48; *eip++ = 0x39; *eip++ = 0xCF;

        // Jcc
        assert(left * predicateSize + 1 < 256);
        *eip++ = getCmpOpcode(filter.oper); *eip++ = static_cast<unsigned char>(left * predicateSize + 1);
    }

    // RET
    *eip++ = 0xC3;

    // XOR EAX, EAX
    *eip++ = 0x31; *eip++ = 0xC0;

    // RET
    *eip++ = 0xC3;

    assert(eip - reinterpret_cast<unsigned char*>(mem) <= static_cast<ptrdiff_t>(allocSize));
    return reinterpret_cast<FilterFn>(mem);
}

// B0 01 - MOV al, 0x1
// 48 B9 val - MOVABS RCX, val
// 48 39 CF - CMP RDI, RCX

// 73 - JAE
// 74 - JE
// 75 - JNE
// 76 - JBE

// c3 - RET
// 31 C0 - XOR EAX, EAX
// c3 - RET

// 48 B8 val - MOV RAX, val
// 55 - push RBP
// 48 89 e5 - MOV RBP, RSP
// 5d pop RBP
