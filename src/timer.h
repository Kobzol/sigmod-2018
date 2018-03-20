#pragma once

#include <chrono>
#include <ctime>
#ifdef __linux__
#include <sys/time.h>
#endif

#include "settings.h"

double get_wall_time();

using TimerClock = std::chrono::high_resolution_clock;

class Timer
{
public:
    Timer()
    {
        this->start();
    }
    
    void start()
    {
#ifdef MEASURE_REAL_TIME
        this->point = get_wall_time();
#else
        this->point = TimerClock::now();
#endif
    }
    double get()
    {
#ifdef MEASURE_REAL_TIME
        return (get_wall_time() - this->point) * 1000.0;
#else
        auto elapsed = TimerClock::now() - this->point;
        return std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count();
#endif
    }
    double add()
    {
        this->total += this->get();
        return this->total;
    }
    void reset()
    {
        this->total = 0;
        this->start();
    }

    double total = 0.0;

#ifdef MEASURE_REAL_TIME
    double point = 0.0;
#else
    std::chrono::time_point<TimerClock> point;
#endif
};
