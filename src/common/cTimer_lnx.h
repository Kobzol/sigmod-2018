/**************************************************************************}
{                                                                          }
{    cTimer.h                                                              }
{      Warning - Win32 only!                                               }
{                                                                          }
{                                                                          }
{    Copyright (c) 2001                      Michal Kratky                 }
{                                                                          }
{    VERSION: 0.2                            DATE 8/2/2002                 }
{                                                                          }
{    following functionality:                                              }
{       timer                                                              }
{                                                                          }
{                                                                          }
{    UPDATE HISTORY:                                                       }
{      8/2/2002                                                            }
{                                                                          }
{**************************************************************************/

#include <time.h>
#include "cTimer.h"

/**
*	Time measurement in Linux.
*
*	\author Michal Kratky
*	\version 0.1
*	\date 2014
**/
namespace common {
	namespace utils {

class cTimer: public ITimer
{
private:
	timespec mStartRealTime;
	timespec mStartProcessTime;
	timespec mEndRealTime;
	timespec mEndProcessTime;
	float mRealTime;
	float mProcessTime;

	inline float GetDiffSec(timespec *startTime, timespec *endTime);

public:
	cTimer();

	inline void Reset();
	inline void Start();
	inline void Stop();
	inline void Run();

	inline double GetRealTime() const;
	inline double GetProcessTime() const;
	inline double GetProcessUserTime() const;
	inline double GetProcessKernelTime() const;

	inline void ResetSum();
	void AddTime();

	inline double GetAvgRealTime() const;
	inline double GetAvgProcessTime() const;
	inline double GetAvgProcessUserTime() const;
	inline double GetAvgProcessKernelTime() const;

	void Print(const char *) const;
	void PrintAvg(const char *) const;
	void PrintSum(const char *) const;
};

inline void cTimer::Start()
{
  Reset();
  Run();
}

inline void cTimer::Run()
{
	clock_gettime(CLOCK_REALTIME, &mStartRealTime);
	clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &mStartProcessTime);
}

inline void cTimer::Stop()
{
  clock_gettime(CLOCK_REALTIME, &mEndRealTime);
  clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &mEndProcessTime);
	mRealTime += GetDiffSec(&mStartRealTime, &mEndRealTime);
	mProcessTime += GetDiffSec(&mStartProcessTime, &mEndProcessTime);
}

/**
 * Get difference between two times in seconds.
 */
inline float cTimer::GetDiffSec(timespec *startTime, timespec *endTime)
{
  const unsigned int nano = 1000000000;
  return (float)((endTime->tv_sec - startTime->tv_sec) * nano + (endTime->tv_nsec - startTime->tv_nsec)) / nano;
}

inline void cTimer::Reset()
{
	mRealTime = mProcessTime = 0.0;
}

inline void cTimer::ResetSum()
{
}

inline double cTimer::GetRealTime() const 
{
	return mRealTime;
}

}}
