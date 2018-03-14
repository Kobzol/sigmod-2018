/**************************************************************************}
{                                                                          }
{    cTimer.cpp                                                            }
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
{      7/8/2002                                                            }
{                                                                          }
{**************************************************************************/

#include "cTimer.h"

namespace common {
	namespace utils {

cTimer::cTimer()
{
	if (PROCESS_TIME)
	{
		mPid = GetCurrentProcess();
	}

	mTimeConst = (llong)pow((float)10,7);
	Reset();
}

/// Copy process and kernel time. Copy only basic information for preserving the time results.
void cTimer::operator =(const cTimer &timer)
{
	mTime = timer.GetRealTime();
	if (PROCESS_TIME)
	{
		mProcessUserTime = timer.GetProcessUserTime();
		mProcessKernelTime = timer.GetProcessKernelTime();
	}
}

llong cTimer::ProcessUserTime() const
{
	llong startTime, endTime;

	CopyMemory(&startTime, &mStartProcessUserTime, sizeof(mStartProcessUserTime));
	CopyMemory(&endTime, &mEndProcessUserTime, sizeof(mEndProcessUserTime));
	return (endTime - startTime); 
}

llong cTimer::ProcessKernelTime() const
{
	llong startTime, endTime;

	CopyMemory(&startTime, &mStartProcessKernelTime, sizeof(mStartProcessKernelTime));
	CopyMemory(&endTime, &mEndProcessKernelTime, sizeof(mEndProcessKernelTime));
	return (endTime - startTime);
}

double cTimer::DiffTime(_timeb *end, _timeb *start)
{
	double endTime = end->time + end->millitm / 1000.0;
	double startTime = start->time + start->millitm / 1000.0;
	double time = endTime - startTime;
	return time;
}

inline double cTimer::GetProcessTime() const       
{ 
	return GetProcessUserTime() + GetProcessKernelTime(); 
}

inline double cTimer::GetProcessUserTime() const   
{ 
	return (double)mProcessUserTime/mTimeConst; 
}

inline double cTimer::GetProcessKernelTime() const 
{ 
	return (double)mProcessKernelTime/mTimeConst; 
}

inline double cTimer::GetAvgRealTime() const        
{	
	return mTime/mNumber; 
}

inline double cTimer::GetAvgProcessTime() const 
{	
	return mSumProcessTime/mNumber; 
}

inline double cTimer::GetAvgProcessUserTime() const 
{	
	return mSumProcessUserTime/mNumber; 
}

inline double cTimer::GetAvgProcessKernelTime() const 
{	
	return mSumProcessKernelTime/mNumber; 
}

void cTimer::AddTime()
{
	mSumTime += GetRealTime();
	if (PROCESS_TIME)
	{
		mSumProcessTime += GetProcessTime();
		mSumProcessUserTime += GetProcessUserTime();
		mSumProcessKernelTime += GetProcessKernelTime();
	}
	mNumber++;
}

void cTimer::Print(const char *str) const
{
	double processTime = GetProcessTime();
	double otherTime = (mTime - processTime);

	printf("%g", mTime);
	printf(" (%g (%g+%g)", processTime, GetProcessUserTime(), GetProcessKernelTime());
	// printf("%g", otherTime);
	printf(")s%s", str);
}


void cTimer::PrintAvg(const char *str) const
{
	double avgProcessTime = GetAvgProcessTime();
	double avgTime = GetAvgRealTime();
	double otherTime = (avgTime - avgProcessTime);

	if (otherTime < 0)
	{
		printf("x");
	}
	else
	{
		printf("%g", GetAvgRealTime());
	}

	printf(" (%g(%g+%g) + ", avgProcessTime, GetAvgProcessUserTime(), GetAvgProcessKernelTime());

	if (otherTime < 0)
	{
		printf("x");
	}
	else
	{
		printf("%g",  otherTime);
	}

	printf(") s%s", str);
}

void cTimer::PrintSum(const char *str) const
{
	double avgProcessTime = mSumProcessTime;
	double avgTime = mSumTime;
	double otherTime = (avgTime - avgProcessTime);

	if (otherTime < 0)
	{
		printf("x");
	}
	else
	{
		printf("%g", mSumTime);
	}

	printf(" (%.3g(%.3g+%.3g) + ", avgProcessTime, mSumProcessUserTime, mSumProcessKernelTime);

	if (otherTime < 0)
	{
		printf("x");
	}
	else
	{
		printf("%.3g",  otherTime);
	}

	printf(") s%s", str);
}

}}
