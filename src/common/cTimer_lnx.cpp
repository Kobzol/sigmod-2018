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
}

inline double cTimer::GetProcessTime() const       
{ 
  printf("This method is not implemented!\n");
	return 0;
}

inline double cTimer::GetProcessUserTime() const   
{ 
  printf("This method is not implemented!\n");
	return 0;
}

inline double cTimer::GetProcessKernelTime() const 
{ 
  printf("This method is not implemented!\n");
	return 0;
}

inline double cTimer::GetAvgRealTime() const        
{	
  printf("This method is not implemented!\n");
	return 0;
}

inline double cTimer::GetAvgProcessTime() const 
{	
  printf("This method is not implemented!\n");
	return 0;
}

inline double cTimer::GetAvgProcessUserTime() const 
{	
  printf("This method is not implemented!\n");
	return 0;
}

inline double cTimer::GetAvgProcessKernelTime() const 
{	
  printf("This method is not implemented!\n");
	return 0;
}

void cTimer::AddTime()
{
  printf("This method is not implemented!\n");
}

void cTimer::Print(const char *str) const
{
	printf("RT: %.4f, PT: %.4f [s]%s", mRealTime, mProcessTime, str);
}

void cTimer::PrintAvg(const char *str) const
{
  printf("This method is not implemented!\n");
}

void cTimer::PrintSum(const char *str) const
{
  printf("This method is not implemented!\n");
}

}}

