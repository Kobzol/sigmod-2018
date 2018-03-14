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

#ifndef __cTimer_h__
#define __cTimer_h__

#include <stdio.h>
#include "../common/cCommon.h"

namespace common {
	namespace utils {

class ITimer
{
public:
  virtual void Reset() = 0;
  virtual void Start() = 0;
  virtual void Stop() = 0;
  virtual void Run() = 0;

  virtual double GetRealTime() const = 0;
  virtual double GetProcessTime() const = 0;
  virtual double GetProcessUserTime() const = 0;
  virtual double GetProcessKernelTime() const = 0;

  virtual void ResetSum() = 0;
  virtual void AddTime() = 0;

  virtual double GetAvgRealTime() const = 0;
  virtual double GetAvgProcessTime() const = 0;
  virtual double GetAvgProcessUserTime() const = 0;
  virtual double GetAvgProcessKernelTime() const = 0;

  virtual void Print(const char *) const = 0;
  virtual void PrintAvg(const char *) const = 0;
  virtual void PrintSum(const char *) const = 0;
};
}}

#ifndef __linux__
	#include <windows.h>
	#include <math.h>
	#include <iostream>
	#include <time.h>
	#include <sys/types.h>
	#include <sys/timeb.h>
	#include "cTimer_win.h"
#else
	#include "cTimer_lnx.h"
#endif

#endif
