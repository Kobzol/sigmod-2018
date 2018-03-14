/**
*	For measuring the process time in windows
*
*	\author Michal Kratky
*	\version 0.1
*	\date 2001
**/
namespace common {
	namespace utils {

class cTimer: public ITimer
{
private:
	/** 
	* measurement process time and elapsed time/ or only elapsed time
	*
	* measuring of process time is time consuming operation
	* it is not recommended to use it for multiple calls -- it causes significant degradation of efficiency
	**/
	static const bool PROCESS_TIME = true; 

	FILETIME mStartProcessKernelTime;
	FILETIME mStartProcessUserTime;
	_timeb mStartTime;
	FILETIME mEndProcessKernelTime;
	FILETIME mEndProcessUserTime;
	_timeb mEndTime;
	llong mProcessUserTime, mProcessKernelTime;
	double mTime;
	llong mTimeConst;                    // for conversion into any time unit
	HANDLE mPid;

	double mSumTime, mSumProcessTime, mSumProcessUserTime, mSumProcessKernelTime;
	unsigned int mNumber;              // for average timer

	llong ProcessUserTime() const;
	llong ProcessKernelTime() const;

	double DiffTime( _timeb *end, _timeb *start);

public:
	cTimer();

	inline void Reset();
	inline void Start();
	inline void Stop();
	inline void Run();

	void operator =(const cTimer &timer);
	inline void ResetSum();
	void AddTime();

	inline double GetRealTime() const;
	inline double GetProcessTime() const;
	inline double GetProcessUserTime() const;
	inline double GetProcessKernelTime() const;

	inline double GetAvgRealTime() const;
	inline double GetAvgProcessTime() const;
	inline double GetAvgProcessUserTime() const;
	inline double GetAvgProcessKernelTime() const;

	inline double GetSumProcessTime() const;

	void Print(const char *) const;
	void PrintAvg(const char *) const;
	void PrintSum(const char *) const;
};

inline void cTimer::Start()
{
  Reset();
  Run();
}

inline void cTimer::Stop()
{
	if (PROCESS_TIME)
	{
		FILETIME tc, te;
		GetProcessTimes(mPid, &tc, &te, &mEndProcessKernelTime, &mEndProcessUserTime);
		mProcessUserTime += ProcessUserTime();
		mProcessKernelTime += ProcessKernelTime();
	}

	_ftime_s(&mEndTime);
	mTime += DiffTime(&mEndTime, &mStartTime);
}

inline void cTimer::Run()
{
	if (PROCESS_TIME)
	{
		FILETIME tc, te;
		GetProcessTimes(mPid, &tc, &te, &mStartProcessKernelTime, &mStartProcessUserTime);
	}

	_ftime_s(&mStartTime);
}

inline void cTimer::Reset()
{
	mProcessUserTime = mProcessKernelTime = 0;
	mTime = 0.0;
}

inline void cTimer::ResetSum()
{
	mSumTime = mSumProcessUserTime = mSumProcessKernelTime = mSumProcessTime = 0.0;
	mNumber = 0;
}

inline double cTimer::GetRealTime() const 
{	
	return mTime; 
}

double cTimer::GetSumProcessTime() const 
{	
	return mSumProcessTime; 
}

}}
