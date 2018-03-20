#include "timer.h"

double get_wall_time()
{
#ifdef  __linux__
	struct timeval time {};
	if (gettimeofday(&time, nullptr))
	{
		return 0;
	}
	return (double)time.tv_sec + (double)time.tv_usec * 0.000001;

#else
	return 0;
#endif //  __linux__
}
