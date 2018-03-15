/**************************************************************************}
{                                                                          }
{    cCommon.h                                                             }
{                                                                          }
{                                                                          }
{    Copyright (c) 2001                      Michal Kratky                 }
{                                                                          }
{    VERSION: 0.2                            DATE 5/11/2002                }
{                                                                          }
{    following functionality:                                              }
{       common definitions                                                 }
{                                                                          }
{                                                                          }
{    UPDATE HISTORY:                                                       }
{      xx/xx/xxxx                                                          }
{                                                                          }
{**************************************************************************/

#ifndef __cCommon_h__
#define __cCommon_h__

namespace common {

typedef unsigned int uint;
typedef unsigned short ushort;
typedef unsigned char uchar;
typedef long long llong;
typedef unsigned long long ullong;

#define UNUSED(p) ((void)(p))
class cCommon
{
public:
	static const unsigned char MAX_CHAR = 255;

	cCommon(void);
	~cCommon(void);

	static const unsigned int UNDEFINED_UINT = (unsigned int)~0;
	static const int UNDEFINED_INT = -1;
	static const int MODE_DEC = 0;
	static const int MODE_BIN = 1;
	static const int MODE_CHAR = 2;

	static const int STRING_LENGTH = 1024;
};
}
#endif
