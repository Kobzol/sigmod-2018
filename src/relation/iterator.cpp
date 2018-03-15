#include "iterator.h"

unsigned int Iterator::operatorIndexCounter = 0;

void Iterator::printResult()
{
	unsigned int columnCount = getColumnCount();

	while (getNext())
	{
		for (unsigned int i = 0; i < columnCount; i++)
		{
			int64_t val = getColumn(i);
			printf("%10d", val);
		}
		printf("\n");
	}
}

void Iterator::writeRowToFile()
{
	//char fileName[100];
	//sprintf(fileName, "c:\\users\\petr\\desktop\\X_%d.txt", operatorIndex);

	//FILE* file = fopen(fileName, "a");

	//unsigned int columnCount = getColumnCount();
	//for (unsigned int i = 0; i < columnCount; i++)
	//{
	//	if (i > 0)
	//	{
	//		fprintf(file, "\t");
	//	}

	//	int64_t val = getColumn(i);
	//	fprintf(file, "%d", val);
	//}
	//fprintf(file, "\n");

	//fclose(file);
}