/*
 * PartitionMaster.cpp
 *
 *  Created on: 2013-8-19
 *      Author: root
 */

// #include "MemoryBuffer.h"
// #include "BitmapBuffer.h"
// #include "TripleBitRepository.h"
// #include "EntityIDBuffer.h"
// #include "TripleBitQueryGraph.h"
// #include "util/BufferManager.h"
// #include "util/PartitionBufferManager.h"
// #include "comm/TasksQueueWP.h"
// #include "comm/ResultBuffer.h"
// #include "comm/TasksQueueChunk.h"
// #include "comm/subTaskPackage.h"
// #include "comm/Tasks.h"
// #include "TempBuffer.h"
// #include "MMapBuffer.h"
#include "PartitionMaster.h"
// #include "ThreadPool.h"
// #include "TempMMapBuffer.h"
// #include "util/Timestamp.h"

#define QUERY_TIME


void PartitionMaster::test() {
#ifdef MYDEBUG
	cout << __FUNCTION__ << " partitionID: " << partitionID<< endl;
#endif
    cout<<" hello test"<<endl;
}