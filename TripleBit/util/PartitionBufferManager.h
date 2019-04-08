/*
 * PartitionBufferManager.h
 *
 *  Created on: 2013-8-16
 *      Author: root
 */

#ifndef PARTITIONBUFFERMANAGER_H_
#define PARTITIONBUFFERMANAGER_H_

#define INIT_PARTITION_BUFFERS 20
#define MAX_BUFFERS 100

class EntityIDBuffer;

#include "../TripleBit.h"
#include <boost/thread/mutex.hpp>

class PartitionBufferManager {
private:
	vector<EntityIDBuffer*> bufferPool;
	vector<EntityIDBuffer*> usedBuffer;
	vector<EntityIDBuffer*> cleanBuffer;
	int bufferCnt;

	boost::mutex bufferMutex;

public:
	PartitionBufferManager();
	PartitionBufferManager(int initBufferNum);
	virtual ~PartitionBufferManager();
	EntityIDBuffer* getNewBuffer();
	Status freeBuffer(EntityIDBuffer* buffer);
	Status reserveBuffer();
	void destroyBuffers();
};

#endif /* PARTITIONBUFFERMANAGER_H_ */
