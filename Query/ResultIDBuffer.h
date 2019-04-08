/*
 * ResultIDBuffer.h
 *
 *  Created on: 2013-12-18
 *      Author: root
 */

#ifndef RESULTIDBUFFER_H_
#define RESULTIDBUFFER_H_

#include "TripleBit.h"
#include "EntityIDBuffer.h"
#include "comm/subTaskPackage.h"

using namespace boost;

class ResultIDBuffer
{
// private:
public:
	bool isEntityID;
	EntityIDBuffer *buffer;
	shared_ptr<subTaskPackage> taskPackage;
	int IDCount;

public:
	ResultIDBuffer(){}
	ResultIDBuffer(shared_ptr<subTaskPackage> package);
	~ResultIDBuffer();
	bool isEntityIDBuffer() { return isEntityID; }
	shared_ptr<subTaskPackage> getTaskPackage() { return taskPackage; }
	EntityIDBuffer *getEntityIDBuffer();
	void transForEntityIDBuffer();
	void setEntityIDBuffer(EntityIDBuffer *buf);
	void setTaskPackage(shared_ptr<subTaskPackage> package);

	void getMinMax(ID &min, ID  &max);
	int getIDCount();
	size_t getSize();
	Status sort(int sortKey);
	ID *getBuffer();
	void print();
	void print2ID(EntityIDBuffer *ed);
	size_t printMinMax(ID &a,ID &b, ID &c,ID &d);
};

#endif /* RESULTIDBUFFER_H_ */
