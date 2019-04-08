/*
 * TripleBitWorker.h
 *
 *  Created on: 2013-6-28
 *      Author: root
 */

#ifndef TRIPLEBITWORKER_H_
#define TRIPLEBITWORKER_H_

class BitmapBuffer;
class URITable;
class PredicateTable;
class TripleBitRepository;
class TripleBitQueryGraph;
class EntityIDBuffer;
class HashJoin;
class SPARQLLexer;
class SPARQLParser;
class QuerySemanticAnalysis;
class PlanGenerator;
class transQueueSW;
class transaction;
class TasksQueueWP;
class TripleBitWorkerQuery;

#include "TripleBit.h"
#include <boost/thread/mutex.hpp>

class TripleBitWorker
{
private:
	TripleBitRepository* tripleBitRepo;
	PredicateTable* preTable;
	URITable* uriTable;
	BitmapBuffer* bitmapBuffer;
	transQueueSW* transQueSW;

	QuerySemanticAnalysis* semAnalysis;
	PlanGenerator* planGen;
	TripleBitQueryGraph* queryGraph;
	vector<string> resultSet;
	vector<string>::iterator resBegin;
	vector<string>::iterator resEnd;

	ID workerID;

	boost::mutex* uriMutex;

	TripleBitWorkerQuery* workerQuery;
	transaction *trans;

public:
	TripleBitWorker(TripleBitRepository* repo, ID workID);
	Status Execute(string& queryString);
	~TripleBitWorker(){}
	void Work();
	void Print();
};

#endif /* TRIPLEBITWORKER_H_ */
