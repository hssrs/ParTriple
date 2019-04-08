/*
 * TripleBitWorkerQuery.cpp
 *
 *  Created on: 2013-7-1
 *      Author: root
 */
#include "MemoryBuffer.h"
#include "BitmapBuffer.h"
#include "TripleBitWorkerQuery.h"
#include "TripleBitRepository.h"
#include "URITable.h"
#include "PredicateTable.h"
#include "TripleBitQueryGraph.h"
#include "EntityIDBuffer.h"
#include "util/BufferManager.h"
#include "comm/TasksQueueWP.h"
#include "comm/ResultBuffer.h"
#include "comm/IndexForTT.h"
#include "comm/Tasks.h"
#include <algorithm>
#include <math.h>
#include <pthread.h>
// #include <mutex>
#include "util/Timestamp.h"
// #include <all.hpp>
// #include "./bloom_filter.h"
#include "./CBitMap.h"
#include <tbb/tbb.h>

#define BLOCK_SIZE 1024
//#define PRINT_RESULT
#define COLDCACHE

int ans = 0;
int gR = 0;
ID xMin = UINT_MAX;
ID xMax = 0;
ID zMin = UINT_MAX;
ID zMax = 0;





// std::mutex xMutex;
bool isP5 = false;
bool isP3 = false;

// #include "libcuckoo/cuckoohash_map.hh"
// const int mod =  128;
// const ID flag = 128;
// cuckoohash_map<ID,int> fountain[4][xflag+1];

// bf::basic_bloom_filter * fountain[4][3];
// bloom_filter * fountain[4][3];
// CBitMap *fountain[4][4];

vector<vector<CBitMap *> > fountain;
map<ID,pair<ID,ID> > var_min_max;


ID firstVar;
ID secVar;
int firstHeight;
int secHeight;



void setBM(ID max_ID){
	// ID minID[4] = {0,454,10,13};
	ID minID[4] = {0,1,1,1};
	ID maxID[4] = {0,max_ID,max_ID,max_ID};

	for(int i=1;i<4;i++){
		for(int j=0;j<4;j++){
			// bloom_parameters parameters;
			// // How many elements roughly do we expect to insert?
			// parameters.projected_element_count = maxSize[i]*64;
			// // Maximum tolerable false positive probability? (0,1)
			// parameters.false_positive_probability = 0.0001; // 1 in 10000
			// // Simple randomizer (optional)
			// parameters.random_seed = 0xA5A5A5A5;

			// parameters.compute_optimal_parameters();

			fountain[i][j] = new CBitMap(minID[i],maxID[i]);
			// fountain[i][j] = new bf::basic_bloom_filter(0.99,maxSize[i]*64);
		}
	}
}

void setSize(){
	// cout<<"-------setSize--------"<<endl;
	// size_t maxSize[4] = {0,108449,13552,84988};
	// for(int i=1;i<4;i++){
	// 	for(int j=0;j<xflag+1;j++){
	// 		fountain[i][j].reserve(maxSize[i]);
	// 	}
	// }
}

void to_reduce_p5(EntityIDBuffer * idb){
	size_t total = idb->getSize();
	size_t j = 0;
	size_t i;

	ID* p = idb->buffer;
	// ID *buffer = idb->buffer;

	size_t IDCount = idb->getIDCount();

	int k;

	// int * pval = NULL;


	for( i = 0 ; i != total; i++)
	{
		// if(fountain[1][p[(i * IDCount + 0)]&xflag].find_free(p[(i * IDCount + 0)],pval) && *pval == 2){
		if(fountain[1][2]->contains(p[(i * IDCount + 0)])){
			for( k = 0; k < IDCount; k++)
			{
				p[j*IDCount+k] = p[i * IDCount + k];
			}
			j++;
		}
	}

	idb->setSize(j);
}

void reduce_p5(ResultIDBuffer *res5){
	// map<ID, EntityIDBuffer*>::iterator iter;
	
	// vector<EntityIDBuffer*> eds;

	// for(auto iter = res5->taskPackage->xTempBuffer.begin(); iter != res5->taskPackage->xTempBuffer.end(); iter++){
	// 	// totalSize += iter->second->getSize();
	// 	eds.push_back(iter->second);
	// }

	// for(iter = res5->taskPackage->xyTempBuffer.begin(); iter != res5->taskPackage->xyTempBuffer.end(); iter++){
	// 	// totalSize += iter->second->getSize();
	// 	eds.push_back(iter->second);
	// }

	// std::for_each(eds.begin(),eds.end(),to_reduce_p5);
	//tbb::parallel_do(eds.begin(),eds.end(),[](EntityIDBuffer *task) {to_reduce_p5(task);});
	// return totalSize;
}



void to_reduce_p(EntityIDBuffer * idb){
	size_t total = idb->getSize();
	size_t j = 0;
	size_t i;

	ID* p = idb->buffer;
	// ID *buffer = idb->buffer;

	size_t IDCount = idb->getIDCount();

	int k;

	// int * pval = NULL;


	for( i = 0 ; i != total; i++)
	{
		// if(fountain[1][p[(i * IDCount + 0)]&xflag].find_free(p[(i * IDCount + 0)],pval) && *pval == 2){
		if(fountain[firstVar][fountain[firstVar].size()-1]->contains(p[i*IDCount+0]) &&
		fountain[secVar][fountain[secVar].size()-1]->contains(p[i*IDCount+1])){
			for( k = 0; k < IDCount; k++)
			{
				p[j*IDCount+k] = p[i * IDCount + k];
			}
			j++;

		}
	}
	idb->setSize(j);
}

void doGroupReduce(vector<EntityIDBuffer *> & eds,int begin,int end){
	for(int i = begin; i < end; i++){
		to_reduce_p(eds[i]);
	}
}

void reduce_p(ResultIDBuffer *res){
	map<ID, EntityIDBuffer*>::iterator iter;
	
	vector<EntityIDBuffer*> eds;

	for(iter = res->taskPackage->xTempBuffer.begin(); iter != res->taskPackage->xTempBuffer.end(); iter++){
		// totalSize += iter->second->getSize();
		eds.push_back(iter->second);
	}

	for(iter = res->taskPackage->xyTempBuffer.begin(); iter != res->taskPackage->xyTempBuffer.end(); iter++){
		// totalSize += iter->second->getSize();
		eds.push_back(iter->second);
	}


	// #pragma omp parallel for num_threads(openMP_thread_num)

	int _chunkCount = TEST_THREAD_NUMBER;
	size_t chunkSize =  eds.size() / _chunkCount;
	int i = 0;
	for( i = 0; i < _chunkCount; i++ )
	{
		if( i == _chunkCount -1 ){
			ThreadPool::getTestPool().addTask(boost::bind(&doGroupReduce,eds,i*chunkSize,eds.size()-1));
		}
		else{

			ThreadPool::getTestPool().addTask(boost::bind(&doGroupReduce,eds,i*chunkSize,i * chunkSize + chunkSize - 1));
		}
	}
	ThreadPool::getTestPool().wait();
	
	
	
	
	
	
	// for(int i = 0; i < eds.size() ;i++){
	// 	to_reduce_p(eds[i]);
	// }

	// for(vector<EntityIDBuffer*>::iterator iter = eds.begin(); iter != eds.end(); iter++){
	// 	to_reduce_p(*iter);
	// }

	// std::for_each(eds.begin(),eds.end(),to_reduce_p5);
	// tbb::parallel_do(eds.begin(),eds.end(),[](EntityIDBuffer *task) {to_reduce_p(task);});
	// return totalSize;
}

// void to_reduce_p0(EntityIDBuffer * idb){
// 	size_t total = idb->getSize();
// 	size_t j = 0;
// 	size_t i;

// 	ID* p = idb->buffer;
// 	// ID *buffer = idb->buffer;

// 	size_t IDCount = idb->getIDCount();

// 	int k;

// 	// int * pval = NULL;


// 	for( i = 0 ; i != total; i++)
// 	{
// 		// if(fountain[1][p[(i * IDCount + 0)]&xflag].find_free(p[(i * IDCount + 0)],pval) && *pval == 2){
		
		
// 		if(fountain[3][2]->contains(p[i*IDCount + 0]) && fountain[2][2]->contains(p[i*IDCount+1])){
// 			for( k = 0; k < IDCount; k++)
// 			{
// 				p[j*IDCount+k] = p[i * IDCount + k];
// 			}
// 			j++;
// 		}
// 	}
// 	idb->setSize(j);
// }




// void reduce_p0(ResultIDBuffer *res5){
// 	map<ID, EntityIDBuffer*>::iterator iter;
	
// 	vector<EntityIDBuffer*> eds;

// 	for(auto iter = res5->taskPackage->xTempBuffer.begin(); iter != res5->taskPackage->xTempBuffer.end(); iter++){
// 		// totalSize += iter->second->getSize();
// 		eds.push_back(iter->second);
// 	}

// 	for(iter = res5->taskPackage->xyTempBuffer.begin(); iter != res5->taskPackage->xyTempBuffer.end(); iter++){
// 		// totalSize += iter->second->getSize();
// 		eds.push_back(iter->second);
// 	}

// 	// std::for_each(eds.begin(),eds.end(),to_reduce_p5);
// 	tbb::parallel_do(eds.begin(),eds.end(),[](EntityIDBuffer *task) {to_reduce_p0(task);});
// 	// return totalSize;
// }


TripleBitWorkerQuery::TripleBitWorkerQuery(TripleBitRepository*& repo, ID workID) {
	tripleBitRepo = repo;
	bitmap = repo->getBitmapBuffer();
	uriTable = repo->getURITable();
	preTable = repo->getPredicateTable();

	workerID = workID; // 1 ~ WORKERNUM
	max_id = repo->getURITable()->getMaxID();

	tasksQueueWP = repo->getTasksQueueWP();
	resultWP = repo->getResultWP();
	tasksQueueWPMutex = repo->getTasksQueueWPMutex();
	partitionNum = repo->getPartitionNum();

	for (int partitionID = 1; partitionID <= partitionNum; ++partitionID) {
		tasksQueue[partitionID] = tasksQueueWP[partitionID - 1];
	}

	for (int partitionID = 1; partitionID <= partitionNum; ++partitionID) {
		resultBuffer[partitionID] = resultWP[(workerID - 1) * partitionNum + partitionID - 1];
	}
}

TripleBitWorkerQuery::~TripleBitWorkerQuery() {
	EntityIDList.clear();
}

void TripleBitWorkerQuery::releaseBuffer() {
	idTreeBFS.clear();
	leafNode.clear();
	varVec.clear();

	EntityIDListIterType iter = EntityIDList.begin();

	for (; iter != EntityIDList.end(); iter++) {
		if (iter->second != NULL)
			BufferManager::getInstance()->freeBuffer(iter->second);
	}

	BufferManager::getInstance()->reserveBuffer();
	EntityIDList.clear();
}

Status TripleBitWorkerQuery::query(TripleBitQueryGraph* queryGraph, vector<string>& resultSet, timeval& transTime) {
	this->_queryGraph = queryGraph;
	this->_query = &(queryGraph->getQuery());
	this->resultPtr = &resultSet;
	this->transactionTime = &transTime;

	switch (_queryGraph->getOpType()) {
	case TripleBitQueryGraph::QUERY:
		return excuteQuery();
	case TripleBitQueryGraph::INSERT_DATA:
		return excuteInsertData();
	case TripleBitQueryGraph::DELETE_DATA:
		return excuteDeleteData();
	case TripleBitQueryGraph::DELETE_CLAUSE:
		return excuteDeleteClause();
	case TripleBitQueryGraph::UPDATE:
		return excuteUpdate();
	}
}

Status TripleBitWorkerQuery::excuteQuery() {
	if (_query->joinVariables.size() == 1) {
#ifdef MYDEBUG
		cout << "execute singleVariableJoin" << endl;
#endif
		singleVariableJoin();
	} else {
		if (_query->joinGraph == TripleBitQueryGraph::ACYCLIC) {
#ifdef MYDEBUG
			cout << "execute asyclicJoin" << endl;
#endif
			acyclicJoin();
		} else if (_query->joinGraph == TripleBitQueryGraph::CYCLIC) {
#ifdef MYDEBUG
			cout << "execute cyclicJoin" << endl;
#endif
			cyclicJoin();
		}
	}
	return OK;
}


void TripleBitWorkerQuery::tasksEnQueue(ID partitionID, SubTrans *subTrans) {
	if (tasksQueue[partitionID]->Queue_Empty()) {
		tasksQueue[partitionID]->EnQueue(subTrans);
		ThreadPool::getPartitionPool().addTask(boost::bind(&PartitionMaster::Work, tripleBitRepo->getPartitionMaster(partitionID)));
	} else {
		tasksQueue[partitionID]->EnQueue(subTrans);
	}
}

Status TripleBitWorkerQuery::excuteInsertData() {
	size_t tripleSize = _query->tripleNodes.size();
	shared_ptr<IndexForTT> indexForTT(new IndexForTT(tripleSize * 2));

	classifyTripleNode();

	TripleBitQueryGraph::OpType operationType = TripleBitQueryGraph::INSERT_DATA;

	map<ID, set<TripleNode*> >::iterator iter = tripleNodeMap.begin();
	for (; iter != tripleNodeMap.end(); ++iter) {
		size_t tripleNodeSize = iter->second.size();
		ID partitionID = iter->first;
		set<TripleNode*>::iterator tripleNodeIter = iter->second.begin();

		tasksQueueWPMutex[partitionID - 1]->lock();
		for (; tripleNodeIter != iter->second.end(); ++tripleNodeIter) {
			SubTrans *subTrans = new SubTrans(*transactionTime, workerID, 0, 0, operationType, tripleNodeSize, *(*tripleNodeIter), indexForTT);
			tasksEnQueue(partitionID, subTrans);
		}
		tasksQueueWPMutex[partitionID - 1]->unlock();
	}

	indexForTT->wait();
	return OK;
}

Status TripleBitWorkerQuery::excuteDeleteData() {
	shared_ptr<IndexForTT> indexForTT(new IndexForTT);

	classifyTripleNode();

	TripleBitQueryGraph::OpType operationType = TripleBitQueryGraph::DELETE_DATA;

	map<ID, set<TripleNode*> >::iterator iter = tripleNodeMap.begin();
	for (; iter != tripleNodeMap.end(); ++iter) {
		size_t tripleNodeSize = iter->second.size();
		ID partitionID = iter->first;
		set<TripleNode*>::iterator tripleNodeIter = iter->second.begin();

		tasksQueueWPMutex[partitionID - 1]->lock();
		for (; tripleNodeIter != iter->second.end(); ++tripleNodeIter) {
			SubTrans *subTrans = new SubTrans(*transactionTime, workerID, 0, 0, operationType, tripleNodeSize, *(*tripleNodeIter), indexForTT);
			tasksEnQueue(partitionID, subTrans);
		}
		tasksQueueWPMutex[partitionID - 1]->unlock();
	}

	return OK;
}

Status TripleBitWorkerQuery::excuteDeleteClause() {
	shared_ptr<IndexForTT> indexForTT(new IndexForTT);

	vector<TripleNode>::iterator iter = _query->tripleNodes.begin();
	ID partitionID = iter->predicate;
	TripleBitQueryGraph::OpType operationType = TripleBitQueryGraph::DELETE_CLAUSE;

	SubTrans *subTrans = new SubTrans(*transactionTime, workerID, 0, 0, operationType, 1, *iter, indexForTT);
	tasksEnQueue(partitionID, subTrans);

	return OK;
}

Status TripleBitWorkerQuery::excuteUpdate() {
	shared_ptr<IndexForTT> indexForTT(new IndexForTT);

	vector<TripleNode>::iterator iter = _query->tripleNodes.begin();
	ID partitionID = iter->predicate;
	TripleBitQueryGraph::OpType operationType = TripleBitQueryGraph::UPDATE;

	SubTrans *subTrans1 = new SubTrans(*transactionTime, workerID, 0, 0, operationType, 2, *iter, indexForTT);
	SubTrans *subTrans2 = new SubTrans(*transactionTime, workerID, 0, 0, operationType, 2, *++iter, indexForTT);

	tasksQueueWPMutex[partitionID - 1]->lock();
	tasksEnQueue(partitionID, subTrans1);
	tasksEnQueue(partitionID, subTrans2);
	tasksQueueWPMutex[partitionID - 1]->unlock();

	return OK;
}

void TripleBitWorkerQuery::classifyTripleNode() {
	tripleNodeMap.clear();
	vector<TripleNode>::iterator iter = _query->tripleNodes.begin();

	for (; iter != _query->tripleNodes.end(); ++iter) {
		tripleNodeMap[iter->predicate].insert(&(*iter));
	}
}

static void generateProjectionBitVector(uint& bitv, std::vector<ID>& project) {
	bitv = 0;
	for (size_t i = 0; i != project.size(); ++i) {
		bitv |= 1 << project[i];
	}
}

static void generateTripleNodeBitVector(uint& bitv, TripleNode& node) {
	bitv = 0;
	if (!node.constSubject)
		bitv |= (1 << node.subject);
	if (!node.constPredicate)
		bitv |= (1 << node.predicate);
	if (!node.constObject)
		bitv |= (1 << node.object);
}

static size_t countOneBits(uint bitv) {
	size_t count = 0;
	while (bitv) {
		bitv = bitv & (bitv - 1);
		count++;
	}
	return count;
}

static ID bitVtoID(uint bitv) {
	uint mask = 0x1;
	ID count = 0;
	while (true) {
		if ((mask & bitv) == mask)
			break;
		bitv = bitv >> 1;
		count++;
	}
	return count;
}

static int insertVarID(ID key, std::vector<ID>& idvec, TripleNode& node, ID& sortID) {
	int ret = 0;
	switch (node.scanOperation) {
	case TripleNode::FINDO:
	case TripleNode::FINDOBYP:
	case TripleNode::FINDOBYS:
	case TripleNode::FINDOBYSP:
		if (key != node.object)
			idvec.push_back(node.object);
		sortID = node.object;
		break;
	case TripleNode::FINDOPBYS:
		if (key != node.object)
			idvec.push_back(node.object);
		else {
			sortID = node.object;
			ret = 0;
		}
		if (key != node.predicate)
			idvec.push_back(node.predicate);
		else {
			sortID = node.predicate;
			ret = 1;
		}
		break;
	case TripleNode::FINDOSBYP:
		if (key != node.object)
			idvec.push_back(node.object);
		else {
			sortID = node.object;
			ret = 0;
		}
		if (key != node.subject)
			idvec.push_back(node.subject);
		else {
			sortID = node.subject;
			ret = 1;
		}
		break;
	case TripleNode::FINDP:
	case TripleNode::FINDPBYO:
	case TripleNode::FINDPBYS:
	case TripleNode::FINDPBYSO:
		if (key != node.predicate)
			idvec.push_back(node.predicate);
		sortID = node.predicate;
		break;
	case TripleNode::FINDPOBYS:
		if (key != node.predicate)
			idvec.push_back(node.predicate);
		else {
			sortID = node.predicate;
			ret = 0;
		}
		if (key != node.object)
			idvec.push_back(node.object);
		else {
			sortID = node.object;
			ret = 1;
		}
		break;
	case TripleNode::FINDPSBYO:
		if (key != node.predicate)
			idvec.push_back(node.predicate);
		else {
			sortID = node.predicate;
			ret = 0;
		}
		if (key != node.subject)
			idvec.push_back(node.subject);
		else {
			sortID = node.subject;
			ret = 1;
		}
		break;
	case TripleNode::FINDS:
	case TripleNode::FINDSBYO:
	case TripleNode::FINDSBYP:
	case TripleNode::FINDSBYPO:
		if (key != node.subject)
			idvec.push_back(node.subject);
		sortID = node.subject;
		break;
	case TripleNode::FINDSOBYP:
		if (key != node.subject)
			idvec.push_back(node.subject);
		else {
			sortID = node.subject;
			ret = 0;
		}
		if (key != node.object)
			idvec.push_back(node.object);
		else {
			sortID = node.object;
			ret = 1;
		}
		break;
	case TripleNode::FINDSPBYO:
		if (key != node.subject)
			idvec.push_back(node.subject);
		else {
			sortID = node.subject;
			ret = 0;
		}
		if (key != node.predicate)
			idvec.push_back(node.predicate);
		else {
			sortID = node.predicate;
			ret = 1;
		}
		break;
	case TripleNode::NOOP:
		break;
	}
	
	return ret;
}

static void generateResultPos(std::vector<ID>& idVec, std::vector<ID>& projection, std::vector<int>& resultPos) {
	resultPos.clear();

	std::vector<ID>::iterator iter;
	for (size_t i = 0; i != projection.size(); ++i) {
		iter = find(idVec.begin(), idVec.end(), projection[i]);
		resultPos.push_back(iter - idVec.begin());
	}
}

static void generateVerifyPos(std::vector<ID>& idVec, std::vector<int>& verifyPos) {
	verifyPos.clear();
	size_t i, j;
	size_t size = idVec.size();
	for (i = 0; i != size; ++i) {
		for (j = i + 1; j != size; j++) {
			if (idVec[i] == idVec[j]) {
				verifyPos.push_back(i);
				verifyPos.push_back(j);
			}
		}
	}
}

void PrintEntityBufferID(EntityIDBuffer *buffer) {
	size_t size = buffer->getSize();
	ID* resultBuffer = buffer->getBuffer();

	for (size_t i = 0; i < size; ++i) {
		cout << "ID:" << resultBuffer[i] << endl;
	}
	cout << "ResultBuffer Size: " << size << endl;
}


void printSomethingWorker(EntityIDBuffer* buffer) {
	size_t size = buffer->getSize();
	int count = 0;
	ID* resultBuffer = buffer->getBuffer();

	int IDCount = buffer->getIDCount();

	if (IDCount == 1) {
		for (size_t i = 1; i < size; ++i) {
			if (resultBuffer[i - 1] > resultBuffer[i])
				cout << "Buffer Error" << endl;
		}
	} else if (IDCount == 2) {
		for (size_t i = 3; i < size; i = i + 2) {
			if (resultBuffer[i - 3] > resultBuffer[i - 1])
				cout << "Buffer Error i-3,i-1" << endl;
			else if (resultBuffer[i - 3] == resultBuffer[i - 1]) {
				if (resultBuffer[i - 2] > resultBuffer[i])
					cout << "Buffer Error i-2,i" << endl;
			}
		}
	}

	for (size_t i = 0; i < size; ++i) {
		cout << "ID:" << resultBuffer[i] << ' ';
		count++;
		if (count % 20 == 0)
			cout << endl;
	}
	cout << "ResultBuffer Size: " << size << endl;
	cout << endl;
}

// Status TripleBitWorkerQuery::singleVariableJoin() {
// 	vector<TripleBitQueryGraph::JoinVariableNodeID>::iterator joinNodeIter = _query->joinVariables.begin();
// 	vector<pair<TripleBitQueryGraph::TripleNodeID, TripleBitQueryGraph::JoinVariableNode::DimType> >::iterator nodePatternIter;

// 	TripleBitQueryGraph::JoinVariableNode* node = NULL;
// 	EntityIDBuffer* buffer = NULL;
// 	TripleNode* triple = NULL;

// 	//TODO Initialize the first query pattern's triple of the pattern group which has the same variable
// 	getVariableNodeByID(node, *joinNodeIter);
// 	nodePatternIter = node->appear_tpnodes.begin();

// 	getTripleNodeByID(triple, nodePatternIter->first);
// 	buffer = findEntityIDByTriple(triple, 0, INT_MAX)->getEntityIDBuffer();
// 	if (buffer->getSize() == 0) {
// #ifdef PRINT_RESULT
// 		cout << "empty result" << endl;
// #else
// 		resultPtr->push_back("-1");
// 		resultPtr->push_back("Empty Result");
// #endif
// 		return OK;
// 	}
// 	EntityIDList[nodePatternIter->first] = buffer;
// 	nodePatternIter++;

// 	ResultIDBuffer* tempBuffer;
// 	ID minID, maxID;

// 	if (_queryGraph->getProjection().size() == 1) {
// 		for (; nodePatternIter != node->appear_tpnodes.end(); ++nodePatternIter) {
// 			buffer->getMinMax(minID, maxID);
// 			getTripleNodeByID(triple, nodePatternIter->first);
// 			tempBuffer = findEntityIDByTriple(triple, minID, maxID);
// 			mergeJoin.Join(buffer, tempBuffer, 1, 1, false);
// 			if (buffer->getSize() == 0) {
// #ifdef PRINT_RESULT
// 				cout << "empty result" << endl;
// #else
// 				resultPtr->push_back("-1");
// 				resultPtr->push_back("Empty Result");
// #endif
// 				return OK;
// 			}
// 		}
// 	} else {
// 		for (; nodePatternIter != node->appear_tpnodes.end(); nodePatternIter++) {
// 			buffer->getMinMax(minID, maxID);
// 			getTripleNodeByID(triple, nodePatternIter->first);
// 			tempBuffer = findEntityIDByTriple(triple, minID, maxID);
// 			mergeJoin.Join(buffer, tempBuffer, 1, 1, true);
// 			if (buffer->getSize() == 0) {
// #ifdef PRINT_RESULT
// 				cout << "empty result" << endl;
// #else
// 				resultPtr->push_back("-1");
// 				resultPtr->push_back("Empty Result");
// #endif
// 				return OK;
// 			}
// 			EntityIDList[nodePatternIter->first] = tempBuffer->getEntityIDBuffer();
// 		}
// 	}

// 	//TODO materialization the result;
// 	size_t i;
// 	size_t size = buffer->getSize();

// 	std::string URI;
// 	ID* p = buffer->getBuffer();

// 	size_t projectNo = _queryGraph->getProjection().size();
// #ifndef PRINT_RESULT
// 	char temp[2];
// 	sprintf(temp, "%d", projectNo);
// 	resultPtr->push_back(temp);
// #endif

// 	if (projectNo == 1) {
// 		for (i = 0; i < size; ++i) {
// 			if (uriTable->getURIById(URI, p[i]) == OK) {
// #ifdef PRINT_RESULT
// 				cout << URI << endl;
// #else
// 				resultPtr->push_back("NULL");
// #endif
// 			} else {
// #ifdef PRINT_RESULT
// 				cout << p[i] << " " << "not found" << endl;
// #else
// 				resultPtr->push_back("NULL");
// #endif
// 			}
// 		}
// 	} else {
// 		std::vector<EntityIDBuffer*> bufferlist;
// 		std::vector<ID> resultVar;
// 		resultVar.resize(0);
// 		uint projBitV, nodeBitV, resultBitV, tempBitV;
// 		resultBitV = 0;
// 		ID sortID;
// 		int keyp;
// 		keyPos.clear();

// 		generateProjectionBitVector(projBitV, _queryGraph->getProjection());
// 		for (i = 0; i != _query->tripleNodes.size(); i++) {
// 			//generate the bit vector of query pattern
// 			generateTripleNodeBitVector(nodeBitV, _query->tripleNodes[i]);
// 			//get the common bit
// 			tempBitV = projBitV & nodeBitV;
// 			if (tempBitV == nodeBitV) {
// 				//the query pattern which contains two or more variables is better
// 				if (EntityIDList.count(_query->tripleNodes[i].tripleNodeID) == 0 || (EntityIDList[_query->tripleNodes[i].tripleNodeID]->getIDCount() == 1))
// 					continue;
// 				bufferlist.push_back(EntityIDList[_query->tripleNodes[i].tripleNodeID]);
// 				if (countOneBits(resultBitV) == 0) {
// 					//the first time, last joinKey should be set as UNIT_MAX
// 					keyp = insertVarID(UINT_MAX, resultVar, _query->tripleNodes[i], sortID);
// 				} else {
// 					keyp = insertVarID(*joinNodeIter, resultVar, _query->tripleNodes[i], sortID);
// 					keyPos.push_back(keyp);
// 				}
// 				resultBitV |= nodeBitV;
// 				//the buffer of query pattern is enough
// 				if (countOneBits(resultBitV) == projectNo)
// 					break;
// 			}
// 		}

// 		if (projectNo == 2) {
// 			generateResultPos(resultVar, _queryGraph->getProjection(), resultPos);

// 			EntityIDBuffer* buf = bufferlist[0];
// 			size_t bufsize = buf->getSize();
// 			ID* ids = buf->getBuffer();
// 			int IDCount = buf->getIDCount();
// 			for (i = 0; i < bufsize; i++) {
// 				for (int j = 0; j < IDCount; j++) {
// 					if (uriTable->getURIById(URI, ids[i * IDCount + resultPos[j]]) == OK) {
// #ifdef PRINT_RESULT
// 						cout << URI << " ";
// #else
// 						resultPtr->push_back(URI);
// #endif
// 					} else
// 						cout << "not found" << endl;
// 				}
// 				cout << endl;
// 			}
// 		} else {
// 			generateResultPos(resultVar, _queryGraph->getProjection(), resultPos);
// 			needselect = false;

// 			EntityIDBuffer* buf = bufferlist[0];
// 			size_t bufsize = buf->getSize();
// 			bufPreIndexs.clear();
// 			bufPreIndexs.resize(bufferlist.size(), 0);
// 			ID key;
// 			int IDCount = buf->getIDCount();
// 			ID* ids = buf->getBuffer();
// 			int sortKey = buf->getSortKey();
// 			for (i = 0; i != bufsize; i++) {
// 				resultVec.resize(0);
// 				key = ids[i * IDCount + sortKey];
// 				for (int j = 0; j < IDCount; j++) {
// 					resultVec.push_back(ids[i * IDCount + j]);
// 				}

// 				bool ret = getResult(key, bufferlist, 1);
// 				if (ret == false) {
// 					while (i < bufsize && ids[i * IDCount + sortKey] == key) {
// 						i++;
// 					}
// 					i--;
// 				}
// 			}
// 		}
// 	}

// 	return OK;
// }


bool isSingle = false;


// Status TripleBitWorkerQuery::singleVariableJoin(){
// 	// Timestamp t1;
// 	setBM(max_id);
// 	// setBM(tripleBitRepo->UriTable->getMaxID());	
// 	ID minID = 0;
// 	ID maxID = UINT_MAX;
// 	// EntityIDBuffer* buffer = NULL;
// 	TripleNode* triple = NULL;
	

// 	map<TripleNodeID,ResultIDBuffer *> res;
// 	EntityIDBuffer *buffer = new EntityIDBuffer;

// 	int tsize = _query->tripleNodes.size();
// 	int i = 0;
// 	// for(auto & x : _query->tripleNodes){
// 	for(int i = 0;i < _query->tripleNodes.size(); i++){
// 		TripleNode& x = _query->tripleNodes[i]; 
// 		if(i == tsize-1){
// 			isSingle = true;
// 		}
// 		cout<<"varHeight "<<x.varHeight.size()<<endl;
// 		firstVar = x.varHeight[0].first;
// 		firstHeight = x.varHeight[0].second;
// 		cout<<"firstVar "<<firstVar<<endl;
// 		cout<<"firstHeight "<<firstHeight<<endl;
// 		if(x.varHeight.size() > 1){
// 			secVar = x.varHeight[1].first;
// 			secHeight = x.varHeight[1].second;
// 			cout<<"secVar "<<secVar<<endl;
// 			cout<<"secHeight "<<secHeight<<endl;
// 		}


// 		res[x.tripleNodeID] = findEntityIDByTriple(&x, minID, maxID);
// 		if(isSingle){
// 			buffer->empty();
// 			buffer->setIDCount(1);
// 			res[x.tripleNodeID]->print2ID(buffer);
// 			// EntityIDList[x.tripleNodeID] = buf;
// 		}
// 		cout<<"tripleNodeID "<<x.tripleNodeID<<" SIZE: "<<res[x.tripleNodeID]->getSize()<<endl;
// 		i++;
// 	}


// 	isSingle = false;
// 	// size_t i;
// 	size_t size = buffer->getSize();

// 	std::string URI;
// 	ID* p = buffer->getBuffer();

// 	int ans = 0;
// 	for (i = 0; i < size; ++i) {
// 			if (uriTable->getURIById(URI, p[i]) == OK) {
// 				cout<<"p[i] "<<p[i]<<endl;
// 				ans++;
// #ifdef PRINT_RESULT
// 				cout << URI << endl;
// #else
// 				resultPtr->push_back("NULL");
// #endif
// 			} else {
// #ifdef PRINT_RESULT
// 				cout << p[i] << " " << "not found" << endl;
// #else
// 				resultPtr->push_back("NULL");
// #endif
// 			}
// 	}
// 	cout << "singleJoin ans " << ans << endl;
// 	// Timestamp t2;
// 	// cout << "getResult time:" << (static_cast<double>(t2-t1)/1000.0) << endl;
// 	return OK;
// }


Status TripleBitWorkerQuery::singleVariableJoin() {
	cout << "singleVariableJoin" << endl;
    cout <<"acyclic join"<<endl;
	cout <<"variable count  " <<  this->_queryGraph->variableCount << endl;

	/*set BM*/
	fountain.clear();
	// fountain.resize(_query->joinVariables.size() + 1);
	fountain.resize(this->_queryGraph->variableCount);
	cout << fountain.size() << endl;
	// TripleBitQueryGraph::JoinVariableNode* node = NULL;
	// getVariableNodeByID(node, 0);

    //从一开始，不一定都有
	for(int i = 1;i < fountain.size(); i++){
		TripleBitQueryGraph::JoinVariableNode* node = NULL;
		getVariableNodeByID(node, i);
		if(!node){
			CBitMap * temp = new CBitMap(1,max_id);
			fountain[i].push_back(temp);
		}else{
			// cout << node->appear_tpnodes.size() << endl;
			for(int j = 0;j < node->appear_tpnodes.size();j++){
				CBitMap * temp = new CBitMap(1,max_id);
				fountain[i].push_back(temp);
			}
		}
	}



	cout << "fountain" << endl;
	for(int i=1;i<=fountain.size();i++){
		cout << "variable i  : "<< i << " total height " <<fountain[i].size() << endl;
	}

	for(int i = 1; i <= _query->joinVariables.size();i++){
		var_min_max[i] = make_pair(0,UINT_MAX);
	}

	timeval sj_begin,sj_end;
	



	gettimeofday(&sj_begin,NULL);
	ID minID = 0;
	ID maxID = UINT_MAX;


	EntityIDBuffer* buffer = NULL;
	TripleNode* triple = NULL;
	

	map<TripleNodeID,ResultIDBuffer *> res;





	// for(auto & x : _query->tripleNodes){
	for(int i = 0;i < _query->tripleNodes.size(); i++){
		TripleNode& x = _query->tripleNodes[i]; 
		// cout<<"varHeight "<<x.varHeight.size()<<endl;
		firstVar = x.varHeight[0].first;
		firstHeight = x.varHeight[0].second;
		if(x.varHeight.size() > 1){
			secVar = x.varHeight[1].first;
			secHeight = x.varHeight[1].second;
		}


		// if(x.tripleNodeID == 3){
		// 	res[0]->printMinMax();
		// 	res[1]->printMinMax();
		// 	res[2]->printMinMax();

		// 	minID = 117;
		// 	maxID = 118;
		// 	x.scanOperation = TripleNode::FINDOSBYP;

		// }
	
		cout << "firstVar " << firstVar << " " << var_min_max[firstVar].first << " " << var_min_max[firstVar].second;
	    
		res[x.tripleNodeID] = findEntityIDByTriple(&x, var_min_max[firstVar].first, var_min_max[firstVar].second);
		// res[x.tripleNodeID] = findEntityIDByTriple(&x, minID, maxID);
		
        


		ID a_min = UINT_MAX;
		ID a_max = 1;
		ID b_min = UINT_MAX;
		ID b_max = 1;
		res[x.tripleNodeID]->printMinMax(a_min,a_max,b_min,b_max);
		cout << "new a b" << endl;
		cout << "a_min " << a_min << " a_max " << a_max << " b_min " << b_min << " b_max " << b_max << endl;

		if(!( a_min == UINT_MAX && a_max == 1 && b_min == UINT_MAX && b_max == 1)){
            var_min_max[firstVar].first = max(var_min_max[firstVar].first,a_min);
			var_min_max[firstVar].second = min(var_min_max[firstVar].second,a_max);

			if(x.varHeight.size() > 1){
				var_min_max[secVar].first = max(var_min_max[secVar].first,b_min);
				var_min_max[secVar].second = min(var_min_max[secVar].second,b_max);
				// secVar = x.varHeight[1].first;
				// secHeight = x.varHeight[1].second;
			}
		}
		

		if(x.tripleNodeID == 1){
			cout << "Triple 1" << endl;
			// res[x.tripleNodeID]->print();
		}

		for(int i = 1; i <= _query->joinVariables.size();i++){
			cout << "var " << i << " " << var_min_max[i].first << " " << var_min_max[i].second << endl;
			// var_min_max[i] = make_pair(0,UINT_MAX);
		}

		cout<<"tripleNodeID "<<x.tripleNodeID<<" SIZE: "<<res[x.tripleNodeID]->getSize()<<endl;
		cout << "i " << i << " _query->tripleNodes.size() " <<  _query->tripleNodes.size() << endl;
	}


	/*  reduce */
	cout << "go to reduce part" << endl;
    for(int i = 0;i < _query->tripleNodes.size(); i++){
		TripleNode& x = _query->tripleNodes[i];
		if(res[x.tripleNodeID]->getSize()>0){
			firstVar = x.varHeight[0].first;
			firstHeight = x.varHeight[0].second;
			secVar = x.varHeight[1].first;
			secHeight = x.varHeight[1].second;

			if(firstHeight < fountain[firstVar].size() - 1 || secHeight < fountain[secVar].size() - 1){
				cout<<"triple "<<x.tripleNodeID<<" "<<"before reduce "<<res[x.tripleNodeID]->getSize()<<endl;
				reduce_p(res[x.tripleNodeID]);
				cout<<"after reduce "<<res[x.tripleNodeID]->getSize()<<endl;
			}
		}
	} 

	gettimeofday(&sj_end,NULL);
	cout << "scan and join time  " << ((sj_end.tv_sec - sj_begin.tv_sec) * 1000000.0 + (sj_end.tv_usec - sj_begin.tv_usec)) << " us " << endl;
	return OK;



// 	vector<TripleBitQueryGraph::JoinVariableNodeID>::iterator joinNodeIter = _query->joinVariables.begin();
// 	vector<pair<TripleBitQueryGraph::TripleNodeID, TripleBitQueryGraph::JoinVariableNode::DimType> >::iterator nodePatternIter;

// 	TripleBitQueryGraph::JoinVariableNode* node = NULL;
// 	EntityIDBuffer* buffer = NULL;
// 	TripleNode* triple = NULL;

// 	//TODO Initialize the first query pattern's triple of the pattern group which has the same variable
// 	getVariableNodeByID(node, *joinNodeIter);
// 	nodePatternIter = node->appear_tpnodes.begin();

// 	getTripleNodeByID(triple, nodePatternIter->first);
// 	buffer = findEntityIDByTriple(triple, 0, INT_MAX)->getEntityIDBuffer();
// 	if (buffer->getSize() == 0) {
// #ifdef PRINT_RESULT
// 		cout << "empty result" << endl;
// #else
// 		resultPtr->push_back("-1");
// 		resultPtr->push_back("Empty Result");
// #endif
// 		return OK;
// 	}
// 	EntityIDList[nodePatternIter->first] = buffer;
// 	nodePatternIter++;

// 	ResultIDBuffer* tempBuffer;
// 	ID minID, maxID;

// 	if (_queryGraph->getProjection().size() == 1) {
// 		for (; nodePatternIter != node->appear_tpnodes.end(); ++nodePatternIter) {
// 			buffer->getMinMax(minID, maxID);
// 			getTripleNodeByID(triple, nodePatternIter->first);
// 			tempBuffer = findEntityIDByTriple(triple, minID, maxID);
// 			mergeJoin.Join(buffer, tempBuffer, 1, 1, false);
// 			if (buffer->getSize() == 0) {
// #ifdef PRINT_RESULT
// 				cout << "empty result" << endl;
// #else
// 				resultPtr->push_back("-1");
// 				resultPtr->push_back("Empty Result");
// #endif
// 				return OK;
// 			}
// 		}
// 	} else {
// 		for (; nodePatternIter != node->appear_tpnodes.end(); nodePatternIter++) {
// 			buffer->getMinMax(minID, maxID);
// 			getTripleNodeByID(triple, nodePatternIter->first);
// 			tempBuffer = findEntityIDByTriple(triple, minID, maxID);
// 			mergeJoin.Join(buffer, tempBuffer, 1, 1, true);
// 			if (buffer->getSize() == 0) {
// #ifdef PRINT_RESULT
// 				cout << "empty result" << endl;
// #else
// 				resultPtr->push_back("-1");
// 				resultPtr->push_back("Empty Result");
// #endif
// 				return OK;
// 			}
// 			EntityIDList[nodePatternIter->first] = tempBuffer->getEntityIDBuffer();
// 		}
// 	}

// 	//TODO materialization the result;
// 	size_t i;
// 	size_t size = buffer->getSize();

// 	std::string URI;
// 	ID* p = buffer->getBuffer();

// 	size_t projectNo = _queryGraph->getProjection().size();
// #ifndef PRINT_RESULT
// 	char temp[2];
// 	sprintf(temp, "%d", projectNo);
// 	resultPtr->push_back(temp);
// #endif

// 	if (projectNo == 1) {
// 		for (i = 0; i < size; ++i) {
// 			if (uriTable->getURIById(URI, p[i]) == OK) {
// #ifdef PRINT_RESULT
// 				cout << URI << endl;
// #else
// 				resultPtr->push_back("NULL");
// #endif
// 			} else {
// #ifdef PRINT_RESULT
// 				cout << p[i] << " " << "not found" << endl;
// #else
// 				resultPtr->push_back("NULL");
// #endif
// 			}
// 		}
// 	} else {
// 		std::vector<EntityIDBuffer*> bufferlist;
// 		std::vector<ID> resultVar;
// 		resultVar.resize(0);
// 		uint projBitV, nodeBitV, resultBitV, tempBitV;
// 		resultBitV = 0;
// 		ID sortID;
// 		int keyp;
// 		keyPos.clear();

// 		generateProjectionBitVector(projBitV, _queryGraph->getProjection());
// 		for (i = 0; i != _query->tripleNodes.size(); i++) {
// 			//generate the bit vector of query pattern
// 			generateTripleNodeBitVector(nodeBitV, _query->tripleNodes[i]);
// 			//get the common bit
// 			tempBitV = projBitV & nodeBitV;
// 			if (tempBitV == nodeBitV) {
// 				//the query pattern which contains two or more variables is better
// 				if (EntityIDList.count(_query->tripleNodes[i].tripleNodeID) == 0 || (EntityIDList[_query->tripleNodes[i].tripleNodeID]->getIDCount() == 1))
// 					continue;
// 				bufferlist.push_back(EntityIDList[_query->tripleNodes[i].tripleNodeID]);
// 				if (countOneBits(resultBitV) == 0) {
// 					//the first time, last joinKey should be set as UNIT_MAX
// 					keyp = insertVarID(UINT_MAX, resultVar, _query->tripleNodes[i], sortID);
// 				} else {
// 					keyp = insertVarID(*joinNodeIter, resultVar, _query->tripleNodes[i], sortID);
// 					keyPos.push_back(keyp);
// 				}
// 				resultBitV |= nodeBitV;
// 				//the buffer of query pattern is enough
// 				if (countOneBits(resultBitV) == projectNo)
// 					break;
// 			}
// 		}

// 		if (projectNo == 2) {
// 			generateResultPos(resultVar, _queryGraph->getProjection(), resultPos);

// 			EntityIDBuffer* buf = bufferlist[0];
// 			size_t bufsize = buf->getSize();
// 			ID* ids = buf->getBuffer();
// 			int IDCount = buf->getIDCount();
// 			for (i = 0; i < bufsize; i++) {
// 				for (int j = 0; j < IDCount; j++) {
// 					if (uriTable->getURIById(URI, ids[i * IDCount + resultPos[j]]) == OK) {
// #ifdef PRINT_RESULT
// 						cout << URI << " ";
// #else
// 						resultPtr->push_back(URI);
// #endif
// 					} else
// 						cout << "not found" << endl;
// 				}
// #ifdef PRINT_RESULT
// 				cout << endl;
// #endif
// 			}
// 		} else {
// 			generateResultPos(resultVar, _queryGraph->getProjection(), resultPos);
// 			needselect = false;

// 			EntityIDBuffer* buf = bufferlist[0];
// 			size_t bufsize = buf->getSize();
// 			bufPreIndexs.clear();
// 			bufPreIndexs.resize(bufferlist.size(), 0);
// 			ID key;
// 			int IDCount = buf->getIDCount();
// 			ID* ids = buf->getBuffer();
// 			int sortKey = buf->getSortKey();
// 			for (i = 0; i != bufsize; i++) {
// 				resultVec.resize(0);
// 				key = ids[i * IDCount + sortKey];
// 				for (int j = 0; j < IDCount; j++) {
// 					resultVec.push_back(ids[i * IDCount + j]);
// 				}

// 				bool ret = getResult(key, bufferlist, 1);
// 				if (ret == false) {
// 					while (i < bufsize && ids[i * IDCount + sortKey] == key) {
// 						i++;
// 					}
// 					i--;
// 				}
// 			}
// 		}
// 	}
// 	return OK;
}




ResultIDBuffer* TripleBitWorkerQuery::findEntityIDBySubTrans(SubTrans *subtrans,TripleNode* triple, ID minID, ID maxID) {
// #ifdef PATTERN_TIME
// 	struct timeval start, end;
// 	gettimeofday(&start, NULL);
// #endif

// 	// TripleBitQueryGraph::OpType operationType = TripleBitQueryGraph::QUERY;
// 	// shared_ptr<IndexForTT> indexForTT(new IndexForTT);
// 	// SubTrans *subTrans = new SubTrans(*transactionTime, workerID, minID, maxID, operationType, 1, *triple, indexForTT);
// 	tasksEnQueue(triple->predicate, subTrans);
// 	ResultIDBuffer *retBuffer = resultBuffer[triple->predicate]->DeQueue();

// #ifdef PATTERN_TIME
// 	gettimeofday(&end, NULL);
	
// 	cout<<endl<< "#######find pattern " << triple->tripleNodeID << " time elapsed: " << ((end.tv_sec - start.tv_sec) * 1000000 + end.tv_usec - start.tv_usec) / 1000000.0
// 			<< endl;
// 	retBuffer->getSize();
// #endif

	// return retBuffer;
	return NULL;
}

ResultIDBuffer* TripleBitWorkerQuery::findEntityIDByTriple(TripleNode* triple, ID minID, ID maxID) {
#ifdef PATTERN_TIME
	struct timeval start, end;
	gettimeofday(&start, NULL);
#endif
	TripleBitQueryGraph::OpType operationType = TripleBitQueryGraph::QUERY;
	shared_ptr<IndexForTT> indexForTT(new IndexForTT);
	SubTrans *subTrans = new SubTrans(*transactionTime, workerID, minID, maxID, operationType, 1, *triple, indexForTT);

	//2018年9月21日22:18:17
	tasksEnQueue(triple->predicate, subTrans);
	ResultIDBuffer *retBuffer = resultBuffer[triple->predicate]->DeQueue();

#ifdef PATTERN_TIME
	gettimeofday(&end, NULL);
	
	cout<<endl<< "#######find pattern " << triple->tripleNodeID << " time elapsed: " << ((end.tv_sec - start.tv_sec) * 1000000 + end.tv_usec - start.tv_usec) / 1000000.0
			<< endl;
	// retBuffer->getSize();
	
#endif

	return retBuffer;
}

// ResultIDBuffer* TripleBitWorkerQuery::findEntityIDByTriple_sync(TripleNode* triple, ID minID, ID maxID) {
// #ifdef PATTERN_TIME
// 	struct timeval start, end;
// 	gettimeofday(&start, NULL);
// #endif
// 	TripleBitQueryGraph::OpType operationType = TripleBitQueryGraph::QUERY;
// 	shared_ptr<IndexForTT> indexForTT(new IndexForTT);
// 	SubTrans *subTrans = new SubTrans(*transactionTime, workerID, minID, maxID, operationType, 1, *triple, indexForTT);
	

// 	tripleBitRepo->getPartitionMaster(partitionID)->Work_sync(subTrans);

	
// 	ResultIDBuffer *retBuffer = resultBuffer[triple->predicate]->DeQueue();

// #ifdef PATTERN_TIME
// 	gettimeofday(&end, NULL);
	
// 	cout<<endl<< "#######find pattern " << triple->tripleNodeID << " time elapsed: " << ((end.tv_sec - start.tv_sec) * 1000000 + end.tv_usec - start.tv_usec) / 1000000.0
// 			<< endl;
// #endif

// 	return retBuffer;
// }



Status TripleBitWorkerQuery::getTripleNodeByID(TripleNode*& triple, TripleBitQueryGraph::TripleNodeID nodeID) {
	vector<TripleNode>::iterator iter = _query->tripleNodes.begin();
	// cout<<"before "<<triple<<endl;

	for (; iter != _query->tripleNodes.end(); iter++) {
		if (iter->tripleNodeID == nodeID) {
			triple = &(*iter);
			// cout<<"after "<<triple<<endl;
			
			return OK;
		}
	}
	return NOT_FOUND;
}

int TripleBitWorkerQuery::getVariablePos(TripleBitQueryGraph::JoinVariableNodeID id, TripleBitQueryGraph::TripleNodeID tripleID) {
	TripleNode* triple = NULL;
	getTripleNodeByID(triple, tripleID);
	return getVariablePos(id, triple);
}

int TripleBitWorkerQuery::getVariablePos(TripleBitQueryGraph::JoinVariableNodeID id, TripleNode* triple) {
	int pos = 0;

	switch (triple->scanOperation) {
	case TripleNode::FINDO:
	case TripleNode::FINDOBYP:
	case TripleNode::FINDOBYS:
	case TripleNode::FINDOBYSP:
		pos = 1;
		break;
	case TripleNode::FINDOPBYS:
		if (id == triple->object)
			pos = 1;
		else
			pos = 2;
		break;
	case TripleNode::FINDOSBYP:
		if (id == triple->object)
			pos = 1;
		else
			pos = 2;
		break;
	case TripleNode::FINDP:
	case TripleNode::FINDPBYO:
	case TripleNode::FINDPBYS:
	case TripleNode::FINDPBYSO:
		pos = 1;
		break;
	case TripleNode::FINDPOBYS:
		if (id == triple->predicate)
			pos = 1;
		else
			pos = 2;
		break;
	case TripleNode::FINDPSBYO:
		if (id == triple->predicate)
			pos = 1;
		else
			pos = 2;
		break;
	case TripleNode::FINDS:
	case TripleNode::FINDSBYO:
	case TripleNode::FINDSBYP:
	case TripleNode::FINDSBYPO:
		pos = 1;
		break;
	case TripleNode::FINDSOBYP:
		if (id == triple->subject)
			pos = 1;
		else
			pos = 2;
		break;
	case TripleNode::FINDSPBYO:
		if (id == triple->subject)
			pos = 1;
		else
			pos = 2;
		break;
	case TripleNode::NOOP:
		pos = -1;
		break;
	}
	return pos;
}

Status TripleBitWorkerQuery::getVariableNodeByID(TripleBitQueryGraph::JoinVariableNode*& node, TripleBitQueryGraph::JoinVariableNodeID id) {
	int size = _query->joinVariableNodes.size();

	for (int i = 0; i < size; i++) {
		if (_query->joinVariableNodes[i].value == id) {
			node = &(_query->joinVariableNodes[i]);
			break;
		}
	}

	return OK;
}

bool TripleBitWorkerQuery::nodeIsLeaf(TripleBitQueryGraph::JoinVariableNodeID varID) {
	vector<TripleBitQueryGraph::JoinVariableNodeID>::iterator iter;
	iter = find(_query->leafNodes.begin(), _query->leafNodes.end(), varID);
	if (iter != _query->leafNodes.end())
		return true;
	else
		return false;
}

int TripleBitWorkerQuery::getVariableCount(TripleNode* node) {
	if (node == NULL) {
		cerr << "Error TripleBitWorkerQuery::getVariableCount" << endl;
		return -1;
	}
	switch (node->scanOperation) {
	case TripleNode::FINDO:
		return 1;
	case TripleNode::FINDOBYP:
		return 1;
	case TripleNode::FINDOBYS:
		return 1;
	case TripleNode::FINDOBYSP:
		return 1;
	case TripleNode::FINDP:
		return 1;
	case TripleNode::FINDPBYO:
		return 1;
	case TripleNode::FINDPBYS:
		return 1;
	case TripleNode::FINDPBYSO:
		return 1;
	case TripleNode::FINDS:
		return 1;
	case TripleNode::FINDSBYO:
		return 1;
	case TripleNode::FINDSBYP:
		return 1;
	case TripleNode::FINDSBYPO:
		return 1;
	case TripleNode::FINDOSBYP:
		return 2;
	case TripleNode::FINDOPBYS:
		return 2;
	case TripleNode::FINDPSBYO:
		return 2;
	case TripleNode::FINDPOBYS:
		return 2;
	case TripleNode::FINDSOBYP:
		return 2;
	case TripleNode::FINDSPBYO:
		return 2;
	case TripleNode::NOOP:
		return -1;
	}

	return -1;
}

int TripleBitWorkerQuery::getVariableCount(TripleBitQueryGraph::TripleNodeID id) {
	TripleNode* triple = NULL;
	getTripleNodeByID(triple, id);
	return getVariableCount(triple);
}

bool TripleBitWorkerQuery::getResult(ID key, std::vector<EntityIDBuffer*>& bufferlist, size_t buflist_index) {
	if (buflist_index == bufferlist.size())
		return true;

	EntityIDBuffer* entBuf = bufferlist[buflist_index];
	size_t currentIndex = bufPreIndexs[buflist_index];
	size_t bufsize = entBuf->getSize();
	while (currentIndex < bufsize && (*entBuf)[currentIndex] < key) {
		currentIndex++;
	}
	bufPreIndexs[buflist_index] = currentIndex;
	if (currentIndex >= bufsize || (*entBuf)[currentIndex] > key)
		return false;

	bool ret;
	ID* buf = entBuf->getBuffer();
	int IDCount = entBuf->getIDCount();
	int sortKey = entBuf->getSortKey();

	ret = true;
	size_t resultVecSize = resultVec.size();
	std::string URI;
	while (currentIndex < bufsize && buf[currentIndex * IDCount + sortKey] == key) {
		for (int i = 0; i < IDCount; ++i) {
			if (i != sortKey) {
				resultVec.push_back(buf[currentIndex * IDCount + i]);
			}
		}
		if (buflist_index == (bufferlist.size() - 1)) {
			if (needselect == true) {
				if (resultVec[verifyPos[0]] != resultVec[verifyPos[1]]) {
					++currentIndex;
					continue;
				}
			}
			for (size_t j = 0; j != resultPos.size(); j++) {
				uriTable->getURIById(URI, resultVec[resultPos[j]]);
#ifdef PRINT_RESULT
				std::cout << URI << " ";
#else
				resultPtr->push_back(URI);
#endif
			}
#ifdef PRINT_RESULT
			std::cout << std::endl;
#endif
		} else {
			ret = getResult(resultVec[keyPos[buflist_index]], bufferlist, buflist_index + 1);
			if (ret != true)
				break;
		}

		++currentIndex;
		resultVec.resize(resultVecSize);
	}

	return ret;
}

void TripleBitWorkerQuery::getResult_join(ID key, std::vector<EntityIDBuffer*>& bufferlist, size_t buflist_index) {
	cout << "key " << key << " bufferlist  " << buflist_index << endl;
	gR++;

	if (buflist_index == bufferlist.size())
		return;

	EntityIDBuffer* entBuf = bufferlist[buflist_index];
	size_t currentIndex = entBuf->getEntityIDPos(key); //bufPreIndexs[buflist_index]

	cout << "got what ?? " << currentIndex << endl;
	cout << "got what key " << key << endl;
	if (currentIndex == size_t(-1))
		return;
	size_t bufsize = entBuf->getSize();

	ID* buf = entBuf->getBuffer();
	int IDCount = entBuf->getIDCount();
	int sortKey = entBuf->getSortKey();



	size_t resultVecSize = resultVec.size();
	std::string URI;


    

	while (currentIndex < bufsize && buf[currentIndex * IDCount + sortKey] == key) {
		cout << "for debug " << endl;
		cout << "currentIndex " << currentIndex << endl;
		cout << "bufsize " << bufsize << endl;
		
		for (int i = 0; i < IDCount; ++i) {
			if (i != sortKey){
				resultVec.push_back(buf[currentIndex * IDCount + i]);
			}
		}
		if (buflist_index == (bufferlist.size() - 1)) {
			if (needselect == true) {
				if (resultVec[verifyPos[0]] != resultVec[verifyPos[1]]) {
					// resultVec.resize(resultVecSize);
					resultVec.pop_back();
					++currentIndex;
					continue;
				}
			}
			for (size_t j = 0; j != resultPos.size(); ++j) {
				uriTable->getURIById(URI, resultVec[resultPos[j]]);
#ifdef PRINT_RESULT
				std::cout << URI << " ";
#else
				resultPtr->push_back(URI);
#endif
			}
#ifdef PRINT_BUFFERSIZE
			ans++;
#endif
#ifdef PRINT_RESULT
			std::cout << std::endl;
#endif
		} else {
			/* on this*/
			getResult_join(resultVec[keyPos[buflist_index]], bufferlist, buflist_index + 1);
		}

		++currentIndex;
		resultVec.resize(resultVecSize);
	}


// 	while (currentIndex < bufsize && buf[currentIndex * IDCount + sortKey] == key) {
// 		if(resultVec.size() >= 3 && resultVec[0] == 72828
// 			&& resultVec[1] == 2381
// 			&& resultVec[2] == 71503){
// 			if(buf[currentIndex * IDCount + 0] == 72828){
				
// 				cout<<"got You!!!"<<endl;
// 				cout<<resultVec[0]<<endl;
// 				cout<<resultVec[1]<<endl;
// 				cout<<resultVec[2]<<endl;
// 				cout<<buf[currentIndex * IDCount + 0]<<endl;
// 				cout<<buf[currentIndex * IDCount + 1]<<endl;
// 				cout<<buflist_index<<endl;
// 				cout<<(bufferlist.size() - 1)<<endl;
// 				cout<<"---------------"<<endl;
				
				
				
// 			}
// 			cout<<buf[currentIndex * IDCount + 0]<<" "<<buf[currentIndex * IDCount + 1]<<endl;
// 		}
		
// 		for (int i = 0; i < IDCount; ++i) {
// 			if (i != sortKey){
// 				resultVec.push_back(buf[currentIndex * IDCount + i]);
// 				if(resultVec.size() >= 3 && resultVec[0] == 72828
// 				&& resultVec[1] == 2381
// 				&& resultVec[2] == 71503){
// 					cout<<"size    "<<resultVec.size()<<endl;
// 					cout<<"append--->  "<<buf[currentIndex * IDCount + i]<<endl;
// 				}
// 			}
				
// 		}
// 		if (buflist_index == (bufferlist.size() - 1)) {
// 			if (needselect == true) {
// 				if(resultVec.size() >= 3 && resultVec[0] == 72828
// 				&& resultVec[1] == 2381
// 				&& resultVec[2] == 71503){
// 					cout<<"--->  "<<resultVec[3]<<endl;
// 					cout<<"size    "<<resultVec.size()<<endl;
// 				}



// 				if (resultVec[verifyPos[0]] != resultVec[verifyPos[1]]) {
// 					// resultVec.resize(resultVecSize);
// 					++currentIndex;
// 					continue;
// 				}
// 			}
// 			for (size_t j = 0; j != resultPos.size(); ++j) {
// 				uriTable->getURIById(URI, resultVec[resultPos[j]]);
// #ifdef PRINT_RESULT
// 				std::cout << URI << " ";
// #else
// 				resultPtr->push_back(URI);
// #endif
// 			}
// #ifdef PRINT_BUFFERSIZE
// 			ans++;
// 			// for(int kk = 0;kk<resultVec.size();kk++){
// 			// 	cout<<resultVec[kk]<<" ";
// 			// }
// 			// cout<<endl;
// #endif
// #ifdef PRINT_RESULT
// 			std::cout << std::endl;
// #endif
// 		} else {
// 			/* on this*/
// 			getResult_join(resultVec[keyPos[buflist_index]], bufferlist, buflist_index + 1);
// 		}

// 		++currentIndex;
// 		resultVec.resize(resultVecSize);
// 	}
}

Status TripleBitWorkerQuery::findEntitiesAndJoin(TripleBitQueryGraph::JoinVariableNodeID id,
		vector<pair<TripleBitQueryGraph::TripleNodeID, TripleBitQueryGraph::JoinVariableNode::DimType> >& tpnodes, bool firstTime) {
	size_t minSize; // minimum record size;
	ID minSizeID; //the bufferer No. which contains the minimum records;
	size_t size;
	EntityIDBuffer* buffer;
	map<ID, bool> firstInsertFlag; // flag the first inserted pattern buffer;
	Status s;

	minSize = INT_MAX;
	minSizeID = 0;

	size = EntityIDList.size();

	//TODO get the buffer id which contains minimum candidate result
	EntityIDListIterType iter;
	size_t i;

	if (firstTime == true) {
		for (i = 0; i < tpnodes.size(); ++i) {
			firstInsertFlag[tpnodes[i].first] = false;
			iter = EntityIDList.find(tpnodes[i].first);
			if (iter != EntityIDList.end()) {
				if ((size = iter->second->getSize()) < minSize) {
					minSize = size;
					minSizeID = tpnodes[i].first;
				}
			} else if (getVariableCount(tpnodes[i].first) >= 2) {
				//TODO if the number of variable in a pattern is greater than 1, then allocate a buffer for the pattern
				buffer = NULL;
				EntityIDList[tpnodes[i].first] = buffer;
				firstInsertFlag[tpnodes[i].first] = true;
			}
		}

	} else {
		for (i = 0; i < tpnodes.size(); ++i) {
			firstInsertFlag[tpnodes[i].first] = false;
			if (getVariableCount(tpnodes[i].first) >= 2) {
				if (EntityIDList[tpnodes[i].first]->getSize() < minSize) {
					minSizeID = tpnodes[i].first;
					minSize = EntityIDList[minSizeID]->getSize();
				}
			}
		}
	}

	//if the most selective pattern has not been initialized , then initialize it
	iter = EntityIDList.find(minSizeID);
	if (iter == EntityIDList.end()) {
		cout << "unbelieveable" << endl;
		TripleNode* triple = NULL;
		getTripleNodeByID(triple, minSizeID);
		EntityIDList[minSizeID] = findEntityIDByTriple(triple, 0, INT_MAX)->getEntityIDBuffer();
	}

	if (firstTime == true)
		s = findEntitiesAndJoinFirstTime(tpnodes, minSizeID, firstInsertFlag, id);
	else
		s = modifyEntitiesAndJoin(tpnodes, minSizeID, id);

	return s;
}

Status TripleBitWorkerQuery::findEntitiesAndJoinFirstTime(
		vector<pair<TripleBitQueryGraph::TripleNodeID, TripleBitQueryGraph::JoinVariableNode::DimType> >& tpnodes, ID tripleID, map<ID, bool>& firstInsertFlag,
		TripleBitQueryGraph::JoinVariableNodeID id) {
	//use merge join operator to do the joins
	EntityIDBuffer* buffer = EntityIDList[tripleID];

	EntityIDBuffer* tempEntity = NULL;
	ResultIDBuffer* tempResult = NULL;
	TripleNode* triple = NULL;
	int joinKey = 0, joinKey2 = 0;
	bool insertFlag;
	size_t i;
	ID tripleNo;
	int varCount;

	ID maxID, minID;

	if (tpnodes.size() == 1) {
		return OK;
	}

	joinKey = this->getVariablePos(id, tripleID);
	Timestamp tcaseSort;
	buffer->sort(joinKey);//可能很耗时
	Timestamp tcaseSort0;
	cout << ">>>>>>>>>>>sort time:" << (static_cast<double>(tcaseSort0-tcaseSort)/1000.0) << endl;

	for (i = 0; i < tpnodes.size(); ++i) {
		tripleNo = tpnodes[i].first;

		if (tripleNo != tripleID) {
			joinKey2 = this->getVariablePos(id, tripleNo);
			insertFlag = firstInsertFlag[tripleNo];
			getTripleNodeByID(triple, tripleNo);
			varCount = this->getVariableCount(triple);

			if (insertFlag == false && varCount == 1) {
				buffer->getMinMax(minID, maxID);
#ifdef PRINT_BUFFERSIZE
				cout <<endl<<endl<< "case 1" << endl;
				cout<<"var: "<<id<<" @minID "<<minID<<" @maxID "<<maxID<<endl;
#endif
				tempResult = findEntityIDByTriple(triple, minID, maxID);
#ifdef PRINT_BUFFERSIZE

				cout << "***merge join : "<<tripleID<<" and "<<tripleNo<<" "<<"type @ "<<false<<endl;
				cout << "--------before join-----------"<<endl;
				Timestamp tcase10;
				cout << "pattern " << tripleID << " buffer size(before join): " << buffer->getSize() << endl;
				cout << "pattern " << tripleNo << " buffer size(before join): " << tempResult->getSize() << endl;
#endif
				mergeJoin.Join(buffer, tempResult, joinKey, 1, false);
#ifdef PRINT_BUFFERSIZE
				cout << "--------after join-----------"<<endl;
				Timestamp tcase11;
				cout << ">>>>>>>>>>>merge Join time:" << (static_cast<double>(tcase11-tcase10)/1000.0) << endl;
				cout << "pattern " << tripleID << " buffer size: " << buffer->getSize() << endl;
				cout << "pattern " << tripleNo << " buffer size(before join): " << tempResult->getSize() << endl;				
#endif
				BufferManager::getInstance()->freeBuffer(tempResult->getEntityIDBuffer());
				delete tempResult;
			} else if (insertFlag == false && varCount == 2) {
				tempEntity = EntityIDList[tripleNo];
			#ifdef PRINT_BUFFERSIZE
				cout << "case 2" << endl;
				cout << "***merge join : "<<tripleID<<" and "<<tripleNo<<" "<<"type @ "<<true<<endl;
				cout << "---------before join----------"<<endl;
				cout << "sig-> pattern : "<<tripleID<<endl;
				buffer->sig();
				cout << "sig-> pattern : "<<tripleNo<<endl;
				tempEntity->sig();
				cout<<endl<<endl;
				Timestamp tcase20;
			#endif
				
				mergeJoin.Join(buffer, tempEntity, joinKey, joinKey2, true);
#ifdef PRINT_BUFFERSIZE
				cout << "---------after join----------"<<endl;
				Timestamp tcase21;
				cout << ">>>>>>>>>merge Join time:" << (static_cast<double>(tcase21-tcase20)/1000.0) << endl;
				cout << "pattern " << tripleID << " buffer size: " << buffer->getSize() << endl;
				cout << "pattern " << tripleNo << " buffer size: " << tempEntity->getSize() << endl;
				buffer->sig();
				tempEntity->sig();
#endif
			} else {
				buffer->getMinMax(minID, maxID);
#ifdef PRINT_BUFFERSIZE
				cout <<endl<<endl<< "case 3" << endl;
				cout<<"var: "<<id<<" @minID "<<minID<<" @maxID "<<maxID<<endl;
#endif
				// if(tripleNo == 3){
				// 	cout<<"###xiu gai 3"<<endl;
				// 	minID = 417;
				// 	maxID = 164416021;
				// }

				tempResult = findEntityIDByTriple(triple, minID, maxID);
#ifdef PRINT_BUFFERSIZE
				cout << "***merge join : "<<tripleID<<" and "<<tripleNo<<" "<<"type @ "<<true<<endl;
				cout<<"------------before join--------"<<endl;
				cout << "pattern " << tripleID << " buffer size: " << buffer->getSize() << endl;
				cout << "pattern " << tripleNo << " buffer size: " << tempResult->getSize() << endl;
				
				cout << "sig-> pattern : "<<tripleID<<endl;				
				buffer->sig();
				// tempResult->sig();
				Timestamp tcase30;
#endif
				// if(tripleID == 5 && tripleID == 3){
				// 	mergeJoin.Join(buffer,tempResult->getEntityIDBuffer,joinKey,joinKey2,true);
				// }
				mergeJoin.Join(buffer, tempResult, joinKey, joinKey2, true);
				cout << "------------after join----------"<<endl;
#ifdef PRINT_BUFFERSIZE
				Timestamp tcase31;
				cout << ">>>>>>>merge Join time:" << (static_cast<double>(tcase31-tcase30)/1000.0) << endl;
				cout << "pattern " << tripleID << " buffer size: " << buffer->getSize() << endl;
				cout << "pattern " << tripleNo << " buffer size: " << tempResult->getSize() << endl;
#endif
				EntityIDList[tripleNo] = tempResult->getEntityIDBuffer();
			#ifdef PRINT_BUFFERSIZE
				cout << "sig-> pattern : "<<tripleID<<endl;			
				buffer->sig();
				cout << "sig-> pattern : "<<tripleNo<<endl;				
				EntityIDList[tripleNo]->sig();
				cout<<endl<<endl;
			#endif
			}
		}

		if (buffer->getSize() == 0)
			return NULL_RESULT;
	}

	if (buffer->getSize() == 0) {
		return NULL_RESULT;
	}

	return OK;
}

Status TripleBitWorkerQuery::modifyEntitiesAndJoin(vector<pair<TripleBitQueryGraph::TripleNodeID, TripleBitQueryGraph::JoinVariableNode::DimType> >& tpnodes,
		ID tripleID, TripleBitQueryGraph::JoinVariableNodeID id) {
	//use hash join operator to do the joins
	EntityIDBuffer* buffer = EntityIDList[tripleID];
	EntityIDBuffer* temp = NULL;
	TripleNode* triple = NULL;
	int joinKey = 0, joinKey2 = 0;
	size_t i;
	ID tripleNo;
	int varCount;
	bool sizeChanged = false;

	joinKey = this->getVariablePos(id, tripleID);
	buffer->setSortKey(joinKey - 1);
	size_t size = buffer->getSize();

	for (i = 0; i < tpnodes.size(); ++i) {
		tripleNo = tpnodes[i].first;
		this->getTripleNodeByID(triple, tripleNo);
		if (tripleNo != tripleID) {
			varCount = getVariableCount(triple);
			if (varCount == 2) {
				joinKey2 = this->getVariablePos(id, tripleID);
				temp = EntityIDList[tripleNo];
#ifdef PRINT_BUFFERSIZE
				cout<<endl<<endl;
				cout << "------------------------------------" << endl;
				cout << "***hash join : "<<tripleID<<" and "<<tripleNo<<endl;
				cout << "--------before join--------"<<endl;
				cout << "pattern " << tripleID << " buffer size: " << buffer->getSize() << endl;
				cout << "pattern " << tripleNo << " buffer size: " << temp->getSize() << endl;
				
				cout << "sig-> pattern : "<<tripleID<<endl;							
				buffer->sig();
				cout << "sig-> pattern : "<<tripleNo<<endl;			
				temp->sig();
				
				cout<<endl<<endl;
				Timestamp tcaseH0;
#endif
				hashJoin.Join(buffer, temp, joinKey, joinKey2);
#ifdef PRINT_BUFFERSIZE
				cout << "-------after join--------"<<endl;
				Timestamp tcaseH1;
				cout << ">>>>>>>>>>>>>>>>>>>Hash Join time:" << (static_cast<double>(tcaseH1-tcaseH0)/1000.0) << endl;
				cout << "pattern " << tripleID << " buffer size: " << buffer->getSize() << endl;
				cout << "pattern " << tripleNo << " buffer size: " << temp->getSize() << endl;				
				cout << "sig-> pattern : "<<tripleID<<endl;							
				buffer->sig();
				cout << "sig-> pattern : "<<tripleNo<<endl;			
				temp->sig();
#endif
			}
		}
		if (buffer->getSize() == 0) {
			return NULL_RESULT;
		}
	}
	if (size != buffer->getSize() || sizeChanged == true)
		return BUFFER_MODIFIED;
	else
		return OK;
}


Status TripleBitWorkerQuery::acyclicJoin() {


//	cout << "scan and join time  " << ((sj_end.tv_sec - sj_begin.tv_sec) * 1000000.0 + (sj_end.tv_usec - sj_begin.tv_usec)) << " us " << endl;	

	cout <<"acyclic join"<<endl;
	cout <<"variable count  " <<  this->_queryGraph->variableCount << endl;

	/*set BM*/
	fountain.clear();
	// fountain.resize(_query->joinVariables.size() + 1);
	fountain.resize(this->_queryGraph->variableCount);
	cout << fountain.size() << endl;
	// TripleBitQueryGraph::JoinVariableNode* node = NULL;
	// getVariableNodeByID(node, 0);

    //从一开始，不一定都有
	for(int i = 1;i < fountain.size(); i++){
		TripleBitQueryGraph::JoinVariableNode* node = NULL;
		getVariableNodeByID(node, i);
		if(!node){
			CBitMap * temp = new CBitMap(1,max_id);
			fountain[i].push_back(temp);
		}else{
			// cout << node->appear_tpnodes.size() << endl;
			for(int j = 0;j < node->appear_tpnodes.size();j++){
				CBitMap * temp = new CBitMap(1,max_id);
				fountain[i].push_back(temp);
			}
		}
	}



	cout << "fountain" << endl;
	for(int i=1;i<=fountain.size();i++){
		cout << "variable i  : "<< i << " total height " <<fountain[i].size() << endl;
	}

	for(int i = 1; i <= _query->joinVariables.size();i++){
		var_min_max[i] = make_pair(0,UINT_MAX);
	}

	timeval sj_begin,sj_end;
	



	gettimeofday(&sj_begin,NULL);
	ID minID = 0;
	ID maxID = UINT_MAX;


	EntityIDBuffer* buffer = NULL;
	TripleNode* triple = NULL;
	

	map<TripleNodeID,ResultIDBuffer *> res;





	// for(auto & x : _query->tripleNodes){
	for(int i = 0;i < _query->tripleNodes.size(); i++){
		TripleNode& x = _query->tripleNodes[i]; 
		// cout<<"varHeight "<<x.varHeight.size()<<endl;
		firstVar = x.varHeight[0].first;
		firstHeight = x.varHeight[0].second;
		if(x.varHeight.size() > 1){
			secVar = x.varHeight[1].first;
			secHeight = x.varHeight[1].second;
		}


		// if(x.tripleNodeID == 3){
		// 	res[0]->printMinMax();
		// 	res[1]->printMinMax();
		// 	res[2]->printMinMax();

		// 	minID = 117;
		// 	maxID = 118;
		// 	x.scanOperation = TripleNode::FINDOSBYP;

		// }
	
		cout << "firstVar " << firstVar << " " << var_min_max[firstVar].first << " " << var_min_max[firstVar].second;
	    
		res[x.tripleNodeID] = findEntityIDByTriple(&x, var_min_max[firstVar].first, var_min_max[firstVar].second);
		// res[x.tripleNodeID] = findEntityIDByTriple(&x, minID, maxID);
		
        


		ID a_min = UINT_MAX;
		ID a_max = 1;
		ID b_min = UINT_MAX;
		ID b_max = 1;
		res[x.tripleNodeID]->printMinMax(a_min,a_max,b_min,b_max);
		cout << "new a b" << endl;
		cout << "a_min " << a_min << " a_max " << a_max << " b_min " << b_min << " b_max " << b_max << endl;

		if(!( a_min == UINT_MAX && a_max == 1 && b_min == UINT_MAX && b_max == 1)){
            var_min_max[firstVar].first = max(var_min_max[firstVar].first,a_min);
			var_min_max[firstVar].second = min(var_min_max[firstVar].second,a_max);

			if(x.varHeight.size() > 1){
				var_min_max[secVar].first = max(var_min_max[secVar].first,b_min);
				var_min_max[secVar].second = min(var_min_max[secVar].second,b_max);
				// secVar = x.varHeight[1].first;
				// secHeight = x.varHeight[1].second;
			}
		}
		

		if(x.tripleNodeID == 1){
			cout << "Triple 1" << endl;
			// res[x.tripleNodeID]->print();
		}

		for(int i = 1; i <= _query->joinVariables.size();i++){
			cout << "var " << i << " " << var_min_max[i].first << " " << var_min_max[i].second << endl;
			// var_min_max[i] = make_pair(0,UINT_MAX);
		}

		cout<<"tripleNodeID "<<x.tripleNodeID<<" SIZE: "<<res[x.tripleNodeID]->getSize()<<endl;
		cout << "i " << i << " _query->tripleNodes.size() " <<  _query->tripleNodes.size() << endl;
	}


	/*  reduce */
	cout << "go to reduce part" << endl;
    for(int i = 0;i < _query->tripleNodes.size(); i++){
		TripleNode& x = _query->tripleNodes[i];
		if(res[x.tripleNodeID]->getSize()>0){
			firstVar = x.varHeight[0].first;
			firstHeight = x.varHeight[0].second;
			secVar = x.varHeight[1].first;
			secHeight = x.varHeight[1].second;

			if(firstHeight < fountain[firstVar].size() - 1 || secHeight < fountain[secVar].size() - 1){
				cout<<"triple "<<x.tripleNodeID<<" "<<"before reduce "<<res[x.tripleNodeID]->getSize()<<endl;
				reduce_p(res[x.tripleNodeID]);
				cout<<"after reduce "<<res[x.tripleNodeID]->getSize()<<endl;
			}
		}
	} 

	gettimeofday(&sj_end,NULL);
	cout << "scan and join time  " << ((sj_end.tv_sec - sj_begin.tv_sec) * 1000000.0 + (sj_end.tv_usec - sj_begin.tv_usec)) << " us " << endl;
	return OK;


	// for(auto & x : _query->tripleNodes){
	// 	// cout<<"varHeight "<<x.varHeight.size()<<endl;
	// 	if(res[x.tripleNodeID]->getSize()>0){
	// 		firstVar = x.varHeight[0].first;
	// 		firstHeight = x.varHeight[0].second;
	// 		secVar = x.varHeight[1].first;
	// 		secHeight = x.varHeight[1].second;

	// 		if(firstHeight < 2 || secHeight < 2){
	// 			cout<<"triple "<<x.tripleNodeID<<" "<<"before reduce "<<res[x.tripleNodeID]->getSize()<<endl;
	// 			reduce_p(res[x.tripleNodeID]);
	// 			cout<<"after reduce "<<res[x.tripleNodeID]->getSize()<<endl;
	// 		}
	// 	}
	// }

	

	// reduce_p5(res[5]);
	// cout<<"after reduce p5 , got size"<<endl;
	// cout<<res[5]->getSize()<<endl;

	// reduce_p0(res[0]);
	// cout<<"after reduce p0 , got size"<<endl;
	// cout<<res[0]->getSize()<<endl;

// Timestamp mat0;
// 	// for(auto & x : _query->tripleNodes){
// 	for(int i = 0;i < _query->tripleNodes.size(); i++){
// 		TripleNode& x = _query->tripleNodes[i]; 
// 		// cout<<"varHeight "<<x.varHeight.size()<<endl;
// 		if(res[x.tripleNodeID]->getSize()>0){
// 			EntityIDBuffer *buf = new EntityIDBuffer;
// 			buf->empty();
// 			buf->setIDCount(2);
// 			res[x.tripleNodeID]->print2ID(buf);
// 			EntityIDList[x.tripleNodeID] = buf;
// 			// cout <<　"buf size " << bu
// 		}
// 	}

// 	//TODO materialize
// 	int i;
// 	size_t projectionSize = _queryGraph->getProjection().size();
// 	if (projectionSize == 1) {
// 		uint projBitV, nodeBitV, tempBitV;
// 		std::vector<ID> resultVar;
// 		resultVar.resize(0);
// 		ID sortID;

// 		generateProjectionBitVector(projBitV, _queryGraph->getProjection());
// 		TripleBitQueryGraph::TripleNodeID tid;
// 		size_t bufsize = UINT_MAX;
// 		for (int i = 0; i != _query->tripleNodes.size(); ++i) {
// 			if (EntityIDList.count(_query->tripleNodes[i].tripleNodeID) == 0)
// 				continue;
// 			generateTripleNodeBitVector(nodeBitV, _query->tripleNodes[i]);
// 			tempBitV = projBitV | nodeBitV;
// 			if (tempBitV == projBitV) {
// 				//TODO output the result
// 				if (EntityIDList[_query->tripleNodes[i].tripleNodeID]->getIDCount() == 1) {
// 					insertVarID(UINT_MAX, resultVar, _query->tripleNodes[i], sortID);
// 					tid = _query->tripleNodes[i].tripleNodeID;
// 					break;
// 				} else {
// 					if (EntityIDList[_query->tripleNodes[i].tripleNodeID]->getSize() < bufsize) {
// 						insertVarID(UINT_MAX, resultVar, _query->tripleNodes[i], sortID);
// 						tid = _query->tripleNodes[i].tripleNodeID;
// 						bufsize = EntityIDList[tid]->getSize();
// 					}
// 				}
// 			}
// 		}

// 		std::vector<EntityIDBuffer*> bufferlist;
// 		bufferlist.push_back(EntityIDList[_query->tripleNodes[i].tripleNodeID]);
// 		generateResultPos(resultVar, _queryGraph->getProjection(), resultPos);
// 		needselect = false;

// 		std::string URI;
// 		ID* p = EntityIDList[tid]->getBuffer();
// 		int IDCount = EntityIDList[tid]->getIDCount();

// 		for (i = 0; i != bufsize; ++i) {
// 			uriTable->getURIById(URI, p[i * IDCount + resultPos[0]]);
// #ifdef PRINT_RESULT
// 			std::cout << URI << std::endl;
// #else
// 			resultPtr->push_back(URI);
// #endif
// 		}
// 	} else {
// 		cout << "come here 2" << endl;
// 		//TODO materialize
// 	// std::sort(_query->tripleNodes.begin(),_query->tripleNodes.end(),TripleNode::idSort);
// 	std::vector<EntityIDBuffer*> bufferlist;
	
// 	std::vector<ID> resultVar;
// 	ID sortID;
// 	resultVar.resize(0);
	

// 	uint projBitV, nodeBitV, resultBitV, tempBitV;
// 	resultBitV = 0;
// 	generateProjectionBitVector(projBitV, _queryGraph->getProjection());

// 	int sortKey;
// 	size_t tnodesize = _query->tripleNodes.size();
// 	std::set<ID> tids;
// 	ID tid;
// 	bool complete = true;
// 	int i = 0;
// 	vector<ID>::iterator iter;

// 	keyPos.clear();
// 	resultPos.clear();
// 	verifyPos.clear();

// 	while (true) {
// 		//if the pattern has no buffer, then skip it
// 		if (EntityIDList.count(_query->tripleNodes[i].tripleNodeID) == 0) {
// 			++i;
// 			i = i % tnodesize;
// 			continue;
// 		}
// 		generateTripleNodeBitVector(nodeBitV, _query->tripleNodes[i]);
// 		if (countOneBits(nodeBitV) == 1) {
// 			++i;
// 			i = i % tnodesize;
// 			continue;
// 		}

// 		tid = _query->tripleNodes[i].tripleNodeID;
// 		if (tids.count(tid) == 0) {
// 			if (countOneBits(resultBitV) == 0) {
// 				insertVarID(UINT_MAX, resultVar, _query->tripleNodes[i], sortID);
// 				sortKey = EntityIDList[tid]->getSortKey();
// 			} else {
// 				tempBitV = nodeBitV & resultBitV;
// 				if (countOneBits(tempBitV) == 1) {
// 					ID key = bitVtoID(tempBitV); //ID(log((double)tempBitV)/log(2.0));
// 					sortKey = insertVarID(key, resultVar, _query->tripleNodes[i], sortID);
// 					iter = find(resultVar.begin(), resultVar.end(), sortID);
// 					keyPos.push_back(iter - resultVar.begin());
// 				} else if (countOneBits(tempBitV) == 2) {
// 					//verify buffers
// 					ID key = bitVtoID(tempBitV); //ID(log((double)tempBitV)/log(2.0));
// 					sortKey = insertVarID(key, resultVar, _query->tripleNodes[i], sortID);
// 					iter = find(resultVar.begin(), resultVar.end(), sortID);
// 					keyPos.push_back(iter - resultVar.begin());
// 				} else {
// 					complete = false;
// 					++i;
// 					i = i % tnodesize;
// 					continue;
// 				}
// 			}
// 			resultBitV = resultBitV | nodeBitV;
// 			EntityIDList[tid]->setSortKey(sortKey);
// 			bufferlist.push_back(EntityIDList[tid]);
// 			tids.insert(tid);
// 		}

// 		++i;
// 		if (i == tnodesize) {
// 			if (complete == true)
// 				break;
// 			else {
// 				complete = true;
// 				i = i % tnodesize;
// 			}
// 		}
// 	}
// 		// --------------------
// 		// std::vector<EntityIDBuffer*> bufferlist;
// 		// std::vector<ID> resultVar;
// 		// ID sortID;
// 		// resultVar.resize(0);
// 		// uint projBitV, nodeBitV, resultBitV, tempBitV;
// 		// resultBitV = 0;
// 		// generateProjectionBitVector(projBitV, _queryGraph->getProjection());
// 		// i = 0;
// 		// int sortKey;
// 		// size_t tnodesize = _query->tripleNodes.size();
// 		// while (true) {
// 		// 	if (EntityIDList.count(_query->tripleNodes[i].tripleNodeID) == 0 || (EntityIDList[_query->tripleNodes[i].tripleNodeID]->getIDCount() == 1)) {
// 		// 		++i;
// 		// 		i = i % tnodesize;
// 		// 		continue;
// 		// 	}
// 		// 	generateTripleNodeBitVector(nodeBitV, _query->tripleNodes[i]);
// 		// 	tempBitV = projBitV & nodeBitV;
// 		// 	if (tempBitV == nodeBitV) {
// 		// 		if (countOneBits(resultBitV) == 0) {
// 		// 			insertVarID(UINT_MAX, resultVar, _query->tripleNodes[i], sortID);
// 		// 			sortKey = EntityIDList[_query->tripleNodes[i].tripleNodeID]->getSortKey();
// 		// 		} else {
// 		// 			tempBitV = nodeBitV & resultBitV;
// 		// 			if (countOneBits(tempBitV) == 1) {
// 		// 				ID key = ID(log((double) tempBitV) / log(2.0));
// 		// 				sortKey = insertVarID(key, resultVar, _query->tripleNodes[i], sortID);
// 		// 			} else {
// 		// 				++i;
// 		// 				i = i % tnodesize;
// 		// 				continue;
// 		// 			}
// 		// 		}

// 		// 		resultBitV |= nodeBitV;
// 		// 		EntityIDList[_query->tripleNodes[i].tripleNodeID]->setSortKey(sortKey);
// 		// 		bufferlist.push_back(EntityIDList[_query->tripleNodes[i].tripleNodeID]);

// 		// 		if (countOneBits(resultBitV) == projectionSize)
// 		// 			break;
// 		// 	}

// 		// 	++i;
// 		// 	i = i % tnodesize;
// 		// }

// 		for (i = 0; i < bufferlist.size(); ++i) {
// 			bufferlist[i]->sort();
// 		}

// 		generateResultPos(resultVar, _queryGraph->getProjection(), resultPos);
// 		needselect = false;

// 		std::string URI;

// 		cout << "--come here 1" << endl;
// 		if (projectionSize == 2) {
// 			cout << "--come here 2" << endl;
// 			EntityIDBuffer* buf = bufferlist[0];
// 			size_t bufsize = buf->getSize();
// 			ID* ids = buf->getBuffer();
// 			int IDCount = buf->getIDCount();
// 			for (i = 0; i < bufsize; i++) {
// 				for (int j = 0; j < IDCount; ++j) {
// 					if (uriTable->getURIById(URI, ids[i * IDCount + resultPos[j]]) == OK) {
// #ifdef PRINT_RESULT
// 						cout << URI << " ";
// #else
// 						resultPtr->push_back(URI);
// #endif
// 					} else {
// 						cout << "not found" << endl;
// 					}
// 				}
// #ifdef PRINT_RESULT
// 				cout << endl;
// #endif
// 			}
// 		} else {
// 			cout << "buffer list " << endl;
// 			for(int i = 0;i < bufferlist.size(); i++){
// 				bufferlist[i]->print();
// 			}

// 			EntityIDBuffer* buf = bufferlist[0];
// 			size_t bufsize = buf->getSize();
// 			bufPreIndexs.resize(bufferlist.size(), 0);
// 			int IDCount = buf->getIDCount();
// 			ID *ids = buf->getBuffer();

// 			for (i = 0; i != bufsize; ++i) {
// 				resultVec.resize(0);
// 				for (int j = 0; j < IDCount; ++j) {
// 					resultVec.push_back(ids[i * IDCount + j]);
// 				}
// 				cout << " come here 343 "  << endl;
// 				cout << "get to Result_join ya " << endl;
// 				cout << keyPos[0] << endl;
// 				cout << resultVec[keyPos[0]] << endl;

// 				getResult_join(resultVec[keyPos[0]], bufferlist, 1);
// 			}
// 		}
// 	}
	// return OK;
}

// Status TripleBitWorkerQuery::cyclicJoin() {
// 	Timestamp cyc0;
// 	vector<TripleBitQueryGraph::JoinVariableNodeID>::iterator joinNodeIter = _query->joinVariables.begin();
// 	vector<pair<TripleBitQueryGraph::TripleNodeID, TripleBitQueryGraph::JoinVariableNode::DimType> >::iterator nodePatternIter;

// 	TripleBitQueryGraph::JoinVariableNode* node = NULL;
// 	EntityIDBuffer* buffer = NULL;
// 	TripleNode* triple = NULL;

// 	//initialize the patterns which are related to the first variable
// 	getVariableNodeByID(node, *joinNodeIter);
// 	nodePatternIter = node->appear_tpnodes.begin();
// 	getTripleNodeByID(triple, nodePatternIter->first);
// 	buffer = findEntityIDByTriple(triple, 0, INT_MAX)->getEntityIDBuffer();
// 	EntityIDList[nodePatternIter->first] = buffer;
// 	buffer->sig();
// 	//P5 reverse
// 	// TripleNode* triple5 = NULL;
// 	// getTripleNodeByID(triple5, 5);
// 	// triple5->scanOperation = TripleNode::FINDSOBYP;
// 	// node->appear_tpnodes.pop_back();



// 	if (this->findEntitiesAndJoin(*joinNodeIter, node->appear_tpnodes, true) == NULL_RESULT) {
// #ifdef PRINT_RESULT
// 		cout << "empty result" << endl;
// #else
// 		resultPtr->push_back("-1");
// 		resultPtr->push_back("Empty Result");
// #endif
// 		return OK;
// 	}

// Timestamp stage1;
// cout << "--------------stage1 time:" << (static_cast<double>(stage1-cyc0)/1000.0) << endl;

// 	// //p3 reverse
// 	// TripleNode* triple3 = NULL;
// 	// getTripleNodeByID(triple3, 3);
// 	// triple3->scanOperation = TripleNode::FINDSOBYP;
	

	

// 	++joinNodeIter;
// 	//iterate the pattern groups
// 	for (; joinNodeIter != _query->joinVariables.end(); ++joinNodeIter) {
// 		getVariableNodeByID(node, *joinNodeIter);
// 		// if(*joinNodeIter==3){
// 		// 	node->appear_tpnodes.pop_back();
// 		// }
// 		if (node->appear_tpnodes.size() > 1) {
// 			if (this->findEntitiesAndJoin(*joinNodeIter, node->appear_tpnodes, true) == NULL_RESULT) {
// #ifdef PRINT_RESULT
// 				cout << "empty result" << endl;
// #else
// 				resultPtr->push_back("-1");
// 				resultPtr->push_back("Empty Result");
// #endif
// 				return OK;
// 			}
// 		}
// 	}

// Timestamp stage2;
// cout << "-------------stage2 time:" << (static_cast<double>(stage2-stage1)/1000.0) << endl;

// 	//iterate reverse
// 	bool isLeafNode;
// 	TripleBitQueryGraph::JoinVariableNodeID varID;
// 	size_t size = (int) _query->joinVariables.size();
// 	size_t i = 0;

// 	for (i = size - 1; i != ((size_t) (-1)); --i) {
// 		varID = _query->joinVariables[i];
// 		isLeafNode = nodeIsLeaf(varID);
// 		if (isLeafNode == false) {
// 			getVariableNodeByID(node, varID);
// 			if (this->findEntitiesAndJoin(varID, node->appear_tpnodes, false) == NULL_RESULT) {
// #ifdef PRINT_RESULT
// 				cout << "empty result" << endl;
// #else
// 				resultPtr->push_back("-1");
// 				resultPtr->push_back("Empty Result");
// #endif
// 				return OK;
// 			}
// 		}
// 	}


// 	for (i = 0; i < size; ++i) {
// 		varID = _query->joinVariables[i];
// 		isLeafNode = nodeIsLeaf(varID);
// 		if (isLeafNode == false) {
// 			getVariableNodeByID(node, varID);
// 			if (this->findEntitiesAndJoin(varID, node->appear_tpnodes, false) == NULL_RESULT) {
// #ifdef PRINT_RESULT
// 				cout << "empty result" << endl;
// 				cout << "The forth findEntitiesAndJoin" << endl;
// #else
// 				resultPtr->push_back("-1");
// 				resultPtr->push_back("Empty Result");
// #endif
// 				return OK;
// 			}
// 		}
// 	}

// 	for (i = size - 1; i != ((size_t) (-1)); --i) {
// 		varID = _query->joinVariables[i];
// 		isLeafNode = nodeIsLeaf(varID);
// 		if (isLeafNode == false) {
// 			getVariableNodeByID(node, varID);
// 			if (this->findEntitiesAndJoin(varID, node->appear_tpnodes, false) == NULL_RESULT) {
// #ifdef PRINT_RESULT
// 				cout << "empty result" << endl;
// #else
// 				resultPtr->push_back("-1");
// 				resultPtr->push_back("Empty Result");
// #endif
// 				return OK;
// 			}
// 		}
// 	}

// Timestamp stage3;
// cout << "-----------------stage3 time:" << (static_cast<double>(stage3-stage2)/1000.0) << endl;

// 	//TODO materialize
// Timestamp mat0;

// 	std::vector<EntityIDBuffer*> bufferlist;
// 	std::vector<ID> resultVar;
// 	ID sortID;

// 	resultVar.resize(0);
// 	uint projBitV, nodeBitV, resultBitV, tempBitV;
// 	resultBitV = 0;
// 	generateProjectionBitVector(projBitV, _queryGraph->getProjection());

// 	int sortKey;
// 	size_t tnodesize = _query->tripleNodes.size();
// 	std::set<ID> tids;
// 	ID tid;
// 	bool complete = true;
// 	i = 0;
// 	vector<ID>::iterator iter;

// 	keyPos.clear();
// 	resultPos.clear();
// 	verifyPos.clear();

// 	while (true) {
// 		//if the pattern has no buffer, then skip it
// 		if (EntityIDList.count(_query->tripleNodes[i].tripleNodeID) == 0) {
// 			++i;
// 			i = i % tnodesize;
// 			continue;
// 		}
// 		generateTripleNodeBitVector(nodeBitV, _query->tripleNodes[i]);
// 		if (countOneBits(nodeBitV) == 1) {
// 			++i;
// 			i = i % tnodesize;
// 			continue;
// 		}

// 		tid = _query->tripleNodes[i].tripleNodeID;
// 		if (tids.count(tid) == 0) {
// 			if (countOneBits(resultBitV) == 0) {
// 				insertVarID(UINT_MAX, resultVar, _query->tripleNodes[i], sortID);
// 				sortKey = EntityIDList[tid]->getSortKey();
// 			} else {
// 				tempBitV = nodeBitV & resultBitV;
// 				if (countOneBits(tempBitV) == 1) {
// 					ID key = bitVtoID(tempBitV); //ID(log((double)tempBitV)/log(2.0));
// 					sortKey = insertVarID(key, resultVar, _query->tripleNodes[i], sortID);
// 					iter = find(resultVar.begin(), resultVar.end(), sortID);
// 					keyPos.push_back(iter - resultVar.begin());
// 				} else if (countOneBits(tempBitV) == 2) {
// 					//verify buffers
// 					ID key = bitVtoID(tempBitV); //ID(log((double)tempBitV)/log(2.0));
// 					sortKey = insertVarID(key, resultVar, _query->tripleNodes[i], sortID);
// 					iter = find(resultVar.begin(), resultVar.end(), sortID);
// 					keyPos.push_back(iter - resultVar.begin());
// 				} else {
// 					complete = false;
// 					++i;
// 					i = i % tnodesize;
// 					continue;
// 				}
// 			}
// 			resultBitV = resultBitV | nodeBitV;
// 			EntityIDList[tid]->setSortKey(sortKey);
// 			bufferlist.push_back(EntityIDList[tid]);
// 			tids.insert(tid);
// 		}

// 		++i;
// 		if (i == tnodesize) {
// 			if (complete == true)
// 				break;
// 			else {
// 				complete = true;
// 				i = i % tnodesize;
// 			}
// 		}
// 	}

// 	for (i = 0; i < bufferlist.size(); ++i) {
// 		bufferlist[i]->sort();
// 	}

// 	generateResultPos(resultVar, _queryGraph->getProjection(), resultPos);
// 	//generate verify pos vector
// 	generateVerifyPos(resultVar, verifyPos);
// 	needselect = true;

// Timestamp mat1;
// cout << "ready get result join time:" << (static_cast<double>(mat1-mat0)/1000.0) << endl;

// #ifdef COLDCACHE
// 	Timestamp t1;
// #endif

// 	EntityIDBuffer* buf = bufferlist[0];
// 	size_t bufsize = buf->getSize();

// #ifdef PRINT_BUFFERSIZE
//     cout<<"\n------------before getJoin------------"<<endl;
// 	for(int i=0;i<bufferlist.size();i++){
// 		bufferlist[i]->sig();
// 	}
// #endif

// 	int IDCount = buf->getIDCount();
// 	ID *ids = buf->getBuffer();
// 	sortKey = buf->getSortKey();
// 	for (i = 0; i != bufsize; ++i) {
// 		resultVec.resize(0);
// 		for (int j = 0; j < IDCount; ++j) {
// 			resultVec.push_back(ids[i * IDCount + j]);
// 		}

// 		getResult_join(resultVec[keyPos[0]], bufferlist, 1);
// 	}

// #ifdef PRINT_BUFFERSIZE
//     cout<<"\n------------after getJoin------------"<<endl;
// 	cout<<"ans: "<<ans<<endl;
// 	ans = 0;
// #endif

// #ifdef COLDCACHE
// 	Timestamp t2;
// 	cout << "getResult_Join time:" << (static_cast<double>(t2-t1)/1000.0) << endl;
// #endif
// 	Timestamp cyc1;
// 	cout << "cyclic join time:" << (static_cast<double>(cyc1-cyc0)/1000.0) << endl;
// 	return OK;
// }

// #include<hash_map>
// using namespace __gnu_cxx;
// hash_map<ID,int> x_map;
// hash_map<ID,int> y_map;
// hash_map<ID,int> z_map;


// Status TripleBitWorkerQuery::cyclicJoin(){
// 	TripleBitQueryGraph::OpType operationType = TripleBitQueryGraph::QUERY;
// 	shared_ptr<IndexForTT> indexForTT(new IndexForTT);
// 	SubTrans *subTrans = new SubTrans(*transactionTime, workerID, minID, maxID, operationType, 1, *triple, indexForTT);
// 	tasksEnQueue(triple->predicate, subTrans);
// 	ResultIDBuffer *retBuffer = resultBuffer[triple->predicate]->DeQueue();

// #ifdef PATTERN_TIME
// 	gettimeofday(&end, NULL);
	
// 	cout<<endl<< "#######find pattern " << triple->tripleNodeID << " time elapsed: " << ((end.tv_sec - start.tv_sec) * 1000000 + end.tv_usec - start.tv_usec) / 1000000.0
// 			<< endl;
// 	retBuffer->getSize();
// #endif
// }

// map<ID,hash_map<ID,int> > fountain;
// map<ID,map<ID,int> >fountain;
// map<ID,map<ID,int> >fountain;



int data[] = {9,13,779,9,
79,81,1271,79,
2510,2512,2976,2510,
5868,5869,6092,5868,
5868,5869,6292,5868,
7351,7352,8156,7351,
7404,7405,8412,7404,
9635,9637,10506,9635,
9651,9652,10454,9651,
9657,9658,10088,9657,
11242,11244,11940,11242,
12827,12829,13862,12827,
12868,12869,13478,12868,
14731,14732,14991,14731,
14731,14732,15209,14731,
14736,14738,15203,14736,
18160,18161,18782,18160,
18165,18167,18600,18165,
18190,18191,18620,18190,
19962,19964,20253,19962,
19983,19984,20277,19983,
19990,19992,20325,19990,
19990,19992,20147,19990,
21418,21420,21649,21418,
23091,23093,23887,23091,
23105,23107,23683,23105,
24738,24740,24967,24738,
24743,24745,25319,24743,
24748,24749,25463,24748,
24765,24766,25407,24765};

void printV(map<ID,int> &var,int height){
	cout<<"----------------"<<endl;
	map<ID, int>::iterator iter = var.begin();
	while(iter != var.end()){
		// cout<<iter->first<<" "<<iter->second<<endl;
		if(iter->second>=height){
			cout<<iter->first<<endl;
		}
		iter++;
	}
	cout<<endl;
}

void printVar(map<ID,int> &var,int height,int col){
	// cout<<"----------------"<<endl;
	// map<ID, int>::iterator iter = var.begin();
	// while(iter != var.end()){
	// 	// cout<<iter->first<<" "<<iter->second<<endl;
	// 	if(iter->second>=height){
	// 		cout<<iter->first<<endl;
	// 	}
	// 	iter++;
	// }
	// cout<<endl;
	for(int i = 0;i < 30;i++){
		if(var.count(data[i*4+col]) && var[data[i*4+col]]>=height){
			continue;
		}else{
			cout<<"data "<<data[i*4+col]<<" not found!"<<endl;
			cout<<"-----error! @height "<<height<<" @col "<<col<<endl;
			printV(var,height);
			return;
		}
	}
	cout<<"-----correct! @height "<<height<<" @col "<<col<<endl;
}



// #include "ParallelAlgrithm.hpp"
// void printTest(int i){
// 	cout<<i<<endl;
// }


// #include "bf/all.hpp"
// using namespace bf;

bool isP4 = false;
// bf::basic_bloom_filter *P4 = NULL;

Status TripleBitWorkerQuery::cyclicJoin() {
	timeval one_begin,one_end;
	gettimeofday(&one_begin,NULL);
	CBitMap * temp_a = new CBitMap(1,max_id);
	CBitMap * temp_b = new CBitMap(1,max_id);
	gettimeofday(&one_end,NULL);
	cout << "build time  " << ((one_end.tv_sec - one_begin.tv_sec) * 1000000.0 + (one_end.tv_usec - one_begin.tv_usec)) << " us " << endl;
	



	timeval sj_begin,sj_end;
	gettimeofday(&sj_begin,NULL);
	timeval init_begin,init_end;
	gettimeofday(&init_begin,NULL);

	cout <<"cyclic join"<<endl;
	cout <<"variable count  " <<  this->_queryGraph->variableCount << endl;

	/*set BM*/
	fountain.clear();
	// fountain.resize(_query->joinVariables.size() + 1);
	fountain.resize(this->_queryGraph->variableCount);
	cout << fountain.size() << endl;
	// TripleBitQueryGraph::JoinVariableNode* node = NULL;
	// getVariableNodeByID(node, 0);

    //从一开始，不一定都有
	for(int i = 1;i < fountain.size(); i++){
		TripleBitQueryGraph::JoinVariableNode* node = NULL;
		getVariableNodeByID(node, i);
		if(!node){
			CBitMap * temp = new CBitMap(1,max_id);
			fountain[i].push_back(temp);
		}else{
			// cout << node->appear_tpnodes.size() << endl;
			for(int j = 0;j < node->appear_tpnodes.size();j++){
				CBitMap * temp = new CBitMap(1,max_id);
				fountain[i].push_back(temp);
			}
		}
	}

	//这个fountain存放的是每个变量的 appear_tpnodes 生成的CBitMap，用来干什么的？
	cout << "fountain" << endl;
	for(int i=1;i<=fountain.size();i++){
		cout << "variable i  : "<< i << " total height " <<fountain[i].size() << endl;
	}

	for(int i = 1; i <= _query->joinVariables.size();i++){
		var_min_max[i] = make_pair(0,UINT_MAX);
	}

	gettimeofday(&init_end,NULL);
	cout << "init time  " << ((init_end.tv_sec - init_begin.tv_sec) * 1000000.0 + (init_end.tv_usec - init_begin.tv_usec)) << " us " << endl;
	


	ID minID = 0;
	ID maxID = UINT_MAX;


	EntityIDBuffer* buffer = NULL;
	TripleNode* triple = NULL;
	

	map<TripleNodeID,ResultIDBuffer *> res;

	// for(auto & x : _query->tripleNodes){
	for(int i = 0;i < _query->tripleNodes.size(); i++){
		TripleNode& x = _query->tripleNodes[i]; 
		// cout<<"varHeight "<<x.varHeight.size()<<endl;
		firstVar = x.varHeight[0].first;
		firstHeight = x.varHeight[0].second;
		if(x.varHeight.size() > 1){
			secVar = x.varHeight[1].first;
			secHeight = x.varHeight[1].second;
		}


		// if(x.tripleNodeID == 3){
		// 	res[0]->printMinMax();
		// 	res[1]->printMinMax();
		// 	res[2]->printMinMax();

		// 	minID = 117;
		// 	maxID = 118;
		// 	x.scanOperation = TripleNode::FINDOSBYP;

		// }
	
		cout << "firstVar " << firstVar << " " << var_min_max[firstVar].first << " " << var_min_max[firstVar].second;
	    

		// if(x.tripleNodeID == 3){
		// 	isP3 = true;
		// }
		res[x.tripleNodeID] = findEntityIDByTriple(&x, var_min_max[firstVar].first, var_min_max[firstVar].second);
		// res[x.tripleNodeID] = findEntityIDByTriple(&x, minID, maxID);

		
		#ifdef TEST_ENTITYIDBUFFER
			EntityIDBuffer* testbuffer = NULL;
			testbuffer=res[x.tripleNodeID]->getEntityIDBuffer();
			testbuffer->sig();
		#endif
        


		ID a_min = UINT_MAX;
		ID a_max = 1;
		ID b_min = UINT_MAX;
		ID b_max = 1;
		res[x.tripleNodeID]->printMinMax(a_min,a_max,b_min,b_max);
		cout << "new a b" << endl;
		cout << "a_min " << a_min << " a_max " << a_max << " b_min " << b_min << " b_max " << b_max << endl;

		if(!( a_min == UINT_MAX && a_max == 1 && b_min == UINT_MAX && b_max == 1)){
            var_min_max[firstVar].first = max(var_min_max[firstVar].first,a_min);
			var_min_max[firstVar].second = min(var_min_max[firstVar].second,a_max);

			if(x.varHeight.size() > 1){
				var_min_max[secVar].first = max(var_min_max[secVar].first,b_min);
				var_min_max[secVar].second = min(var_min_max[secVar].second,b_max);
				// secVar = x.varHeight[1].first;
				// secHeight = x.varHeight[1].second;
			}
		}
		

		// if(x.tripleNodeID == 1){
		// 	cout << "Triple 1" << endl;
		// 	res[x.tripleNodeID]->print();
		// }

		for(int i = 1; i <= _query->joinVariables.size();i++){
			cout << "var " << i << " " << var_min_max[i].first << " " << var_min_max[i].second << endl;
			// var_min_max[i] = make_pair(0,UINT_MAX);
		}

                

		cout<<"tripleNodeID "<<x.tripleNodeID<<" SIZE: "<<res[x.tripleNodeID]->getSize()<<endl;
		cout << "i " << i << " _query->tripleNodes.size() " <<  _query->tripleNodes.size() << endl;
               // struct timeval sortStart,sortEnd;
                //gettimeofday(&sortStart,NULL);
                /*if(x.tripleNodeID == 0){
		       gettimeofday(&sortStart,NULL);
                       EntityIDBuffer * idb = res[x.tripleNodeID]->getEntityIDBuffer();
		       idb->sort();
                       gettimeofday(&sortEnd,NULL);
                       cout << " ---shuffle time elapsed " << ((sortEnd.tv_sec - sortStart.tv_sec) * 1000000 + sortEnd.tv_usec - sortStart.tv_usec) / 100000.0 << " s" << endl;
		}*/
                //gettimeofday(&sortEnd,NULL);
                //cout << " time elapsed: " << ((end.tv_sec - start.tv_sec) * 1000000 + end.tv_usec - start.tv_usec) / 1000000.0 << " s" << endl;

	}


	/*  reduce */
	cout << "go to reduce part" << endl;
    for(int i = 0;i < _query->tripleNodes.size(); i++){
		TripleNode& x = _query->tripleNodes[i];
		if(res[x.tripleNodeID]->getSize()>0){
			firstVar = x.varHeight[0].first;
			firstHeight = x.varHeight[0].second;
			secVar = x.varHeight[1].first;
			secHeight = x.varHeight[1].second;

			if(firstHeight < fountain[firstVar].size() - 1 || secHeight < fountain[secVar].size() - 1){
				cout<<"triple "<<x.tripleNodeID<<" "<<"before reduce "<<res[x.tripleNodeID]->getSize()<<endl;
				reduce_p(res[x.tripleNodeID]);
				cout<<"after reduce "<<res[x.tripleNodeID]->getSize()<<endl;
			}
		}
	} 

	gettimeofday(&sj_end,NULL);
	cout << "scan and join time  " << ((sj_end.tv_sec - sj_begin.tv_sec) * 1000000.0 + (sj_end.tv_usec - sj_begin.tv_usec)) << " us " << endl;
	return OK;
// /*set BM*/
// 	fountain.clear();
// 	fountain.resize(_query->joinVariables.size() + 1);
// 	cout << fountain.size() << endl;
// 	// TripleBitQueryGraph::JoinVariableNode* node = NULL;
// 	// getVariableNodeByID(node, 0);

//     //从一开始，不一定都有
// 	for(int i = 1;i < fountain.size(); i++){
// 		TripleBitQueryGraph::JoinVariableNode* node = NULL;
// 		getVariableNodeByID(node, i);
// 		cout << node->appear_tpnodes.size() << endl;
// 		for(int j = 0;j < node->appear_tpnodes.size();j++){
// 			CBitMap * temp = new CBitMap(1,max_id);
// 			fountain[i].push_back(temp);
// 		}
// 	}

// 	cout << "fountain" << endl;
// 	for(int i=0;i<fountain.size();i++){
// 		cout << "i  : "<<fountain[i].size() << endl;
// 	}


// 	// setBM(max_id);
	
// 	ID minID = 0;


// 	ID maxID = UINT_MAX;
// 	EntityIDBuffer* buffer = NULL;
// 	TripleNode* triple = NULL;
	

// 	map<TripleNodeID,ResultIDBuffer *> res;

// 	// for(auto & x : _query->tripleNodes){
// 	for(int i = 0;i < _query->tripleNodes.size(); i++){
// 		TripleNode& x = _query->tripleNodes[i]; 
// 		// cout<<"varHeight "<<x.varHeight.size()<<endl;
// 		firstVar = x.varHeight[0].first;
// 		firstHeight = x.varHeight[0].second;
// 		if(x.varHeight.size() > 1){
// 			secVar = x.varHeight[1].first;
// 			secHeight = x.varHeight[1].second;
// 		}
// 		res[x.tripleNodeID] = findEntityIDByTriple(&x, minID, maxID);
// 		cout<<"tripleNodeID "<<x.tripleNodeID<<" SIZE: "<<res[x.tripleNodeID]->getSize()<<endl;
// 	}


// 	/*  reduce */
// 	// for(auto & x : _query->tripleNodes){
// 	for(int i = 0;i < _query->tripleNodes.size(); i++){
// 		TripleNode& x = _query->tripleNodes[i]; 
// 		// cout<<"varHeight "<<x.varHeight.size()<<endl;
// 		if(res[x.tripleNodeID]->getSize()>0){
// 			firstVar = x.varHeight[0].first;
// 			firstHeight = x.varHeight[0].second;
// 			secVar = x.varHeight[1].first;
// 			secHeight = x.varHeight[1].second;

// 			if(firstHeight < 2 || secHeight < 2){
// 				cout<<"triple "<<x.tripleNodeID<<" "<<"before reduce "<<res[x.tripleNodeID]->getSize()<<endl;
// 				reduce_p(res[x.tripleNodeID]);
// 				cout<<"after reduce "<<res[x.tripleNodeID]->getSize()<<endl;
// 			}
// 		}
// 	}

	

// 	// reduce_p5(res[5]);
// 	// cout<<"after reduce p5 , got size"<<endl;
// 	// cout<<res[5]->getSize()<<endl;

// 	// reduce_p0(res[0]);
// 	// cout<<"after reduce p0 , got size"<<endl;
// 	// cout<<res[0]->getSize()<<endl;






// Timestamp mat0;
	// for(int i = 0;i < _query->tripleNodes.size(); i++){
	// 	TripleNode& x = _query->tripleNodes[i]; 
	// // for(auto & x : _query->tripleNodes){
	// 	// cout<<"varHeight "<<x.varHeight.size()<<endl;
	// 	if(res[x.tripleNodeID]->getSize()>0){
	// 		EntityIDBuffer *buf = new EntityIDBuffer;
	// 		buf->empty();
	// 		buf->setIDCount(2);
	// 		res[x.tripleNodeID]->print2ID(buf);
	// 		EntityIDList[x.tripleNodeID] = buf;
	// 	}
	// }



	// EntityIDBuffer *buf0 = new EntityIDBuffer;
	// buf0->empty();
	// buf0->setIDCount(2);


	// EntityIDBuffer *buf3 = new EntityIDBuffer;
	// buf3->empty();
	// buf3->setIDCount(2);


	// EntityIDBuffer *buf5 = new EntityIDBuffer;
	// buf5->empty();
	// buf5->setIDCount(2);
	
	// res[5]->print2ID(buf5);
	// res[0]->print2ID(buf0);
	// res[3]->print2ID(buf3);

	// Timestamp mat001;
	// buf5->sort();
	// buf0->sort();
	// buf3->sort();
	// Timestamp mat002;
	// cout << "get sort : " << (static_cast<double>(mat002-mat001)/1000.0) << endl;

	/*FULL JOIN*/
	// EntityIDList[5] = buf5;
	// EntityIDList[0] = buf0;
	// EntityIDList[3] = buf3;

	//TODO materialize
	// std::sort(_query->tripleNodes.begin(),_query->tripleNodes.end(),TripleNode::idSort);
// 	std::vector<EntityIDBuffer*> bufferlist;
	
// 	std::vector<ID> resultVar;
// 	ID sortID;
// 	resultVar.resize(0);
	

// 	uint projBitV, nodeBitV, resultBitV, tempBitV;
// 	resultBitV = 0;
// 	generateProjectionBitVector(projBitV, _queryGraph->getProjection());

// 	int sortKey;
// 	size_t tnodesize = _query->tripleNodes.size();
// 	std::set<ID> tids;
// 	ID tid;
// 	bool complete = true;
// 	int i = 0;
// 	vector<ID>::iterator iter;

// 	keyPos.clear();
// 	resultPos.clear();
// 	verifyPos.clear();

// 	while (true) {
// 		//if the pattern has no buffer, then skip it
// 		if (EntityIDList.count(_query->tripleNodes[i].tripleNodeID) == 0) {
// 			++i;
// 			i = i % tnodesize;
// 			continue;
// 		}
// 		generateTripleNodeBitVector(nodeBitV, _query->tripleNodes[i]);
// 		if (countOneBits(nodeBitV) == 1) {
// 			++i;
// 			i = i % tnodesize;
// 			continue;
// 		}

// 		tid = _query->tripleNodes[i].tripleNodeID;
// 		if (tids.count(tid) == 0) {
// 			if (countOneBits(resultBitV) == 0) {
// 				insertVarID(UINT_MAX, resultVar, _query->tripleNodes[i], sortID);
// 				sortKey = EntityIDList[tid]->getSortKey();
// 			} else {
// 				tempBitV = nodeBitV & resultBitV;
// 				if (countOneBits(tempBitV) == 1) {
// 					ID key = bitVtoID(tempBitV); //ID(log((double)tempBitV)/log(2.0));
// 					sortKey = insertVarID(key, resultVar, _query->tripleNodes[i], sortID);
// 					iter = find(resultVar.begin(), resultVar.end(), sortID);
// 					keyPos.push_back(iter - resultVar.begin());
// 				} else if (countOneBits(tempBitV) == 2) {
// 					//verify buffers
// 					ID key = bitVtoID(tempBitV); //ID(log((double)tempBitV)/log(2.0));
// 					sortKey = insertVarID(key, resultVar, _query->tripleNodes[i], sortID);
// 					iter = find(resultVar.begin(), resultVar.end(), sortID);
// 					keyPos.push_back(iter - resultVar.begin());
// 				} else {
// 					complete = false;
// 					++i;
// 					i = i % tnodesize;
// 					continue;
// 				}
// 			}
// 			resultBitV = resultBitV | nodeBitV;
// 			EntityIDList[tid]->setSortKey(sortKey);
// 			bufferlist.push_back(EntityIDList[tid]);
// 			tids.insert(tid);
// 		}

// 		++i;
// 		if (i == tnodesize) {
// 			if (complete == true)
// 				break;
// 			else {
// 				complete = true;
// 				i = i % tnodesize;
// 			}
// 		}
// 	}

// 	for (i = 0; i < bufferlist.size(); ++i) {
// 		bufferlist[i]->sort();
// 	}

// 	generateResultPos(resultVar, _queryGraph->getProjection(), resultPos);
// 	//generate verify pos vector
// 	generateVerifyPos(resultVar, verifyPos);
// 	needselect = true;

// Timestamp mat1;
// // cout << "ready get result join time:" << (static_cast<double>(mat1-mat0)/1000.0) << endl;

// #ifdef COLDCACHE
// 	Timestamp t1;
// #endif

// 	EntityIDBuffer* buf = bufferlist[0];
// 	size_t bufsize = buf->getSize();

// #ifdef PRINT_BUFFERSIZE
//     cout<<"\n------------before getJoin------------"<<endl;
// 	for(int i=0;i<bufferlist.size();i++){
// 		bufferlist[i]->sig();
// 	}
// #endif

// 	int IDCount = buf->getIDCount();
// 	ID *ids = buf->getBuffer();
// 	// sortKey = buf->getSortKey();
// 	for (i = 0; i != bufsize; ++i) {
// 		resultVec.resize(0);
// 		for (int j = 0; j < IDCount; ++j) {
// 			resultVec.push_back(ids[i * IDCount + j]);
// 		}
// 		if(ids[i*IDCount] == 72828){
// 			cout<<"Got 72828 1"<<endl;
// 		}

// 		getResult_join(resultVec[keyPos[0]], bufferlist, 1);
// 	}

// #ifdef PRINT_BUFFERSIZE
//     cout<<"\n------------after getJoin------------"<<endl;
// 	cout<<"ans: "<<ans<<endl;
// 	cout<<"gR: "<<gR<<endl;
// 	ans = 0;
// 	gR = 0;
// #endif

// #ifdef COLDCACHE
// 	Timestamp t2;
// 	cout << "getResult_Join time:" << (static_cast<double>(t2-t1)/1000.0) << endl;
// #endif
// 	// Timestamp cyc1;
// 	// cout << "cyclic join time:" << (static_cast<double>(cyc1-cyc0)/1000.0) << endl;
// 	return OK;
 }
