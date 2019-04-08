/*
 * PlanGenerator.h
 *
 *  Created on: 2010-5-12
 *      Author: liupu
 */

#ifndef PLANGENERATOR_H_
#define PLANGENERATOR_H_

class IRepository;
class TripleBitQueryGraph;

#include "IRepository.h"
#include "TripleBitQueryGraph.h"
#include "TripleBit.h"

/*
 * Compared with the older version,delete the static PlanGenerator *self
 * because in the multithread process, it will result in some thread-safe problems
 */

class PlanGenerator {
private:
	IRepository& repo;
	TripleBitQueryGraph::SubQuery* query;
	TripleBitQueryGraph* graph;
public:
	PlanGenerator(IRepository& _repo);
	Status generatePlan(TripleBitQueryGraph& _graph);
	virtual ~PlanGenerator();
	int	getSelectivity(TripleBitQueryGraph::TripleNodeID& tripleID);
	void sortJoinVariableNode(TripleBitQueryGraph::JoinVariableNode& node);
private:
	/// Generate the scan operator for the query pattern.
	Status generateScanOperator(TripleNode& node, TripleBitQueryGraph::JoinVariableNodeID varID,map<TripleBitQueryGraph::JoinVariableNodeID,int> & varRecord);
	Status thunder_generateScanOperator(TripleNode& node, map<TripleBitQueryGraph::JoinVariableNodeID,int> & varRecord);
	Status generateSelectivity(TripleBitQueryGraph::JoinVariableNode& node, map<TripleBitQueryGraph::JoinVariableNodeID,int>& selectivityMap);
	TripleBitQueryGraph::JoinVariableNode::JoinType getJoinType(TripleBitQueryGraph::JoinVariableNode& node);
	Status bfsTraverseVariableNode();
	Status getAdjVariableByID(TripleBitQueryGraph::JoinVariableNodeID id, vector<TripleBitQueryGraph::JoinVariableNodeID>& nodes);
};

#endif /* PLANGENERATOR_H_ */
