
#ifndef REG_SELECTION_C                                        
#define REG_SELECTION_C

#include "RegularSelection.h"

RegularSelection :: RegularSelection (MyDB_TableReaderWriterPtr inputIn, MyDB_TableReaderWriterPtr outputIn,
                string selectionPredicateIn, vector <string> projectionsIn, int threadNumIn) {
	input = inputIn;
	output = outputIn;
	selectionPredicate = selectionPredicateIn;
	projections = projectionsIn;
	threadNum = threadNumIn;
}


void RegularSelection :: regSelThread(
	MyDB_TableReaderWriterPtr inputIn, MyDB_TableReaderWriterPtr outputIn,
	MyDB_RecordPtr inputRec, MyDB_RecordPtr outputRec,
	vector <func> finalComputations,
	int low, int high) {
	// A thread gets a pinned page to append
	MyDB_PageReaderWriterPtr localPageRW = make_shared<MyDB_PageReaderWriter>(true, *(input->getBufferMgr()));

	// now, iterate through the B+-tree query results
	MyDB_RecordIteratorAltPtr myIter = input->getIteratorAlt (low, high);
	while (myIter->advance ()) {

		myIter->getCurrent (inputRec);

		// see if it is accepted by the predicate
		if (!pred()->toBool ()) {
			continue;
		}

		// run all of the computations
		int i = 0;
		for (auto &f : finalComputations) {
			outputRec->getAtt (i++)->set (f());
		}

		outputRec->recordContentHasChanged ();
		if(!(localPageRW -> append(outputRec)) ) {
			//lock.lock();
			output->appendPage(*localPageRW);
			//lock.unlock();
			localPageRW->clear();
			localPageRW->append(outputRec);
		}
		//output->append (outputRec);
	}
}

void RegularSelection :: run () {

	MyDB_RecordPtr inputRec = input->getEmptyRecord ();
	MyDB_RecordPtr outputRec = output->getEmptyRecord ();
	
	// compile all of the coputations that we need here
	vector <func> finalComputations;
	for (string s : projections) {
		finalComputations.push_back (inputRec->compileComputation (s));
	}
	func pred = inputRec->compileComputation (selectionPredicate);

	// Table partition for each thread
	int pageNumber = input->getNumPages();
	int pagePartition = pageNumber / threadNum;
	int i;
	for(i = 0; i < threadNum - 1; i++) {
		threads.push_back(thread(&RegularSelection::regSelThread, this, i * pagePartition, (i + 1) * pagePartition - 1));
	}
	threads.push_back(thread(&RegularSelection::regSelThread, this, i * pagePartition, pageNumber - 1));

	for(auto& t : threads) {
		t.join();
	}
}

#endif
