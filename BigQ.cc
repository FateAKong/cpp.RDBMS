/*
 * BigQ.cpp
 *
 *  Created on: Feb 13, 2013
 *      Author: Fate.AKong
 */

#include <cstdlib>
#include <iostream>

#include "BigQ.h"
#include "File.h"

using namespace std;

//struct thrd_data {
//	Pipe &inputPipe;
//	Pipe &outputPipe;
//	OrderMaker &sortOrder;
//	int runLength;
//};

struct RecordInRun {
    Record record;
    int runIndex;

    ~RecordInRun() { //TODO do sth to make it better
	record.Reuse();
    }
};

class RecComp {
private:
    bool reverse;
    OrderMaker sortOrder;
public:

    RecComp(OrderMaker& _sortOrder, const bool &_reverse) :
    sortOrder(_sortOrder), reverse(_reverse) {
    }

    bool operator()(Record &lhs, Record &rhs) {
	ComparisonEngine compEng;
	int rc = compEng.Compare(&lhs, &rhs, &sortOrder);
	if (reverse) // return left>right
	    return rc == 1;
	else
	    // return left<right
	    return rc == -1;
    }
};

class RIRComp {
private:
    RecComp* comp;
public:

    RIRComp(OrderMaker& _sortOrder, const bool &_reverse) {
	comp = new RecComp(_sortOrder, _reverse);
    }

    bool operator()(RecordInRun &lhs, RecordInRun &rhs) {
	return comp->operator ()(lhs.record, rhs.record);
    }
};

BigQ::BigQ(Pipe &_inputPipe, Pipe &_outputPipe, OrderMaker &_sortOrder,
	int runLength) : inputPipe(_inputPipe), outputPipe(_outputPipe), sortOrder(_sortOrder), runLenInBytes(
runLength * PAGE_SIZE) {
    //	struct thrd_data my_thrd_data = { inputPipe, outputPipe, sortOrder,
    //			runLength };
    int rc;
    rc = pthread_create(&workerThrd, NULL, workerHelper,
	    this /*&my_thrd_data*/);
    if (rc) {
	printf("ERROR; return code from pthread_create() is %d\n", rc);
	exit(-1);
    }
    //	pthread_join(workerThrd, NULL);
}

void *BigQ::workerHelper(void *context) {
    return ((BigQ *) context)->workerFunc();
}

void *BigQ::workerFunc() {
    // TODO all these variables should be member variables or should be like now
    //	Pipe &inputPipe = ((struct thrd_data *)my_thrd_data)->inputPipe;
    //	Pipe &outputPipe = ((struct thrd_data *)my_thrd_data)->outputPipe;
    //	OrderMaker &sortOrder = ((struct thrd_data *)my_thrd_data)->sortOrder;
    //	int runLength = ((struct thrd_data *)my_thrd_data)->runLength;
    //	BigQ *obj = (BigQ*)_obj;

    Schema* psn = new Schema("catalog", "nation");

    // TODO create a temp file as  sorting buffer using pid as file name
    pthread_t self = pthread_self();
    char fileName[100];
#if defined _WIN32
    sprintf(fileName, "%u.sr", self.x);
#elif defined __linux
    sprintf(fileName, "%u.sr", (unsigned int) self);
#endif   
    //	File sortedRuns;
    sortedRuns.Open(0, fileName);
    //	runLenInBytes = ((struct thrd_data *) my_thrd_data)->runLength * PAGE_SIZE;

    // --- RUN GENERATION ---
    // TODO read from input and once there is enough then sort
    Record curRec;
    int curSizeInBytes = 0, curRecSize;
    while (inputPipe.Remove(&curRec)) {
	curRecSize = curRec.GetLength();
	if (curSizeInBytes + curRecSize > runLenInBytes) {
	    exportSortedRun();
	    curSizeInBytes = 0;
	}
	listBuf.push_back(curRec);
	curSizeInBytes += curRecSize;
	curRec.Reuse();
    }
    if (listBuf.size()) {
	exportSortedRun();
    }
    runStartPage.push_back(sortedRuns.GetLength()); // store one more page index to indicate the end of the last run

    // inputPipe.ShutDown();	// should be shut down by producer rather than consumer

    cout << "reading from input finished" << endl;

    // --- RUN MERGING ---
    // TODO use stl::priority queue to merge the runs and then output to the pipe and shutdown die
    priority_queue<RecordInRun, vector<RecordInRun>, RIRComp> pq(
	    RIRComp(sortOrder, true));
    int runCount = runStartPage.size() - 1, curRun = 0;
    while (curRun < runCount) {
	// TODO here is the case that get one from each run respectively thus sure to be more effective in this way
	sortedRuns.RemovePageHead(&curRec, runStartPage[curRun]);
	//	sortedRuns.GetPage(&pageBuf, runStartPage[curRun]); // TODO a more specified GetPageFirst()
	//	pageBuf.GetFirst(&curRec); // TODO store the first while exporting sorted runs
	//	sortedRuns.AddPage(&pageBuf, runStartPage[curRun]); // delete the head of each run every time we push it into heap

	// TODO sending in an object arguments then it will be copied and used and finally destoryed regardless it's a temporary obj or anything
	// TODO since the one destroyed is the one the function copied

	pq.push((RecordInRun) {
	    curRec, curRun
	});
	curRun++;
    }

    cout << "heap initialized" << endl;

    curRun--; // reset curRun index to the last run, i.e. the run we keep in the memory buffer now
    while (!pq.empty()) {
	curRec = pq.top().record;
	outputPipe.Insert(&curRec);
	curRun = pq.top().runIndex;
	pq.pop();
	if (sortedRuns.RemovePageHead(&curRec, runStartPage[curRun])) {

	    pq.push((RecordInRun) {
		curRec, curRun
	    });

	    //	curRun = pq.top().runIndex;
	    //	sortedRuns.GetPage(&pageBuf, runStartPage[curRun]); // TODO try to add page here (only when the page changes)  (effective when it's almost sorted / runs reside on different ranges but not sorted themselves)
	    //	//	}
	    //
	    //	// now the page is loaded correctly
	    //	pq.pop();
	    //	// TODO check the size each time to avoid loading of an empty page   OR   do one more GetFirst to utilize the return val
	    //	if (pageBuf.GetFirst(&curRec)) {
	    //	    sortedRuns.AddPage(&pageBuf, runStartPage[curRun]); // TODO here in AddPage() pageBuf is Reuse()ed thus we must re-populate the buffer regardless curRun==top().runIndex?
	    //
	    //	    pq.push((RecordInRun) {
	    //		curRec, curRun
	    //	    });
	} else { // no more records exist on this page
	    // move forward the start page to make next GetPage() on this run more convenient
	    if (runStartPage[curRun] + 1 == runStartPage[curRun + 1]) { // only one page for current run
		continue; // every record in this run has already been popped out
	    } else { // this run will not end after this page
		runStartPage[curRun]++;
		//		sortedRuns.GetPage(&pageBuf, runStartPage[curRun]);
		//		pageBuf.GetFirst(&curRec);
		//		sortedRuns.AddPage(&pageBuf, runStartPage[curRun]);
		sortedRuns.RemovePageHead(&curRec, runStartPage[curRun]);

		pq.push((RecordInRun) {
		    curRec, curRun
		});
	    }
	}
    }
    sortedRuns.Close();
    outputPipe.ShutDown(); // producing process finished and thus set the done flag
    cout << "writing to output finished" << endl;
    // TODO where to exit the thread?
    //	pthread_exit(NULL);
    return NULL;
}

void BigQ::exportSortedRun() {
    // TODO sort and write to temp file and record the page index
    runStartPage.push_back(sortedRuns.GetLength());
    listBuf.sort(RecComp(sortOrder, false));
    Schema *psn = new Schema("catalog", "nation");
    for (list<Record>::iterator it = listBuf.begin(); it != listBuf.end();
	    ++it) {
	if (!pageBuf.Append(&(*it))) {
	    sortedRuns.AddPage(&pageBuf, sortedRuns.GetLength());
	    pageBuf.Append(&(*it));
	}
    }
    if (pageBuf.GetLength()) { //TODO check here if it's correct
	sortedRuns.AddPage(&pageBuf, sortedRuns.GetLength());
    }
    listBuf.clear();
}
