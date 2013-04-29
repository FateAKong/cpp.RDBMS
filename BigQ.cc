/*
 * BigQ.cpp
 *
 *  Created on: Feb 13, 2013
 *      Author: Fate.AKong
 */

#include <cstdlib>
#include <iostream>
#include <pthread.h>

#include "BigQ.h"
#include "File.h"

using namespace std;

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

    int rc;
    rc = pthread_create(&workerThrd, NULL, workerHelper,
	    this);
    if (rc) {
	printf("ERROR; return code from pthread_create() is %d\n", rc);
	exit(-1);
    }
}

void *BigQ::workerHelper(void *context) {
    return ((BigQ *) context)->workerFunc();
}

void *BigQ::workerFunc() {

    // create a temp file as sorting buffer using pid as file name
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
    // read from input and once there is enough then sort
    Record curRec;
    vector<int> runStartPage;	// used record start index of each run on the way of reading input
    int curSizeInBytes = 0, curRecSize;

    while (inputPipe.Remove(&curRec)) {
	curRecSize = curRec.GetLength();
	if (curSizeInBytes + curRecSize > runLenInBytes) {
	    runStartPage.push_back(sortedRuns.GetLength());
	    exportSortedRun();
	    curSizeInBytes = 0;
	}
	listBuf.push_back(curRec);
	curSizeInBytes += curRecSize;
	curRec.Reuse();
    }
    if (listBuf.size()) {
	runStartPage.push_back(sortedRuns.GetLength());
	exportSortedRun();
    }
    runStartPage.push_back(sortedRuns.GetLength()); // store one more page index to indicate the end of the last run

    // inputPipe.ShutDown();	// should be shut down by producer rather than consumer

#ifdef DEBUG
    cout << "Run#\t" << "Page#" << endl;

    for (int i = 0; i < runStartPage.size(); i++) {
	cout << i << "\t" << runStartPage[i] << endl;
    }

    cout << "reading from input finished " << endl;
#endif

    // --- RUN MERGING ---
    // use stl::priority queue to merge the runs and then output to the pipe and shutdown die
    int runCount = runStartPage.size() - 1, curRun = 0;
    vector<int> runCurPage(runStartPage);
    priority_queue<RecordInRun, vector<RecordInRun>, RIRComp> pq(
	    RIRComp(sortOrder, true));
    Page inBuf[runCount]; // TODO double buffering
    while (curRun < runCount) { // TODO store the first page while exporting sorted runs and other pipelining scheme
	sortedRuns.GetPage(inBuf + curRun, runCurPage[curRun]);

	inBuf[curRun].GetFirst(&curRec);

	pq.push((RecordInRun) {
	    curRec, curRun
	});
	curRun++;
    }

#if defined DEBUG
    cout << "heap initialized" << endl;
    int cntRec = 6;
    cout << "Run#\t" << "Page#" << endl;
#endif

    while (!pq.empty()) {
	curRec = pq.top().record;
	outputPipe.Insert(&curRec);
	curRun = pq.top().runIndex;
	pq.pop();

	if (inBuf[curRun].GetFirst(&curRec)) {

	    pq.push((RecordInRun) {
		curRec, curRun
	    });
#ifdef DEBUG
	    cntRec++;
#endif
	} else { // load a new page
#ifdef DEBUG
	    cout << curRun << "\t" << runCurPage[curRun] << endl;
#endif
	    if (runCurPage[curRun] + 1 == runStartPage[curRun + 1]) {
		continue; // every record in this run has already been popped out
	    } else { // this run will not end after this page
		runCurPage[curRun] = runCurPage[curRun] + 1;
		sortedRuns.GetPage(inBuf + curRun, runCurPage[curRun]);
		inBuf[curRun].GetFirst(&curRec);

		pq.push((RecordInRun) {
		    curRec, curRun
		});
#ifdef DEBUG
		cntRec++;
#endif
	    }
	}
    }
#ifdef DEBUG
    cout << "writing " << cntRec << " recs to output finished " << endl;
#endif
    sortedRuns.Close();
    remove(fileName);
    //    if (ret != 0)
    //	perror("Error deleting file: errcode# " + ret);
    //    else
    //	puts("File successfully deleted");
    outputPipe.ShutDown(); // producing process finished and thus set the done flag
    return NULL;
}

void BigQ::exportSortedRun() {
    // sort and write to temp file
    listBuf.sort(RecComp(sortOrder, false));
    Page pageBuf;
    for (list<Record>::iterator it = listBuf.begin(); it != listBuf.end();
	    ++it) {
	if (!pageBuf.Append(&(*it))) {
	    sortedRuns.AddPage(&pageBuf, sortedRuns.GetLength());
	    pageBuf.Append(&(*it));
	}
    }
    if (pageBuf.GetLength()) {
	sortedRuns.AddPage(&pageBuf, sortedRuns.GetLength());
    }
    listBuf.clear();
}