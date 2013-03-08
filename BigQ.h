/*
 * BigQ.h
 *
 *  Created on: Feb 13, 2013
 *      Author: Fate.AKong
 */

#ifndef BIGQ_H_
#define BIGQ_H_

#include <list>
#include <vector>
#include <queue>
#include "Pipe.h"

class BigQ {
private:
    pthread_t workerThrd;
    void *workerFunc();
    static void *workerHelper(void *); // helper function to cast a member function to the type that pthread could use
    void exportSortedRun();

    Pipe& inputPipe;
    Pipe& outputPipe;
    OrderMaker& sortOrder;

    int runLenInBytes;
    list<Record> listBuf;
    File sortedRuns;
public:
    BigQ(Pipe &inputPipe, Pipe &outputPipe, OrderMaker &sortOrder, int runLength);
};

#endif /* BIGQ_H_ */
