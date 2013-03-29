/* 
 * File:   DuplicateRemoval.cpp
 * Author: fateakong
 * 
 * Created on March 27, 2013, 6:17 PM
 */
#include <iostream>
#include "DuplicateRemoval.h"
#include "Comparison.h"
#include "BigQ.h"

DuplicateRemoval::DuplicateRemoval()
{
}

DuplicateRemoval::DuplicateRemoval(const DuplicateRemoval& orig)
{
}

DuplicateRemoval::~DuplicateRemoval()
{
}

void DuplicateRemoval::Run(Pipe &_inPipe, Pipe &_outPipe, Schema &_schema)
{
    inPipe = &_inPipe;
    outPipe = &_outPipe;
    schema = &_schema;
    Run();
}

void *DuplicateRemoval::workerFunc()
{
    OrderMaker dupOrder(schema);
    Pipe sortedPipe(100);
    BigQ bigQ(*inPipe, sortedPipe, dupOrder, nPage);
    Record dupRec, curRec;
    // see if the first record exists
    if (!sortedPipe.Remove(&curRec)) {
        cerr << "No records from input pipe!" << endl;
        outPipe->ShutDown();
        return NULL;
    }
    ComparisonEngine compEng;
    bool end = false;
    do {
        dupRec.Copy(&curRec);
        outPipe->Insert(&curRec);
        do {
            if (!sortedPipe.Remove(&curRec)) {
                end = true;
                break;
            }
        } while (compEng.Compare(&dupRec, &curRec, &dupOrder) == 0);
        // the comparison result could only be <0 or ==0 since it's already sorted
    } while (!end);
    outPipe->ShutDown();
}