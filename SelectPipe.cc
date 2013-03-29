/* 
 * File:   SelectPipe.cc
 * Author: fateakong
 * 
 * Created on March 25, 2013, 9:48 PM
 */

#include <cstdlib>
#include <iostream>
#include <pthread.h>

#include "SelectPipe.h"

using namespace std;

SelectPipe::SelectPipe()
{
}

SelectPipe::SelectPipe(const SelectPipe& orig)
{
}

SelectPipe::~SelectPipe()
{
}

void SelectPipe::Run(Pipe &_inPipe, Pipe &_outPipe, CNF &_selOp, Record
        &_literal)
{
    inPipe = &_inPipe;
    outPipe = &_outPipe;
    selOp = &_selOp;
    literal = &_literal;
    Run();
}

void *SelectPipe::workerFunc()
{
    Record curRec;
    if (!inPipe->Remove(&curRec)) {
        cerr << "No records from input pipe!" << endl;
        outPipe->ShutDown();
        return NULL;
    }
    ComparisonEngine compEng;
    do {
        if (compEng.Compare(&curRec, &curRec, literal, selOp)) {
            outPipe->Insert(&curRec);
        }
        curRec.Reuse(); // this is necessary since Insert() might not be called
    } while (inPipe->Remove(&curRec));
    outPipe->ShutDown();
}