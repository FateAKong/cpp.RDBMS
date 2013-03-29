/* 
 * File:   Sum.cc
 * Author: fateakong
 * 
 * Created on March 27, 2013, 7:54 PM
 */
#include <iostream>

#include "Sum.h"
#include "Defs.h"

using namespace std;

Sum::Sum()
{
}

Sum::Sum(const Sum& orig)
{
}

Sum::~Sum()
{
}

void Sum::Run(Pipe &_inPipe, Pipe &_outPipe, Function &_func)
{
    inPipe = &_inPipe;
    outPipe = &_outPipe;
    func = &_func;
    Run();
}

void *Sum::workerFunc()
{
    Record curRec;
    if (!inPipe->Remove(&curRec)) {
        cerr << "No records from input pipe!" << endl;
        outPipe->ShutDown();
        return NULL;
    }
    int retFuncInt = 0, sumFuncInt = 0;
    double retFuncDouble = 0.0, sumFuncDouble = 0.0;
    Type retFuncType;
    do {
        retFuncType = func->Apply(curRec, retFuncInt, retFuncDouble);
        sumFuncInt += retFuncInt;
        sumFuncDouble += retFuncDouble;
        curRec.Reuse();
    } while (inPipe->Remove(&curRec));
    // construct the result record and then output
    char *bits = NULL;
    int lenRecHead = 2 * sizeof (int), lenRec = lenRecHead;
    if (retFuncType == Int) {
        lenRec += sizeof (int);
        bits = new char[lenRec];
        *((int *) (&bits[lenRecHead])) = sumFuncInt;
    } else {
        lenRec += sizeof (double);
        bits = new char[lenRec];
        *((double *) (&bits[lenRecHead])) = sumFuncDouble;
    }
    ((int *) bits)[0] = lenRec;
    ((int *) bits)[1] = lenRecHead;
    Record retRec;
    retRec.SetBits(bits);
    outPipe->Insert(&retRec);
    outPipe->ShutDown();
}
