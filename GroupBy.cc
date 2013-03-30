/* 
 * File:   GroupBy.cpp
 * Author: fateakong
 * 
 * Created on March 27, 2013, 6:16 PM
 */

#include <iostream>
#include "BigQ.h"
#include "GroupBy.h"

using namespace std;

//Attribute _IA = {"int", Int};
//Attribute _SA = {"string", String};
//Attribute _DA = {"double", Double};
//Schema s_sch("catalog", "supplier");
//Schema ps_sch("catalog", "partsupp");
//Attribute s_nationkey = {"s_nationkey", Int};
//Attribute ps_supplycost = {"ps_supplycost", Double};
//Attribute joinatt[] = {_IA, _SA, _SA, s_nationkey, _SA, _DA, _SA, _IA, _IA, _IA, ps_supplycost, _SA};
//Schema join_sch("join_sch", 12, joinatt);

GroupBy::GroupBy()
{
}

GroupBy::GroupBy(const GroupBy& orig)
{
}

GroupBy::~GroupBy()
{
}

void GroupBy::Run(Pipe &_inPipe, Pipe &_outPipe, OrderMaker &_groupOrder, Function &_func)
{
    inPipe = &_inPipe;
    outPipe = &_outPipe;
    groupOrder = &_groupOrder;
    func = &_func;
    Run();
}

void *GroupBy::workerFunc()
{
    Pipe sortedPipe(100);
    BigQ bigQ(*inPipe, sortedPipe, *groupOrder, nPage);
    Record groupRec, curRec;
    // see if the first record exists
    if (!sortedPipe.Remove(&curRec)) {
        cerr << "No records from input pipe!" << endl;
        outPipe->ShutDown();
        return NULL;
    }
    int retFuncInt, sumFuncInt;
    double retFuncDouble, sumFuncDouble;
    Type retFuncType;
    ComparisonEngine compEng;
    bool end = false;
    do {
        sumFuncInt = 0;
        sumFuncDouble = 0.0;
        groupRec.Copy(&curRec);
        //        outPipe->Insert(&curRec);
        do {
            retFuncInt = 0;
            retFuncDouble = 0.0;
            retFuncType = func->Apply(curRec, retFuncInt, retFuncDouble);
            sumFuncInt += retFuncInt;
            sumFuncDouble += retFuncDouble;
            curRec.Reuse();
            if (!sortedPipe.Remove(&curRec)) {
                end = true;
                break;
            }
        } while (compEng.Compare(&groupRec, &curRec, groupOrder) == 0);
        // the comparison result could only be <0 or ==0 since it's already sorted

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
    } while (!end);
    outPipe->ShutDown();
}
