/* 
 * File:   Join.cc
 * Author: fateakong
 * 
 * Created on March 26, 2013, 7:55 PM
 */
#include <deque>
#include <iostream>
#include "Join.h"
#include "BigQ.h"

using namespace std;

Join::Join()
{
}

Join::Join(const Join& orig)
{
}

Join::~Join()
{
}

void Join::Run(Pipe &_inPipeL, Pipe &_inPipeR, Pipe &_outPipe, CNF
        &_selOp, Record &_literal)
{
    inPipeL = &_inPipeL;
    inPipeR = &_inPipeR;
    outPipe = &_outPipe;
    selOp = &_selOp;
    literal = &_literal;
    Run();
}

void *Join::workerFunc()
{
    OrderMaker orderL, orderR;
    int nAttrEqual = selOp->GetSortOrders(orderL, orderR);
    // based on whether CNF includes single (w/ repect to ANDs) equal attributes
    // do a sort-merge join if equal join satisfied
    // otherwise do a block nested loop join
    if (nAttrEqual > 0) { // equal join
        Pipe sortedPipeL(100), sortedPipeR(100);
        // TODO run length of BigQ should also be related to use_n_page()
        BigQ bigQL(*inPipeL, sortedPipeL, orderL, 3), bigQR(*inPipeR, sortedPipeR, orderR, 3);
        Record curRecL, curRecR;
        if (!sortedPipeL.Remove(&curRecL) || !sortedPipeR.Remove(&curRecR)) {
            cerr << "No records from input pipe!" << endl;
            outPipe->ShutDown();
            return NULL;
        }

        // TODO ??? no sophisticated caculation for MergeRecords() since we could make it w/ Project()
        int nAttrL = curRecL.getNumAttrs();
        int nAttrR = curRecR.getNumAttrs();
        int nAttrKeep = nAttrL + nAttrR;
        int arrAttrKeep[nAttrKeep];
        int iCurAttrKeep = 0;
        for (int i = 0; i < nAttrL; i++, iCurAttrKeep++) {
            arrAttrKeep[iCurAttrKeep] = i;
        }
        for (int i = 0; i < nAttrR; i++, iCurAttrKeep++) {
            arrAttrKeep[iCurAttrKeep] = i;
        }
        //        // caculate the various numbers and arrays for MergeRecords() use
        //        int nAttrL = curRecL.getNumAttrs();
        //        int nAttrR = curRecR.getNumAttrs();
        //        int nAttrKeep = nAttrL + nAttrR - nAttrEqual;
        //        int arrAttrKeep[nAttrKeep];
        //        int *arrAttrKeepL = new int[nAttrL];
        //        int *arrAttrKeepR = new int[nAttrR];
        //        for (int i = 0; i < nAttrEqual; i++) {
        //            arrAttrKeep[i] = orderL.whichAtts[i];
        //            arrAttrKeepL[orderL.whichAtts[i]] = -1;
        //            arrAttrKeepR[orderR.whichAtts[i]] = -1;
        //        }
        //        int iCurKeep = nAttrEqual;
        //        for (int i = 0; i < nAttrL; i++) {
        //            if (arrAttrKeepL[i] != -1) {
        //                arrAttrKeep[iCurKeep] = i;
        //                iCurKeep++;
        //            }
        //        }
        //        for (int i = 0; i < nAttrR; i++) {
        //            if (arrAttrKeepR[i] != -1) {
        //                arrAttrKeep[iCurKeep] = i;
        //                iCurKeep++;
        //            }
        //        }

        deque<Record> equalRecsL, equalRecsR;
        ComparisonEngine compEng;
        int compRet = compEng.Compare(&curRecL, &orderL, &curRecR, &orderR);
        bool end = false;
        do {
            if (compRet > 0) {
                do {
                    if (!sortedPipeR.Remove(&curRecR)) {
                        end = true;
                        break;
                    }
                    compRet = compEng.Compare(&curRecL, &orderL, &curRecR, &orderR);
                } while (compRet > 0);
            } else if (compRet < 0) {
                do {
                    if (!sortedPipeL.Remove(&curRecL)) {
                        end = true;
                        break;
                    }
                    compRet = compEng.Compare(&curRecL, &orderL, &curRecR, &orderR);
                } while (compRet < 0);
            } else { // now we get a pair of corresponding chunks with same equal join attrs
                Record nextRecL, nextRecR;
                do {
                    equalRecsL.push_back(curRecL);
                    if (!sortedPipeL.Remove(&nextRecL)) {
                        end = true;
                        break;
                    }
                    compRet = compEng.Compare(&curRecL, &nextRecL, &orderL);
                    curRecL.Reuse();
                    curRecL.Consume(&nextRecL);
                    //                    nextRecL.Reuse();
                } while (compRet == 0);

                do {
                    equalRecsR.push_back(curRecR);
                    if (!sortedPipeR.Remove(&nextRecR)) {
                        end = true;
                        break;
                    }
                    compRet = compEng.Compare(&curRecR, &nextRecR, &orderR);
                    curRecR.Reuse();
                    curRecR.Consume(&nextRecR);
                    //                    nextRecR.Reuse();
                } while (compRet == 0);

                Record joinRec;
                for (deque<Record>::iterator itL = equalRecsL.begin(); itL != equalRecsL.end(); itL++) {
                    for (deque<Record>::iterator itR = equalRecsR.begin(); itR != equalRecsR.end(); itR++) {
                        if (compEng.Compare(&(*itL), &(*itR), literal, selOp)) {
                            joinRec.MergeRecords(&(*itL), &(*itR), nAttrL, nAttrR, arrAttrKeep, nAttrKeep, nAttrL);
                            outPipe->Insert(&joinRec);
                        }
                    }
                }
                compRet = compEng.Compare(&curRecL, &orderL, &curRecR, &orderR);
                equalRecsL.clear();
                equalRecsR.clear();
                // TODO all the following temp records must have been set to null, either by Insert() or deque::clear()
                //                joinRec.Reuse(); 
                //                nextRecL.Reuse();
                //                nextRecR.Reuse();

                // DAMN IT
                //                bool endL = false, endR = false;
                //                do {
                //                    equalRetL.push_back(curRecL);
                //                    equalRetR.push_back(curRecR);
                //                    curRecL.Reuse();
                //                    curRecR.Reuse();
                //                    if (!sortedPipeL.Remove(&curRecL)) {
                //                        endL = true;
                //                    }
                //                    if (!sortedPipeR.Remove(&curRecR)) {
                //                        endR = true;
                //                    }
                //                    if (endL || endR) {
                //                        break;
                //                    }
                //                    compRet = compEng.Compare(&curRecL, &orderL, &curRecR, &orderR);
                //                } while (compRet == 0);
                //                if (compRet == 0) {
                //                    if (endL && !endR) {
                //                        do {
                //                            compRet = compEng.Compare(&curRecL, &orderL, &curRecR, &orderR);
                //                            if (compRet < 0) { // TODO !=0 is okay to verify
                //                                break;
                //                            }
                //                            // now it's still equal to 0
                //                            equalRetR.push_back(curRecR);
                //                            curRecR.Reuse();
                //                        } while (sortedPipeR.Remove(curRecR));
                //                    } else if (endR && !endL) {
                //                        do {
                //                            compRet = compEng.Compare(&curRecL, &orderL, &curRecR, &orderR);
                //                            if (compRet > 0) { // TODO !=0 is okay to verify
                //                                break;
                //                            }
                //                            // now it's still equal to 0
                //                            equalRetL.push_back(curRecL);
                //                            curRecL.Reuse();
                //                        } while (sortedPipeR.Remove(curRecL));
                //                    }
                //                    // else when endL&&endR nothing is needed to add into vectors 
                //                } else { // now compRet!=0
                //                    if (compRet < 0) { // right side steps into next value
                //
                //
                //                    } else {
                //                    }
                //
                //                }

            }
        } while (!end);
        curRecL.Reuse();
        curRecR.Reuse();
    } else { // TODO how to determine the max amount of memory we can use

    }
    outPipe->ShutDown();
}

