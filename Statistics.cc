#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <vector>
#include <iostream>
#include <fstream>
#include "Statistics.h"
#include "Defs.h"
#include "Join.h"

using namespace std;
//using std::tr1::unordered_map;

Statistics::Statistics()
{
}

Statistics::Statistics(Statistics &copyMe)
{
    hashRelTupleCnt = copyMe.hashRelTupleCnt;
    hashRelAttr = copyMe.hashRelAttr;
}

Statistics::~Statistics()
{
    clear();
}

void Statistics::AddRel(char *relName, int numTuples)
{
    // TODO update means replace or increment?
    if (hashRelTupleCnt.count(relName) == 0) {
        hashRelTupleCnt.insert(make_pair(relName, numTuples));
        hashRelSetIdx.insert(make_pair(relName, vecSetRelCnt.size()));
        hashRelAttr.insert(make_pair(relName, IntHash()));
        vecSetRelCnt.push_back(1);
        // TODO do check to see if joined or not based on -1?
        // TODO necessary to have separate structure storing join result estimate?
        // TODO for the case of attr cnt, only # of join attr changes to smaller
        vecSetTupleCnt.push_back(numTuples);
    } else {
        if (vecSetRelCnt[hashRelSetIdx[relName]] > 1) {
            cerr << "Warning: updating relation in joined set!" << endl;
        }
        hashRelTupleCnt[relName] += numTuples;
    }
    /*
     * deprecated
    // operator[] always returns an reference (inserts an element when no match)
    hashRelTupleCnt[relName] = numTuples;
     */
}

void Statistics::AddAtt(char *relName, char *attName, int numDistincts)
{
    // corresponding relation must exist since numTuples is needed when numDistincts==-1
    if (hashRelTupleCnt.count(relName) == 0) {
        cerr << "correspoding relation should exist before add attributes!" << endl;
        exit(EXIT_FAILURE);
    }
    if (numDistincts == -1) {
        numDistincts = hashRelTupleCnt[relName];
    }
    if (vecSetRelCnt[hashRelSetIdx[relName]] > 1) {
        cerr << "Warning: adding attr to a relation in joined set!" << endl;
    }
    hashRelAttr[relName][attName] = numDistincts;

    /*  
     * deprecated
    if (hashRelAttr.count(relName) == 0) {
        IntHash hashAttrTupleCnt;
        hashAttrTupleCnt[attName] = numDistincts;
        hashRelAttr[relName] = hashAttrTupleCnt;
    } else {
        IntHash& hashAttrTupleCnt;
        hashAttrTupleCnt = hashRelAttr[relName];
        if (hashAttrTupleCnt.count(attName) == 0) {
            hashAttrTupleCnt[attName] = 
        } else {

        }
    }
     */
}

void Statistics::CopyRel(char *oldName, char *newName)
{
    if (strcmp(oldName, newName) == 0) return;
    if (!(hashRelTupleCnt.count(oldName) == 1 && hashRelTupleCnt.count(newName) == 0)) {
        cerr << "old relation doesn't exist or new relation already exists" << endl;
        exit(EXIT_FAILURE);
    }
    if (vecSetRelCnt[hashRelSetIdx[oldName]] > 1) {
        cerr << "Warning: copying relation in joined set!" << endl;
    }
    hashRelTupleCnt[newName] = hashRelTupleCnt[oldName];
    hashRelAttr[newName] = hashRelAttr[oldName];
    hashRelSetIdx.insert(make_pair(newName, vecSetRelCnt.size()));
    vecSetRelCnt.push_back(1);
    vecSetTupleCnt.push_back(hashRelTupleCnt[newName]);
    // TODO use iterator to eliminate multiple lookups
    //    IntHash::iterator itRelTupCntOld = hashRelTupleCnt.find(oldName);
    //    IntHash::iterator itRelTupCntNew = hashRelTupleCnt.find(newName);
    //    IntHash::iterator itRelSetIdxOld = hashRelSetIdx.find(oldName);
    //    IntHash::iterator itRelSetIdxNew = hashRelSetIdx.find(newName);
    //    StrIntHash::iterator itRelAttrOld = hashRelAttr.find(oldName);
    //    StrIntHash::iterator itRelAttrNew = hashRelAttr.find(newName);
}

void Statistics::clear()
{
    hashRelTupleCnt.clear();
    hashRelSetIdx.clear();
    hashRelAttr.clear();
    vecSetRelCnt.clear();
    vecSetTupleCnt.clear();
}

void Statistics::Read(char *fromWhere)
{
    // open output stream
    ifstream ifs(fromWhere);
    if (!ifs.is_open()) {
        cerr << "input file open failed!";
        exit(EXIT_FAILURE);
    }
    // clean current structures
    clear();

    // TODO file format check

    // input hash structures from specified file
    string strRelName, strAttrName;
    int nRelCnt, nRelTupleCnt, nRelSetIdx;
    int nAttrCnt, nAttrTupleCnt;
    ifs >> nRelCnt;
    for (int i = 0; i < nRelCnt; i++) {
        ifs >> strRelName >> nRelTupleCnt >> nRelSetIdx >> nAttrCnt;
        hashRelTupleCnt.insert(make_pair(strRelName, nRelTupleCnt));
        hashRelSetIdx.insert(make_pair(strRelName, nRelSetIdx));
        IntHash& hashAttrTupleCnt = hashRelAttr[strRelName];
        for (int j = 0; j < nAttrCnt; j++) {
            ifs >> strAttrName >> nAttrTupleCnt;
            hashAttrTupleCnt.insert(make_pair(strAttrName, nAttrTupleCnt));
        }
    }

    // input vector structures from specified file
    int nSetCnt, nSetRelCnt;
    double dblSetTupleCnt;
    ifs >> nSetCnt;
    for (int i = 0; i < nSetCnt; i++) {
        ifs >> nSetRelCnt >> dblSetTupleCnt;
        vecSetRelCnt.push_back(nSetRelCnt);
        vecSetTupleCnt.push_back(dblSetTupleCnt);
    }
    ifs.close();
}

void Statistics::Write(char *toWhere)
{
    // open output stream
    ofstream ofs(toWhere, ios_base::binary | ios_base::trunc);
    if (!ofs.is_open()) {
        cerr << "output file open failed!";
        exit(EXIT_FAILURE);
    }

    // output hash structures to specified file
    int nRelCnt = hashRelTupleCnt.size();
    if (nRelCnt != hashRelSetIdx.size() || nRelCnt != hashRelAttr.size()) {
        cerr << "hash structures do not accord with each other in relation count!";
        exit(EXIT_FAILURE);
    }
    ofs << nRelCnt;
    IntHash::iterator itRelTupleCnt = hashRelTupleCnt.begin();
    IntHash::iterator itRelSetIdx = hashRelSetIdx.begin();
    StrIntHash::iterator itRelAttr = hashRelAttr.begin();
    for (int i = 0; i < nRelCnt; i++, itRelTupleCnt++, itRelSetIdx++, itRelAttr++) {
        ofs << endl << itRelTupleCnt->first << " " << itRelTupleCnt->second << " " << itRelSetIdx->second << " ";
        IntHash hashAttrTupleCnt = itRelAttr->second;
        int nAttrCnt = hashAttrTupleCnt.size();
        ofs << nAttrCnt;
        IntHash::iterator itAttrTupleCnt = hashAttrTupleCnt.begin();
        for (int j = 0; j < nAttrCnt; j++, itAttrTupleCnt++) {
            ofs << endl << itAttrTupleCnt->first << " " << itAttrTupleCnt->second;
        }
    }
    ofs << endl << endl;

    // output vector structures to specified file
    int nSetCnt = vecSetRelCnt.size();
    if (nSetCnt != vecSetTupleCnt.size()) {
        cerr << "vector structures do not accord with each other in set count!";
        exit(EXIT_FAILURE);
    }
    ofs << nSetCnt;
    for (int i = 0; i < nSetCnt; i++) {
        ofs << endl << vecSetRelCnt[i] << " " << vecSetTupleCnt[i];
    }
    // close output stream
    ofs.close();
}

void Statistics::Apply(struct AndList *parseTree, char *relNames[], int numToJoin)
{
    // TODO check special case rules like logically FALSE   not here? should be in compiler

    // all the relations passed in are valid iff these relations:
    //  - are existing (already added) in current Statistics object
    //  - contain complete partitions (subsets)
    //     * multiple subsets included: estimate crossproduct first
    //       then times a factor based on join or select operations
    //       (join attrs should belong to different subsets)
    //     * single set included: crossproduct is automatically reduced to
    //       the estimated (may be exact when set contains one relation only)
    //       tuple count (i.e. join estimate)
    //       note that self join with two attrs belonging to same relation should
    //       be handled by invoker of Statistics class via copying/renaming

    IIHash hashSetRelCnt;
    int joinSetIdx;
    for (int i = 0; i < numToJoin; i++) {
        // check existence of every relation
        if (hashRelSetIdx.count(relNames[i]) == 0) {
            cerr << "some relation passed in is not existing" << endl;
            exit(EXIT_FAILURE);
        }

        // count relations for each set that is shared by relations
        int &curSetIdx = hashRelSetIdx[relNames[i]];
        if (hashSetRelCnt.count(curSetIdx) == 0) {
            hashSetRelCnt[curSetIdx] = 0;
        }
        hashSetRelCnt[curSetIdx]++;

        // collect all relations listed into the origin set of first relation
        // i.e. update hashRelSetIdx which stores set index for every involved relation
        if (i == 0) {
            joinSetIdx = curSetIdx;
        } else {
            curSetIdx = joinSetIdx;
        }
    }

    // iterate over the sets shared by the relations passed in
    IIHash::iterator it, itEnd = hashSetRelCnt.end();
    it = hashSetRelCnt.begin();
    while (it != itEnd) {
        // check if all the relations of this set are involved
        if (it->second < vecSetRelCnt[it->first]) {
            cerr << "relations passed in contain incomplete partitions" << endl;
            exit(EXIT_FAILURE);
        }
        it++;
    }
    double joinEst = 1.0;
    it = hashSetRelCnt.begin();
    while (it != itEnd) {
        // update vecSetRelCnt which stores relation count for every involved set
        // note that abandoned sets won't be removed to avoid complicated updates
        if (it->first != joinSetIdx) {
            vecSetRelCnt[joinSetIdx] += vecSetRelCnt[it->first];
        }
        // compute product of estimate # of tuples of involved sets
        joinEst *= vecSetTupleCnt[it->first];
        it++;
    }



    struct AndList *curAndList = parseTree;
    double dblAndFactor = 1.0;
    while (curAndList != NULL) {
        struct OrList *curOrList = curAndList->left;
        // TODO complete the checking job for that same attr appears in multiple OR exprs
        multimap<string, bool> hashAttrSelect; // solely handle non-independent OrList
        string strSameAttrName, strSameRelName;
        double nSameAttrTupleOrig; // keep record of numDist of first attr in case of same attrs
        bool hasJoin = false, hasSelect = false;
        double dblOrFactor = 1.0;
        int nOrComp = 0;
        while (curOrList != NULL) {
            ComparisonOp* curComp = curOrList->left;

            // check if attrs in parseTree only come from relations listed in param
            // meanwhile get the corresponding relation name and # of distinguish tuples
            string leftAttrName, rightAttrName;
            string leftRelName, rightRelName;
            int leftNumDist, rightNumDist;
            bool leftIsName = false, rightIsName = false;
            if (curComp->left->code == NAME) {
                leftIsName = true;
                leftAttrName = curComp->left->value;
                // TODO simplify getAttrInfo: return relName and no numDist
                if (!getAttrInfo(leftAttrName, relNames, numToJoin, leftRelName, leftNumDist)) {
                    cerr << "parseTree contains (left) attr not listed in relNames" << endl;
                    exit(EXIT_FAILURE);
                }
            }
            if (curComp->right->code == NAME) {
                rightIsName = true;
                rightAttrName = curComp->right->value;
                if (!getAttrInfo(rightAttrName, relNames, numToJoin, rightRelName, rightNumDist)) {
                    cerr << "parseTree contains (right) attr not listed in relNames" << endl;
                    exit(EXIT_FAILURE);
                }
            }

            // apply estimation rules based on different operations
            if (leftIsName && rightIsName) {
                if (hasSelect) {
                    cerr << "detected mix of join and select predicates in a single OrList! (no applicable rule for Apply() updates)" << endl;
                    //                    exit(EXIT_FAILURE);
                }
                if (hasJoin) {
                    cerr << "detected multiple join predicates in a single OrList! (no applicable rule for Apply() updates)" << endl;
                    //                    exit(EXIT_FAILURE);
                }
                if (leftRelName.compare(rightRelName) == 0) {
                    cerr << "detected self join which is not allowed and should be handled by invoker!" << endl;
                    exit(EXIT_FAILURE);
                }

                hasJoin = true;
                if (curComp->code == EQUALS) {// when equal/natural join
                    // TODO same name?
                    if (leftNumDist > rightNumDist) {
                        dblOrFactor *= 1 - 1.0 / leftNumDist;
                        hashRelAttr[leftRelName][leftAttrName] = rightNumDist;
                    } else {
                        dblOrFactor *= 1 - 1.0 / rightNumDist;
                        hashRelAttr[rightRelName][rightAttrName] = leftNumDist;
                    }
                } else { // when inequality join
                    dblOrFactor *= 1 - 1.0 / 3;
                    // TODO how do numDistincts update ?
                    cerr << "detected inequality join! (no applicable rule for Apply() updates)" << endl;
                }
            } else {
                string curOrAttrName = leftIsName ? leftAttrName : rightAttrName,
                        curOrRelName = leftIsName ? leftRelName : rightRelName;
                int curOrAttrNumDist = leftIsName ? leftNumDist : rightNumDist;
                if (hasSelect) {
                    cerr << "detected multiple select predicates in a single OrList! (not allowed in Apply() method)" << endl;
                    //                    exit(EXIT_FAILURE);
                }
                if (hasJoin) {
                    cerr << "detected mix of join and select predicates in a single OrList! (no applicable rule for Apply() updates)" << endl;
                    //                    exit(EXIT_FAILURE);
                }
                if (!hasSelect) {
                    hasSelect = true;
                    // for now it's uncertain if same attr appears multiple times
                    // but once it occurs, every attr is the same (as the first one)
                    nSameAttrTupleOrig = curOrAttrNumDist;
                    strSameAttrName = curOrAttrName;
                    strSameRelName = curOrRelName;
                }

                if (curComp->code == EQUALS) {// when equality comparison with constant
                    dblOrFactor *= 1 - 1.0 / curOrAttrNumDist;
                    hashRelAttr[curOrRelName][curOrAttrName] = 1;
                    // handle non-independent attributes in OrList
                    // i.e. same attr appears in multiple exprs of an OrList
                    // TODO deal with part of attrs are exactly the same attr
                    //      (right now only effective when all attrs are same
                    //       and no detection of any join in current OrList)
                    hashAttrSelect.insert(make_pair(curOrRelName + curOrAttrName, true));
                } else { // when inequality comparison with constant
                    dblOrFactor *= 1 - 1.0 / 3;
                    hashRelAttr[curOrRelName][curOrAttrName] = curOrAttrNumDist / 3;
                    hashAttrSelect.insert(make_pair(curOrRelName + curOrAttrName, false));
                }
            }
            nOrComp++;
            curOrList = curOrList->rightOr;
        }
        int nAttrSelect = hashAttrSelect.size();
        if (nOrComp > 1 && nOrComp == nAttrSelect &&
                hashAttrSelect.count(hashAttrSelect.begin()->first) == nAttrSelect) {
            double nSameAttrTuple = 0.0;
            for (multimap < string, bool>::iterator itAttrSelect = hashAttrSelect.begin();
                    itAttrSelect != hashAttrSelect.end(); itAttrSelect++) {
                if (itAttrSelect->second) {
                    nSameAttrTuple++;
                } else {
                    // TODO check relationship between equality and inequality exprs
                    //      it may cause more tuple count than original following this method
                    nSameAttrTuple += nSameAttrTupleOrig / 3;
                }
            }
            dblOrFactor = 1 - nSameAttrTuple / nSameAttrTupleOrig;
            hashRelAttr[strSameRelName][strSameAttrName] = nSameAttrTuple;
        }
        dblAndFactor *= 1 - dblOrFactor;
        curAndList = curAndList->rightAnd;
    }
    joinEst *= dblAndFactor;

    // update vecSetTupleCnt which stores tuple count for every involved set
    // note that abandoned sets won't be removed to avoid complicated updates
    vecSetTupleCnt[joinSetIdx] = joinEst;
}

// a simplified version of Apply() - no updates to data structures involved
// TODO make code more reusable between Estimate() and Apply()

double Statistics::Estimate(struct AndList *parseTree, char **relNames, int numToJoin)
{
    IIHash hashSetRelCnt;
    for (int i = 0; i < numToJoin; i++) {
        // check existence of every relation
        if (hashRelSetIdx.count(relNames[i]) == 0) {
            cerr << "some relation passed in is not existing" << endl;
            exit(EXIT_FAILURE);
        }

        // count relations for each set that is shared by relations
        int& curSetIdx = hashRelSetIdx[relNames[i]];
        if (hashSetRelCnt.count(curSetIdx) == 0) {
            hashSetRelCnt[curSetIdx] = 0;
        }
        hashSetRelCnt[curSetIdx]++;
    }

    // iterate over the sets shared by the relations passed in
    IIHash::iterator it = hashSetRelCnt.begin(), itEnd = hashSetRelCnt.end();
    double joinEst = 1.0;
    while (it != itEnd) {
        // check if all the relations of this set are involved
        if (it->second < vecSetRelCnt[it->first]) {
            cerr << "relations passed in contain incomplete partitions" << endl;
            exit(EXIT_FAILURE);
        }
        // compute product of estimate # of tuples of involved sets
        joinEst *= vecSetTupleCnt[it->first];
        it++;
    }

    struct AndList *curAndList = parseTree;
    double dblAndFactor = 1.0;
    while (curAndList != NULL) {
        struct OrList *curOrList = curAndList->left;
        multimap<string, bool> hashAttrSelect; // solely handle non-independent OrList
        int nSameAttrTupleOrig; // keep record of numDist of first attr in case of same attrs        bool hasJoin = false, hasSelect = false;
        bool hasJoin = false, hasSelect = false;
        double dblOrFactor = 1.0;
        int nOrComp = 0;
        while (curOrList != NULL) {
            ComparisonOp* curComp = curOrList->left;

            // check if attrs in parseTree only come from relations listed in param
            // meanwhile get the corresponding relation name and # of distinguish tuples
            string leftAttrName, rightAttrName;
            string leftRelName, rightRelName;
            int leftNumDist, rightNumDist;
            bool leftIsName = false, rightIsName = false;
            if (curComp->left->code == NAME) {
                leftIsName = true;
                leftAttrName = curComp->left->value;
                if (!getAttrInfo(leftAttrName, relNames, numToJoin, leftRelName, leftNumDist)) {
                    cerr << "parseTree contains (left) attr not listed in relNames" << endl;
                    exit(EXIT_FAILURE);
                }
            }
            if (curComp->right->code == NAME) {
                rightIsName = true;
                rightAttrName = curComp->right->value;
                if (!getAttrInfo(rightAttrName, relNames, numToJoin, rightRelName, rightNumDist)) {
                    cerr << "parseTree contains (right) attr not listed in relNames" << endl;
                    exit(EXIT_FAILURE);
                }
            }

            // apply estimation rules based on different operations
            if (leftIsName && rightIsName) {
                if (leftRelName.compare(rightRelName) == 0) {
                    cerr << "detected self join which is not allowed!" << endl;
                    exit(EXIT_FAILURE);
                }
                hasJoin = true;
                if (curComp->code == EQUALS) {// when equal/natural join
                    // TODO same name?
                    if (leftNumDist > rightNumDist) {
                        dblOrFactor *= 1 - 1.0 / leftNumDist;
                        //                        setAttrDistCnt(leftAttrName, leftRelName, rightNumDist);
                    } else {
                        dblOrFactor *= 1 - 1.0 / rightNumDist;
                    }
                } else { // when inequality join
                    dblOrFactor *= 1 - 1.0 / 3;
                    // TODO how do numDistincts update ?
                }
            } else {
                string curOrAttrName = leftIsName ? leftAttrName : rightAttrName,
                        curOrRelName = leftIsName ? leftRelName : rightRelName;
                int curOrAttrNumDist = leftIsName ? leftNumDist : rightNumDist;

                if (!hasSelect) {
                    hasSelect = true;
                    // for now it's uncertain if same attr appears multiple times
                    // but once it occurs, every attr is the same (as the first one)
                    nSameAttrTupleOrig = curOrAttrNumDist;
                }
                if (curComp->code == EQUALS) {// when equality comparison with constant
                    dblOrFactor *= 1 - 1.0 / curOrAttrNumDist;
                    hashAttrSelect.insert(make_pair(curOrRelName + curOrAttrName, true));
                } else { // when inequality comparison with constant
                    dblOrFactor *= 1 - 1.0 / 3;
                    hashAttrSelect.insert(make_pair(curOrRelName + curOrAttrName, false));
                }
            }
            nOrComp++;
            curOrList = curOrList->rightOr;
        }
        int nAttrSelect = hashAttrSelect.size();
        if (nOrComp > 1 && nOrComp == nAttrSelect &&
                hashAttrSelect.count(hashAttrSelect.begin()->first) == nAttrSelect) {
            int nSameAttrTuple = 0;
            for (multimap < string, bool>::iterator itAttrSelect = hashAttrSelect.begin();
                    itAttrSelect != hashAttrSelect.end(); itAttrSelect++) {
                if (itAttrSelect->second) {
                    nSameAttrTuple++;
                } else {
                    nSameAttrTuple += nSameAttrTupleOrig / 3;
                }
            }
            dblOrFactor = 1 - (double) nSameAttrTuple / nSameAttrTupleOrig;
        }
        dblAndFactor *= 1 - dblOrFactor;
        curAndList = curAndList->rightAnd;
    }
    joinEst *= dblAndFactor;
    return joinEst;
}

bool Statistics::getAttrInfo(string &attrName, char **relNames, int numToJoin, string &relName, int &nDist)
{
    int idxDot;
    if ((idxDot = attrName.find('.')) != string::npos) {
        relName = attrName.substr(0, idxDot);
        attrName = attrName.substr(idxDot + 1);
        // TODO check if relation name is not listed in relNames passed in
        //      not implemented yet since string comparison costs a lot
        //      this check is necessary when a wrong CNF gives a wrong relation name
        //      which happens to have an attr with same name as provided by CNF
        /* DEPRECATED no need to check solely if relation exists in Statistics class
         * it is duplicated w/ checking in relNames which are involved in current operation
                StrIntHash::iterator itRelAttr;
                if ((itRelAttr = hashRelAttr.find(relName)) != hashRelAttr.end()) {
         */
        IntHash hashAttrTupleCnt = hashRelAttr[relName];
        IntHash::iterator itAttrTupleCnt = hashAttrTupleCnt.find(attrName);
        if (itAttrTupleCnt != hashAttrTupleCnt.end()) {
            nDist = itAttrTupleCnt->second;
            return true;
        }
    } else {
        for (int i = 0; i < numToJoin; i++) {
            IntHash hashAttrTupleCnt = hashRelAttr[relNames[i]];
            IntHash::iterator itAttrTupleCnt = hashAttrTupleCnt.find(attrName);
            if (itAttrTupleCnt != hashAttrTupleCnt.end()) {
                nDist = itAttrTupleCnt->second;
                relName = relNames[i];
                return true;
            }
        }
    }
    return false;
}
/*
// version for Estimate()
bool Statistics::getAttrInfo(string attrName, char **relNames, int numToJoin, int &nDist)
{
        int idxDot;
    if ((idxDot = attrName.find('.')) != string::npos) {
        IntHash hashAttrTupleCnt = hashRelAttr[attrName.substr(0, idxDot)];
        IntHash::iterator itAttrTupleCnt = hashAttrTupleCnt.find(attrName.substr(idxDot + 1));
        if (itAttrTupleCnt != hashAttrTupleCnt.end()) {
            nDist = itAttrTupleCnt->second;
            return true;
        }
    } else {
        for (int i = 0; i < numToJoin; i++) {
            IntHash hashAttrTupleCnt = hashRelAttr[relNames[i]];
            IntHash::iterator itAttrTupleCnt = hashAttrTupleCnt.find(attrName);
            if (itAttrTupleCnt != hashAttrTupleCnt.end()) {
                nDist = itAttrTupleCnt->second;
                return true;
            }
        }
    }
    return false;
}
 */