#ifndef STATISTICS_H
#define STATISTICS_H

#include <tr1/unordered_map>
#include <vector>
#include "ParseTree.h"

using namespace std;
using std::tr1::unordered_map;
using std::tr1::unordered_multimap;

typedef unordered_map<string, int> IntHash;
typedef unordered_map<string, IntHash> StrIntHash;
//typedef map<string, int> IntHash;
//typedef map<string, IntHash> StrIntHash;
//typedef map<int, int> IIHash;

class Statistics {
public:
    Statistics();
    Statistics(Statistics &copyMe); // Performs deep copy
    ~Statistics();

    void AddRel(char *relName, int numTuples);
    void AddAtt(char *relName, char *attName, int numDistincts);
    void CopyRel(char *oldName, char *newName);

    void Read(char *fromWhere);
    void Write(char *fromWhere);

    void Apply(struct AndList *parseTree, char *relNames[], int numToJoin);
    double Estimate(struct AndList *parseTree, char **relNames, int numToJoin);

private:
    // TODO how to make the two hashes corresponding? necessary to make a struct
    IntHash hashRelTupleCnt; // corresponding tuples' count of relations
    StrIntHash hashRelAttr; // corresponding attrs' hash of relations
    IntHash hashRelSetIdx; // corresponding subset's index of relations
    vector<int> vecSetRelCnt; // count of relations in a set
    vector<double> vecSetTupleCnt;
    bool getAttrInfo(string &attrName, char **relNames, int numToJoin, string& relName, int& nDist);
//    bool getAttrInfo(string attrName, char **relNames, int numToJoin, int &nDist);
    void clear();
};

#endif
