/* 
 * File:   SortedFile.h
 * Author: fateakong
 *
 * Created on March 6, 2013, 4:57 AM
 */

#ifndef SORTEDFILE_H
#define	SORTEDFILE_H

#include "Record.h"
#include "Schema.h"
#include "Comparison.h"
#include "BigQ.h"
#include "GenericDBFile.h"

typedef struct SORTMETA {
    OrderMaker *myOrder;
    int runLength;
} SortMeta;

class SortedFile : public GenericDBFile {
private:
    int runLen;
    OrderMaker sortOrder;
    OrderMaker queryOrder;
    OrderMaker queryLitOrder;
    BigQ *bigQ;
    Pipe *inPipe, *outPipe;
    bool initedQuery;
    int GetStartPageIndex(int start, int end, OrderMaker &queryOrder, OrderMaker &queryLitOrder, Record &literal);
    bool GetStartRec(Record *fetchMe, OrderMaker &queryOrder, OrderMaker &queryLitOrder, Record &literal);
    int MergeDiff(File &data, Record *addMe);
public:
    SortedFile();
    SortedFile(const SortedFile& orig);
    using GenericDBFile::GetNext;
    virtual int Create(char *_path, void *_meta);
    virtual int Open(char *_path);
    virtual int GetNext(Record &fetchme, CNF &cnf, Record &literal);
    virtual void Add(Record &addme);
    virtual void Load(Schema &myschema, char *loadpath);
    virtual int Close();
    virtual void Clean();
    virtual ~SortedFile();
};

#endif	/* SORTEDFILE_H */

