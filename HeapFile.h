/* 
 * File:   HeapFile.h
 * Author: fateakong
 *
 * Created on March 6, 2013, 4:54 AM
 */

#ifndef HEAPFILE_H
#define	HEAPFILE_H

#include "Record.h"
#include "Schema.h"
#include "Comparison.h"
#include "GenericDBFile.h"

class HeapFile : public GenericDBFile {
public:
    HeapFile();
    HeapFile(const HeapFile& orig);
    using GenericDBFile::GetNext;
//    virtual int Create(char *filePath, void *metaData);
//    virtual int Open(char *filePath);
    virtual int GetNext(Record &fetchme, CNF &cnf, Record &literal);
    virtual void Add(Record &addme);
    virtual void Load(Schema &myschema, char *loadpath);
    virtual void Clean();
    virtual ~HeapFile();
};

#endif	/* HEAPFILE_H */

