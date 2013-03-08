/* 
 * File:   GenericDBFile.h
 * Author: fateakong
 *
 * Created on March 6, 2013, 5:29 AM
 */

#ifndef GENERICDBFILE_H
#define	GENERICDBFILE_H

#include "Record.h"
#include "Schema.h"
#include "Comparison.h"

typedef enum {
    heap, sorted, btree
} fType;

class GenericDBFile {
protected:
    char *path;
    File data;
    Page buffer;
    bool dirty; // Show if last op is a write or read indirectly
    // Show if writing buffer is fetched from the last page of disk or a new one
    // Applied case: consecutive Add()s (e.g. Load()) generate new page to write
    // (opening(last page) differs from creating(new page), but not branched by contdWrite)
    // value only got in Clean(), and only set in invoking Add()
    bool contdWrite;
    // Show if reading buffer is already positioned to current index page or not
    // Applied case: consecutive GetNext()s need not repositioning
    // value only got in GetNext(), and only set in Add() and GetNext()
    bool fetchedRead;
    // Show if GetNext() could get back to the first position of the file when it meets the end
    // which is equivalent to the state of indexes' persistence 
    // i.e. true iff the indexes are stored in the metafile when closing
    // default value is false
    // bool cycleNext;

    struct INDEX {
        int page;
        int rec;
    } index; // read index  both from 0 to n-1public:
public:
    GenericDBFile();
    GenericDBFile(const GenericDBFile& orig);
    void MoveFirst();
    int GetNext(Record &fetchme);
    int Close();
    virtual int Create(char *_path, void *_meta);
    virtual int Open(char *_path);
    virtual int GetNext(Record &fetchme, CNF &cnf, Record &literal) = 0;
    virtual void Add(Record &addme) = 0;
    virtual void Load(Schema &myschema, char *loadpath) = 0;
    virtual void Clean() = 0;
    virtual ~GenericDBFile();
};

/*
class BTreeFile :protected GenericDBFile {
        BTreeFile(char* f_path, void* startup = NULL);
        int Close();
        void Load(Schema &myschema, char *loadpath);
        void MoveFirst();
        void Add(Record &addme);
        int GetNext(Record &fetchme);
        int GetNext(Record &fetchme, CNF &cnf, Record &literal);
        void Clean();
};
 */

#endif	/* GENERICDBFILE_H */

