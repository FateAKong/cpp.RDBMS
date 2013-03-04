#ifndef DBFILE_H
#define DBFILE_H

#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "BigQ.h"

typedef enum {
    heap, sorted, btree
} fType;

class GenericDBFile {
protected:
    char *path;
    void *startup;
    File data;
    Page buffer;
    bool dirty; // show if last op is a write or read indirectly
    // show if writing buffer is fetched from the last page of disk or a new one
    // used case: consecutive Add()s (e.g. Load()) generates new page to write
    // (opening(last page) differs from creating(new page), but not branched by contdWrite)
    // value only got in Clean(), and only set in invoking Add()
    bool contdWrite;
    // show if reading buffer is alreading postioned to current index page or not
    // used case: consecutive GetNext()s need not repositioning
    // value only got in GetNext(), and only set in Add() and GetNext()
    bool fetchedRead;
    // show if GetNext() could get back to the first position of the file when it meets the end
    // which is equivalent to the state of indexes' persistence 
    // i.e. true iff the indexes are stored in the metafile when closing
    // default value is false
    // bool cycleNext;

    struct INDEX {
        int page;
        int rec;
    } index; // read index  both from 0 to n-1
public:
    GenericDBFile();
    virtual ~GenericDBFile();
    virtual int Create(char *filePath, void *startup);
    virtual int Open(char *filePath);
    virtual int Close() = 0;
    virtual void Load(Schema &myschema, char *loadpath) = 0;

    void MoveFirst()
    {
        index.page = index.rec = 0;
    };
    virtual void Add(Record &addme) = 0;
    virtual int GetNext(Record &fetchme) = 0;
    virtual int GetNext(Record &fetchme, CNF &cnf, Record &literal) = 0;
    virtual void Clean() = 0;
};

class HeapFile : public GenericDBFile {
public:
    HeapFile();
    ~HeapFile();
    int Close();
    void Load(Schema &myschema, char *loadpath);
    void Add(Record &addme);
    int GetNext(Record &fetchme);
    int GetNext(Record &fetchme, CNF &cnf, Record &literal);
    void Clean();
};

class SortedFile : public GenericDBFile {
private:
    OrderMaker* order;
    BigQ *bigQ;
public:
    SortedFile();
    ~SortedFile();
    int Create(char *filePath, void *startup);
    int Close();
    void Load(Schema &myschema, char *loadpath);
    void Add(Record &addme);
    int GetNext(Record &fetchme);
    int GetNext(Record &fetchme, CNF &cnf, Record &literal);
    void Clean();
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

class DBFile {
private:
    GenericDBFile* myDBFile;

public:

    DBFile()
    {
        myDBFile = NULL;
    };

    ~DBFile()
    {
        delete myDBFile;
    };

    int Create(char *filePath, fType fileType, void *startup);

    int Open(char *filePath);

    int Close()
    {
        return myDBFile->Close();
    };

    void Load(Schema &myschema, char *loadpath)
    {
        myDBFile->Load(myschema, loadpath);
    };

    void MoveFirst()
    {
        myDBFile->MoveFirst();
    };

    void Add(Record &addme)
    {
        myDBFile->Add(addme);
    };

    int GetNext(Record &fetchme)
    {
        return myDBFile->GetNext(fetchme);
    };

    int GetNext(Record &fetchme, CNF &cnf, Record &literal)
    {
        return myDBFile->GetNext(fetchme, cnf, literal);
    };

    void Clean()
    {
        myDBFile->Clean();
    };
};

#endif
