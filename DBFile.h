#ifndef DBFILE_H
#define DBFILE_H

#include "GenericDBFile.h"
#include "Record.h"
#include "Schema.h"
#include "Comparison.h"


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
