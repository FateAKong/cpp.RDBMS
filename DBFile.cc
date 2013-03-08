#include "DBFile.h"
#include "HeapFile.h"
#include "SortedFile.h"
#include <iostream>
#include <cstring>

// TODO multiple threads read and write   should this be applied in concurrency control

int DBFile::Create(char *filePath, fType fileType, void *startup)
{
    char *metaPath = new char[100];
    strcpy(metaPath, filePath);
    strcat(metaPath, ".meta");
    FILE *metaFile = fopen(metaPath, "wb");
    fwrite(&fileType, sizeof (int), 1, metaFile); // TODO write contdWrite and index
    delete[] metaPath;
    fclose(metaFile);
    switch (fileType) {
    case heap:
        myDBFile = new HeapFile();
        break;
    case sorted:
        myDBFile = new SortedFile();
        break;
    case btree:
        break;
    default:
        myDBFile = NULL;
        break;
    }
    if (myDBFile == NULL) {
        cerr << "Wrong MetaFile!" << endl;
        return 0;
    }
    return myDBFile->Create(filePath, startup);
}

int DBFile::Open(char *filePath)
{
    // TODO check if the dbfile is already in use if so close it first
    char metaPath[100];
    fType fileType;
    strcpy(metaPath, filePath);
    strcat(metaPath, ".meta");
    FILE *metaFile = fopen(metaPath, "rb");
    fread(&fileType, sizeof (int), 1, metaFile);
    fclose(metaFile);
    switch (fileType) {
    case heap:
        myDBFile = new HeapFile();
        break;
    case sorted:
        myDBFile = new SortedFile();
        break;
    case btree:
        //            myDBFile = new BTreeFile();
        break;
    default:
        myDBFile = NULL;
        break;
    }
    if (myDBFile == NULL) {
        cerr << "Wrong MetaFile!" << endl;
        return 0;
    }
    return myDBFile->Open(filePath);
}