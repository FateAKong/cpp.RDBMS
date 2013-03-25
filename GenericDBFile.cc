/* 
 * File:   GenericDBFile.cc
 * Author: fateakong
 * 
 * Created on March 6, 2013, 5:29 AM
 */

#include "GenericDBFile.h"
#include "Record.h"

#include <cstring>

GenericDBFile::GenericDBFile()
{
    // TODO record the values of enum file type to determine this initialization
    dirty = false;
    contdWrite = false; // TODO ?
    fetchedRead = false;

    path = new char[100];

    index.page = index.rec = 0;
}

GenericDBFile::GenericDBFile(const GenericDBFile& orig)
{
}

GenericDBFile::~GenericDBFile()
{
    delete[] path;
    // TODO destruct meta data by child class
    //    if (meta != NULL) {
    //        delete meta;
    //    }
}

int GenericDBFile::Create(char *_path, void *_meta)
{
    strcpy(path, _path);
    data.Open(0, path);
    return 1;
}

int GenericDBFile::Open(char *_path)
{
    strcpy(path, _path);
    // TODO change err handle in File.Open to return value instead of exit directly
    data.Open(1, path);
    return 1;
}

void GenericDBFile::MoveFirst()
{
    index.page = index.rec = 0;
}

int GenericDBFile::GetNext(Record &fetchme)
{
    // when just switched from writing to reading or just opened the file for reading
    // need to reposition to current index TODO any adjust after INDEX setting changes?
    if (!fetchedRead) {
        Clean();
        // TODO users should set the index by themselves with MoveFirst()
        // or it should go with the last index?
        data.GetPage(&buffer, index.page);
        for (int i = 0; i < index.rec; ++i) {
            buffer.GetFirst(&fetchme); // TODO free buffer!!!
        }
        fetchedRead = true;
    }
    // get next record
    //	while (buffer.GetLength() == 0) {
    while (buffer.GetFirst(&fetchme) == 0) {
        index.page++;
        index.rec = 0;
        if (index.page == data.GetLength()) {
            return 0;
        }
        data.GetPage(&buffer, index.page);
    }
    index.rec++; // do boundary check at next read by GetLength()
    return 1;
}

int GenericDBFile::Close()
{
    // TODO write contdWrite???
    // TODO clean char* s
    // TODO only closes when file switching
    // TODO check if page is and write the dirty stuff and reset the attributes (store in metafile if needed)
    // maybe there were only read operations and no need to clean (dirty check included in Clean())
    printf("DBFile Closing.\n");
    Clean();
    return data.Close();
    //	FILE *meta = fopen(metapath, "rb+");
    //	printf("Index Value: %d %d\n", index.page, index.rec);
    //	fseek(meta, sizeof (int), SEEK_SET);
    //	fwrite(&index, sizeof (int), 2, meta);
    //	rewind(meta);
    //	int buf[3];
    //	fread(buf, sizeof (int), 3, meta);
    //	printf("Metafile: %d %d %d\n", buf[0], buf[1], buf[2]);
    //	fclose(meta);
    //	delete this;
}

void GenericDBFile::Load(Schema &myschema, char *loadpath)
{
    Record addMe;
    FILE *bulk = fopen(loadpath, "r");
    while (addMe.SuckNextRecord(&myschema, bulk) == 1) {    
        Add(addMe);
    }
    fclose(bulk);
}
