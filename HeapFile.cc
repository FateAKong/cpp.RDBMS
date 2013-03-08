/* 
 * File:   HeapFile.cc
 * Author: fateakong
 * 
 * Created on March 6, 2013, 4:54 AM
 */

#include "HeapFile.h"
#include "DBFile.h"
#include "Record.h"
#include "File.h"
#include "ComparisonEngine.h"
#include "BigQ.h"

HeapFile::HeapFile()
{
}

HeapFile::HeapFile(const HeapFile& orig)
{
}

HeapFile::~HeapFile()
{
}

void HeapFile::Load(Schema &f_schema, char *loadpath)
{
    // TODO check if it is reading?
    Record addMe;
    FILE *bulk = fopen(loadpath, "r");
    while (addMe.SuckNextRecord(&f_schema, bulk) == 1) {
        Add(addMe);
    }
    fclose(bulk);
}

// clean the nasty stufff! write dirty data in the buffer to disk

void HeapFile::Clean()
{
    if (dirty) {
        if (contdWrite) {
            data.AddPage(&buffer, data.GetLength() - 1);
        } else {
            data.AddPage(&buffer, data.GetLength());
        }
        dirty = false;
    }
}

void HeapFile::Add(Record &rec)
{
    if (!dirty) {
        // when just switched from reading to writing or just opened the file for writing
        // if in the process of reading, need to empty the buffer (done by data.GetPage())
        // when adding the first record, no pages exist on disk to fetch
        if (data.GetLength() > 0) {
            data.GetPage(&buffer, data.GetLength() - 1);
            contdWrite = true;
            fetchedRead = false;
        }
    }
    // in the process of writing, no need to fetch the last page to buffer
    if (buffer.Append(&rec) == 0) {
        // if buffer was almost full after fetchedRead from disk, then maybe no need to clean
        // so there is a dirty check inside Clean()
        Clean();
        contdWrite = false;
        // ignore the extreme circumstance when record size > page size
        buffer.Append(&rec);
    }
    dirty = true;

    // LAST VERSION
    //	if (buffer.Append(&rec) == 0 && dirty == true) {
    //		data.AddPage(&buffer, data.GetLength() - 1);
    //		buffer.EmptyItOut();
    //		dirty = false;
    //		buffer.Append(&rec);// ignore the extreme circumstance when record size > page size
    //	}
    //	dirty = true;
}

int HeapFile::GetNext(Record &fetchme, CNF &cnf, Record &literal)
{
    ComparisonEngine engine; // TODO set fetchedRead!!!
    do {
        if (!GetNext(fetchme)) {
            return 0;
        }
    } while (engine.Compare(&fetchme, &literal, &cnf) == 0);
    return 1;
}
