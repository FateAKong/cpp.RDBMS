/* 
 * File:   SortedFile.cc
 * Author: fateakong
 * 
 * Created on March 6, 2013, 4:57 AM
 */

#include "SortedFile.h"
#include "DBFile.h"
#include "Record.h"
#include "File.h"
#include "ComparisonEngine.h"
#include "BigQ.h"
#include <assert.h>
#include <iostream>
#include <cstring>

SortedFile::SortedFile()
{
    runLen = 0;
    bigQ = NULL;
    inPipe = outPipe = NULL;
    initedQuery = false;
}

SortedFile::SortedFile(const SortedFile& orig)
{
}

SortedFile::~SortedFile()
{

}

int SortedFile::Create(char *_path, void *_meta)
{
    GenericDBFile::Create(_path, _meta);
    if (_meta == NULL) {
        cerr << "No meta data provided. Sorted file creation failed.";
        return 0;
    }
    SortMeta *meta = (SortMeta *) _meta;
    sortOrder = *(meta->myOrder);
    runLen = meta->runLength;
    if (runLen <= 0) {
        cerr << "Meta data is in wrong format. Sorted file creation failed.";
        return 0;
    }
    char *metaPath = new char[100];
    strcpy(metaPath, _path);
    strcat(metaPath, ".meta");
    FILE *metaFile = fopen(metaPath, "ab");
    //    fseek(metaFile, sizeof (int), SEEK_SET);
    fwrite(&(runLen), sizeof (int), 1, metaFile);
    fwrite(&(sortOrder.numAtts), sizeof (int), 1, metaFile);
    fwrite(sortOrder.whichAtts, sizeof (int), sortOrder.numAtts, metaFile);
    fwrite(sortOrder.whichTypes, sizeof (int), sortOrder.numAtts, metaFile);
    fclose(metaFile);
    return 1;
}

int SortedFile::Open(char *_path)
{
    GenericDBFile::Open(_path);
    char *metaPath = new char[100];
    strcpy(metaPath, _path);
    strcat(metaPath, ".meta");
    int *metaBuf = new int[1];
    FILE *metaFile = fopen(metaPath, "rb");
    fseek(metaFile, sizeof (int), SEEK_SET);
    fread(metaBuf, sizeof (int), 1, metaFile); // read runLength
    runLen = *metaBuf;
    if (runLen <= 0) {
        cerr << "Meta data is in wrong format. Sorted file opening failed.";
        return 0;
    }
    fread(metaBuf, sizeof (int), 1, metaFile); // read OrderMaker::numAtts
    sortOrder.numAtts = *metaBuf;
    delete metaBuf;
    metaBuf = new int[2 * sortOrder.numAtts];
    fread(metaBuf, sizeof (int), 2 * sortOrder.numAtts, metaFile);
    for (int i = 0; i < sortOrder.numAtts; i++) {
        sortOrder.whichAtts[i] = metaBuf[i];
        switch (metaBuf[sortOrder.numAtts + i]) {
        case Int:
            sortOrder.whichTypes[i] = Int;
            break;
        case Double:
            sortOrder.whichTypes[i] = Double;
            break;
        case String:
            sortOrder.whichTypes[i] = String;
            break;
        default:
            cerr << "Meta data is in wrong format. Sorted file opening failed.";
            return 0;
            break;
        }
    }
}

void SortedFile::Add(Record &addme)
{
    if (!dirty) {
        // TODO need to put pipes into diff threads? no output until input shutdown
        inPipe = new Pipe(100);
        outPipe = new Pipe(100);
        bigQ = new BigQ(*inPipe, *outPipe, sortOrder, runLen);
    }
    inPipe->Insert(&addme);
    dirty = true;
}

int SortedFile::MergeDiff(File &data, Record *addMe)
{
    if (addMe == NULL) {
        return 0;
    }
    if (buffer.Append(addMe) == 0) {
        data.AddPage(&buffer, data.GetLength());
        buffer.Append(addMe);
    }
    return 1;
}

void SortedFile::Clean()
{
    if (dirty) {
        inPipe->ShutDown();
        ComparisonEngine compEng;
        Record *diffRec = new Record(), *origRec = new Record();
        if (data.GetLength() == 0) { // first write to an empty DBFile
            while (outPipe->Remove(diffRec)) {
                MergeDiff(data, diffRec);
            }
            data.AddPage(&buffer, data.GetLength());
        } else { // there is some original data in the DBFile
            // see if original DBFile could still be used to store new data
            // i.e. every new record is bigger than the original ones)
            if (outPipe->Remove(diffRec) == 0) {
                return; // actually not applicable since dirty==true
            }
            data.GetPage(&buffer, data.GetLength() - 1);
            buffer.PeekLast(origRec);
            // compare first (smallest) in BigQ with last (biggest) in DBFile
            if (compEng.Compare(diffRec, origRec, &sortOrder) >= 0) {
                // now continue to write on the last page of original DBFile
                contdWrite = true;
                do {
                    if (buffer.Append(diffRec) == 0) {
                        data.AddPage(&buffer, data.GetLength() - 1);
                        contdWrite = false; // write to last page finished
                        break;
                    }
                } while (outPipe->Remove(diffRec));
                if (!contdWrite) { // write extra records to new pages of orginal DBFile
                    MergeDiff(data, diffRec); // the one intended to insert into buffer but failed
                    while (outPipe->Remove(diffRec)) {
                        MergeDiff(data, diffRec);
                    }
                    data.AddPage(&buffer, data.GetLength()); // write remaining data to original DBFile
                } else { // when contdWrite==true outPipe must be empty
                    data.AddPage(&buffer, data.GetLength() - 1); // update last page of original DBFile
                }
            } else {
                // make a two-way merge between BigQ and SortedFile
                // then write into a new file instead of the original DBFile
                File diffData;
                char diffPath[100];
                strcpy(diffPath, path);
                strcat(diffPath, ".diff");
                diffData.Open(1, diffPath);
                bool diffEnd; // false when original records end first and true otherwise
                MoveFirst();

                // now diffRec is already filled once, thus need to fill a origRec                
                GetNext(*origRec);
                while (true) {
                    if (compEng.Compare(diffRec, origRec, &sortOrder) < 0) {
                        MergeDiff(diffData, diffRec);
                        if (!outPipe->Remove(diffRec)) {
                            diffEnd = true;
                            break;
                        }
                    } else {
                        MergeDiff(diffData, origRec);
                        if (!GetNext(*origRec)) {
                            diffEnd = false;
                            break;
                        }
                    }
                }
                if (diffEnd) {
                    while (GetNext(*origRec)) {
                        MergeDiff(diffData, origRec);
                    }
                } else {
                    while (outPipe->Remove(diffRec)) {
                        MergeDiff(diffData, diffRec);
                    }
                }
                diffData.AddPage(&buffer, diffData.GetLength()); // write remaining data to new DBFile
            }
        }
        dirty = false;
    }

}

int SortedFile::GetStartPageIndex(int start, int end, OrderMaker &queryOrder, OrderMaker &queryLitOrder, Record &literal)
{
    assert(!(start < 0));
    assert(!(end > data.GetLength() - 1));
    if (start == end) {
        return start;
    }
    int mid = (start + end) / 2;
    data.GetPage(&buffer, mid);
    ComparisonEngine compEng;
    Record temp;
    buffer.PeekFirst(&temp);
    int retFront = compEng.Compare(&temp, &queryOrder, &literal, &queryLitOrder);
    buffer.PeekLast(&temp);
    int retBack = compEng.Compare(&temp, &queryOrder, &literal, &queryLitOrder);
    temp.Reuse(); // TODO why we need to reuse it? in your destructor you have set bits to NULL! why cant it work?!
    if (retFront == 1) { // retBack must be 1 since records are sorted
        return GetStartPageIndex(start, mid - 1, queryOrder, queryLitOrder, literal);
    } else if (retBack == -1) { // retFront must be -1 since records are sorted
        return GetStartPageIndex(mid + 1, end, queryOrder, queryLitOrder, literal);
    } else if (retFront == -1) { // retBack must >=0 since retBack==-1 branched
        return mid; // first record must lay on this page
    } else { // remaining scenario: retFront==0 && retBack>=0
        // need to pinpoint the first record since it may not lay on this page
        if (mid == 0) {
            return mid;
        }
        return GetStartPageIndex(mid - 1, mid, queryOrder, queryLitOrder, literal);
    }
}

// given that start page is already in the buffer
// try to find the record to start searching from
// return true if found a match and return as fetchMe, return false otherwise

bool SortedFile::GetStartRec(Record *fetchMe, OrderMaker &queryOrder, OrderMaker &queryLitOrder, Record &literal)
{
    bool found = false;
    ComparisonEngine compEng;
    Record *temp = new Record();
    while (buffer.GetFirst(temp) != 0) {
        index.rec++;
        int ret = compEng.Compare(temp, &queryOrder, &literal, &queryLitOrder);
        if (ret == -1) {
            delete temp;
            temp = new Record();
            continue;
        }
        if (ret == 1) {
            break;
        }
        // now ret==0
        found = true;
        fetchMe->Consume(temp);
        delete temp;
        break;
    }
    return found;
}

int SortedFile::GetNext(Record &fetchme, CNF &cnf, Record &literal)
{
    // TODO try to prove: !fetchedRead => !initedQuery
    // if the above expression works then no problem...
    //    if (!fetchedRead) {
    //        Clean(); // TODO new version needed
    //        fetchedRead = true;
    //    }
    ComparisonEngine compEng;
    Record *temp = new Record();
    if (!initedQuery) {
        // no matter whether there is a quick way to find the first matching
        // the start page:record should be loaded after query initialization
        //  1. w/o attributes to do quick search based on sorted order => linear search
        //  2. w/ ... and found first match either on this buffer page or by binary search
        //        => linear search from first match
        //  3. w/ ... but no matches found => no any search needed

        // loop through attributes used in sorted file OrderMaker
        // to find if matches any single (in ORList), equality w/ literal in CNF
        // construct a query OrderMaker based on the matching results
        for (int i = 0; i < sortOrder.numAtts; i++) {
            int j;
            for (j = 0; j < cnf.numAnds; j++) {
                if (cnf.orLens[j] != 1) {
                    continue;
                }
                Comparison c = cnf.orList[j][0];
                if (c.op != Equals || c.attType != sortOrder.whichTypes[i] ||
                        (c.operand1 != Literal && c.operand2 != Literal)) {
                    continue;
                }
                if ((c.operand1 == Literal && c.whichAtt2 == sortOrder.whichAtts[i]) ||
                        (c.operand2 == Literal && c.whichAtt1 == sortOrder.whichAtts[i])) {
                    // add into query OrderMaker TODO make a func in OrderMaker
                    queryOrder.whichTypes[queryOrder.numAtts] = queryLitOrder.whichTypes[queryLitOrder.numAtts] = sortOrder.whichTypes[i];
                    queryOrder.whichAtts[queryOrder.numAtts] = sortOrder.whichAtts[i];
                    queryLitOrder.whichAtts[queryLitOrder.numAtts] = c.operand1 == Literal ? c.whichAtt1 : c.whichAtt2;
                    queryOrder.numAtts++;
                    queryLitOrder.numAtts++;
                    // TODO what about same attribute in multiple subexp in CNF
                    // TODO clean all these stuff: queryOrder and queryLitIndex when quit
                    break;
                }
            }
            if (j == cnf.numAnds) { // no matches found for this attribute
                break;
            }
        }

        //        if (queryOrder.numAtts != 0) {
        // check if start point is on current page buffer
        // only applicable when we just switch from regular getNext()
        if (fetchedRead) { // now fetchedRead==true while initedQuery==false
            // TODO figure out the combination and transformation of fetchedRead and initedQuery
            // TODO NOT TESTED!
            if (queryOrder.numAtts != 0) {
                buffer.PeekFirst(temp);
                int retBuf = compEng.Compare(temp, &queryOrder, &literal, &queryLitOrder);
                if (retBuf == 1) {
                    return 0;
                } else if (retBuf == 0) {
                    temp->Reuse();
                    buffer.GetFirst(temp);
                } else if (retBuf == -1) {
                    buffer.PeekLast(temp);
                    if (compEng.Compare(temp, &queryOrder, &literal, &queryLitOrder) >= 0) {
                        temp->Reuse();
                        if (!GetStartRec(temp, queryOrder, queryLitOrder, literal)) {
                            return 0;
                        }
                    } else { // not in the buffer: retBufFront==retBufBack==-1
                        // the same way as general handling of binary search as show below
                        if (index.page == data.GetLength() - 1) {
                            return 0;
                        } else {
                            index.page = GetStartPageIndex(index.page + 1, data.GetLength() - 1, queryOrder, queryLitOrder, literal);
                            data.GetPage(&buffer, index.page);
                            index.rec = 0;

                            temp->Reuse();
                            if (!GetStartRec(temp, queryOrder, queryLitOrder, literal)) {
                                return 0;
                            }
                        }
                    }
                }
            } else { // when numAtts==0
                buffer.GetFirst(temp);
            }
        } else { // when fetchedRead==false
            // no buffer data existing, just switched from writing or just opend file
            // need to see whether buffer is with dirty writing data and if so clean the buffer
            Clean(); // TODO new version needed
            fetchedRead = true;
            if (queryOrder.numAtts != 0) {

                // binary search on pages to find the first equal one based on the queryOrder
                // three ways to compare the record with the literal:
                //  1. generate another OrderMaker for literal record
                //     then use Compare(Record, OrderMaker, Record, OrderMaker)
                //  2. generate an array of Comparison objects (actually the same as two OrderMakers)
                //     then use Run(Record, Record,　Comparison)
                //  3. use addLitToFile and suckNextRecord to generate a reference record with same schema as records in file
                //     then use Compare(Record, Record, OrderMaker) with queryOrder to judge
                // here choose the 1st way as it's better encapsulated than 2nd and w/o disk operation like 3rd
                // TODO now index.page should have been reset to 0 by MoveFirst()
                index.page = GetStartPageIndex(index.page, data.GetLength() - 1, queryOrder, queryLitOrder, literal);
                data.GetPage(&buffer, index.page);
                index.rec = 0;
                temp->Reuse();
                if (!GetStartRec(temp, queryOrder, queryLitOrder, literal)) {
                    return 0;
                }
            } else { // now numAtts==0
                data.GetPage(&buffer, index.page);
                for (int i = 0; i < index.rec; ++i) {
                    buffer.GetFirst(temp);
                    delete temp;
                    temp = new Record();
                }
                buffer.GetFirst(temp);
            }
        }
        initedQuery = true;
        if (compEng.Compare(temp, &literal, &cnf) == 1) {
            fetchme.Consume(temp);
            return 1;
        }
    }

    // TODO set Index!!! (if no quick way then from 0,0 otherwise continue reading)
    do {
        delete temp;
        temp = new Record();
        if (!GetNext(*temp)) {
            return 0;
        }
        if (queryOrder.numAtts > 0 &&
                compEng.Compare(temp, &queryOrder, &literal, &queryLitOrder) > 0) {
            return 0;
        }
    } while (compEng.Compare(temp, &literal, &cnf) == 0);
    fetchme.Consume(temp);
    return 1;
}