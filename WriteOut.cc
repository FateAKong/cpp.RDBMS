/* 
 * File:   WriteOut.cc
 * Author: fateakong
 * 
 * Created on March 28, 2013, 12:24 PM
 */

#include <cstdio>
#include <iostream>
#include "WriteOut.h"

using namespace std;

WriteOut::WriteOut()
{
}

WriteOut::WriteOut(const WriteOut& orig)
{
}

WriteOut::~WriteOut()
{
}

void WriteOut::Run(Pipe &_inPipe, FILE *_outFile, Schema &_schema)
{
    inPipe = &_inPipe;
    outFile = _outFile;
    schema = &_schema;
    Run();
}

void *WriteOut::workerFunc()
{
    Record curRec;
    if (!inPipe->Remove(&curRec)) {
        cerr << "No records from input pipe!" << endl;
        fclose(outFile);
        return NULL;
    }
    do {
        if (!curRec.SpitNextRecord(schema, outFile)) {
            cerr << "Write out failed." << endl;
            fclose(outFile);
            return NULL;
        }
    } while (inPipe->Remove(&curRec));
    fclose(outFile);
}

