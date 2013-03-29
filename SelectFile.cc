/* 
 * File:   SelectFile.cc
 * Author: fateakong
 * 
 * Created on March 26, 2013, 12:07 AM
 */

#include "SelectFile.h"
#include "DBFile.h"
#include "Pipe.h"

SelectFile::SelectFile()
{
}

SelectFile::SelectFile(const SelectFile& orig)
{
}

SelectFile::~SelectFile()
{
}

void SelectFile::Run(DBFile &_inFile, Pipe &_outPipe, CNF &_selOp, Record
        &_literal)
{
    inFile = &_inFile;
    outPipe = &_outPipe;
    selOp = &_selOp;
    literal = &_literal;
    Run();
}

void *SelectFile::workerFunc()
{
    Record curRec;
    while (inFile->GetNext(curRec, *selOp, *literal)) {
        outPipe->Insert(&curRec);
        //        temp->Reuse();        // TODO no need to call this since in Insert temp is consumed so that temp->bits is null already
    }
    outPipe->ShutDown();
}