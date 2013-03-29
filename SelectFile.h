/* 
 * File:   SelectFile.h
 * Author: fateakong
 *
 * Created on March 26, 2013, 12:07 AM
 */

#ifndef SELECTFILE_H
#define	SELECTFILE_H

#include "RelationalOp.h"
#include "DBFile.h"
#include "Pipe.h"
#include "Record.h"
#include "Comparison.h"

class SelectFile : public RelationalOp {
public:
    SelectFile();
    SelectFile(const SelectFile& orig);
    virtual ~SelectFile();
    void Run(DBFile &inFile, Pipe &outPipe, CNF &selOp, Record
            &literal);
private:
    DBFile *inFile;
    Pipe *outPipe;
    CNF *selOp;
    Record *literal;
    using RelationalOp::Run;
    virtual void *workerFunc();
};

#endif	/* SELECTFILE_H */

