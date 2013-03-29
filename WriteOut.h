/* 
 * File:   WriteOut.h
 * Author: fateakong
 *
 * Created on March 28, 2013, 12:24 PM
 */

#ifndef WRITEOUT_H
#define	WRITEOUT_H

#include <cstdio>
#include "Pipe.h"
#include "Schema.h"
#include "RelationalOp.h"


class WriteOut : public RelationalOp {
public:
    WriteOut();
    WriteOut(const WriteOut& orig);
    virtual ~WriteOut();
    void Run(Pipe &inPipe, FILE *outFile, Schema &mySchema);
private:
    Pipe *inPipe;
    FILE *outFile;
    Schema *schema;
    using RelationalOp::Run;
    virtual void *workerFunc();
};

#endif	/* WRITEOUT_H */

