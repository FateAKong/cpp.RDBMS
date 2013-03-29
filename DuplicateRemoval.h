/* 
 * File:   DuplicateRemoval.h
 * Author: fateakong
 *
 * Created on March 27, 2013, 6:17 PM
 */

#ifndef DUPLICATEREMOVAL_H
#define	DUPLICATEREMOVAL_H

#include "RelationalOp.h"
#include "Pipe.h"
#include "Schema.h"

class DuplicateRemoval : public RelationalOp {
public:
    DuplicateRemoval();
    DuplicateRemoval(const DuplicateRemoval& orig);
    virtual ~DuplicateRemoval();
    void Run(Pipe &inPipe, Pipe &outPipe, Schema &mySchema);
private:
    Pipe *inPipe;
    Pipe *outPipe;
    Schema *schema;
    using RelationalOp::Run;
    virtual void *workerFunc();
};

#endif	/* DUPLICATEREMOVAL_H */

