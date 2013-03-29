/* 
 * File:   SelectPipe.h
 * Author: fateakong
 *
 * Created on March 25, 2013, 9:48 PM
 */

#ifndef SELECTPIPE_H
#define	SELECTPIPE_H

#include "RelationalOp.h"
#include "Pipe.h"
#include "Comparison.h"
#include "Record.h"

class SelectPipe : public RelationalOp {
public:
    SelectPipe();
    SelectPipe(const SelectPipe& orig);
    virtual ~SelectPipe();
    void Run(Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record
            &literal);
private:
    Pipe *inPipe;
    Pipe *outPipe;
    CNF *selOp;
    Record *literal;
    using RelationalOp::Run;
    virtual void *workerFunc();
//    static void *workerHelper(void *);
};

#endif	/* SELECTPIPE_H */

