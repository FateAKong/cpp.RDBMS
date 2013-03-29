/* 
 * File:   Join.h
 * Author: fateakong
 *
 * Created on March 26, 2013, 7:55 PM
 */

#ifndef JOIN_H
#define	JOIN_H

#include "RelationalOp.h"
#include "Comparison.h"
#include "Pipe.h"
#include "Record.h"

class Join : public RelationalOp {
public:
    Join();
    Join(const Join& orig);
    virtual ~Join();
    void Run(Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF
            &selOp, Record &literal);
private:
    Pipe *inPipeL;
    Pipe *inPipeR;
    Pipe *outPipe;
    CNF *selOp;
    Record *literal;
    using RelationalOp::Run;
    virtual void *workerFunc();
};

#endif	/* JOIN_H */

