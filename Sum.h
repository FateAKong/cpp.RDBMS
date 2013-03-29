/* 
 * File:   Sum.h
 * Author: fateakong
 *
 * Created on March 27, 2013, 7:54 PM
 */

#ifndef SUM_H
#define	SUM_H

#include "Pipe.h"
#include "Function.h"
#include "RelationalOp.h"

class Sum : public RelationalOp {
public:
    Sum();
    Sum(const Sum& orig);
    virtual ~Sum();
    void Run(Pipe &inPipe, Pipe &outPipe, Function &computeMe);
private:
    Pipe *inPipe;
    Pipe *outPipe;
    Function *func;
    using RelationalOp::Run;
    virtual void *workerFunc();
};

#endif	/* SUM_H */

