/* 
 * File:   GroupBy.h
 * Author: fateakong
 *
 * Created on March 27, 2013, 6:16 PM
 */

#ifndef GROUPBY_H
#define	GROUPBY_H

#include "Pipe.h"
#include "Comparison.h"
#include "Function.h"
#include "RelationalOp.h"


class GroupBy : public RelationalOp {
public:
    GroupBy();
    GroupBy(const GroupBy& orig);
    virtual ~GroupBy();
    void Run(Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe);
private:
    Pipe* inPipe;
    Pipe* outPipe;
    OrderMaker* groupOrder;
    Function* func;
    using RelationalOp::Run;
    virtual void *workerFunc();
};

#endif	/* GROUPBY_H */

