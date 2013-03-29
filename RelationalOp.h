/* 
 * File:   RelationalOp.h
 * Author: fateakong
 *
 * Created on March 25, 2013, 8:11 PM
 */

#ifndef RELATIONALOP_H
#define	RELATIONALOP_H

#include <pthread.h>

class RelationalOp {
public:
    RelationalOp();
    RelationalOp(const RelationalOp& orig);
    virtual ~RelationalOp();

    // blocks the caller until the particular relational operator
    // has run to completion
    virtual void WaitUntilDone();
    // tells how much internal memory the operation can use
    virtual void Use_n_Pages(int n);
protected:
    pthread_t workerThrd;
    int nPage;
    void Run();
    static void *workerHelper(void *);
    virtual void *workerFunc() = 0;
};

#endif	/* RELATIONALOP_H */

