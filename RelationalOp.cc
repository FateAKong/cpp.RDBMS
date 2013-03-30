/* 
 * File:   RelationalOp.cc
 * Author: fateakong
 * 
 * Created on March 25, 2013, 8:11 PM
 */

#include <pthread.h>
#include <cstdlib>
#include <cstdio>

#include "RelationalOp.h"

RelationalOp::RelationalOp()
{
    nPage = 100;
}

RelationalOp::RelationalOp(const RelationalOp& orig)
{
}

RelationalOp::~RelationalOp()
{
}

void RelationalOp::Run()
{
    int rc;
    rc = pthread_create(&workerThrd, NULL, workerHelper,
            this);
    if (rc) {
        printf("ERROR; return code from pthread_create() is %d\n", rc);
        exit(-1);
    }
}

void RelationalOp::WaitUntilDone()
{
    // TODO handle ret value in operator subclasses
    void **ret = NULL;
    pthread_join(workerThrd, ret);
}

void *RelationalOp::workerHelper(void *context)
{
    return ((RelationalOp *) context)->workerFunc();
}

void RelationalOp::Use_n_Pages(int n)
{
    if (n > nPage) {
        nPage = n;
    }
}
