/* 
 * File:   Project.h
 * Author: fateakong
 *
 * Created on March 26, 2013, 3:32 PM
 */

#ifndef PROJECT_H
#define	PROJECT_H

#include "Pipe.h"

#include "RelationalOp.h"

class Project : public RelationalOp {
public:
    Project();
    Project(const Project& orig);
    virtual ~Project();
    void Run(Pipe &inPipe, Pipe &outPipe, int *keepMe, int
            numAttsInput, int numAttsOutput);
private:
    Pipe *inPipe;
    Pipe *outPipe;
    int *keepMe;
    int numAttsInput;
    int numAttsOutput;
    using RelationalOp::Run;
    virtual void *workerFunc();

};

#endif	/* PROJECT_H */

