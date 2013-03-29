/* 
 * File:   Project.cc
 * Author: fateakong
 * 
 * Created on March 26, 2013, 3:32 PM
 */

#include "Project.h"
#include "Record.h"

Project::Project()
{
}

Project::Project(const Project& orig)
{
}

Project::~Project()
{
}

void Project::Run(Pipe &_inPipe, Pipe &_outPipe, int *_keepMe, int
        _numAttsInput, int _numAttsOutput)
{
    inPipe = &_inPipe;
    outPipe = &_outPipe;
    keepMe = _keepMe;
    numAttsInput = _numAttsInput;
    numAttsOutput = _numAttsOutput;
    Run();
}

void *Project::workerFunc()
{
    Record *temp = new Record();
    while (inPipe->Remove(temp)) {
        temp->Project(keepMe, numAttsOutput, numAttsInput);
        outPipe->Insert(temp);
//        temp->Reuse();
    }
    outPipe->ShutDown();
    delete temp;
}

