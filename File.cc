#include "File.h"

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <string.h>
#include <iostream>
#include <stdlib.h>
#include <unistd.h>

Page::Page()
{
    curSizeInBytes = sizeof (int);
    //	numRecs = 0;
    //	myRecs = new (std::nothrow) TwoWayList<Record>;
    //	if (myRecs == NULL) {
    //		cout << "ERROR : Not enough memory. EXIT !!!\n";
    //		exit(1);
    //	}
}

int Page::GetLength()
{
    return myRecs.size();
}

//

void Page::Reuse()
{

    //	// get rid of all of the records
    //	while (1) {
    //		Record temp;
    //		if (!GetFirst(&temp))
    //			break;
    //	}

    //	for (list<Record>::iterator it = myRecs.begin(); it != myRecs.end(); ++it) {
    //		if ((*it).GetBits()==NULL) {
    //			(*it).SetBits(NULL);
    //		}
    //	}
    myRecs.clear();

    // reset the page size
    curSizeInBytes = sizeof (int);
    //	numRecs = 0;
}

int Page::PeekFirst(Record *firstOne)
{
    if (myRecs.empty()) {
        return 0;
    }
    firstOne->bits = myRecs.front().GetBits();
    // TODO there is a possible memory leak! may cause duplicate free
    //      if firstOne if freed then front().bits is delete[]ed but not set to NULL!!!
    return 1;
}

int Page::PeekLast(Record *lastOne)
{
    if (myRecs.empty()) {
        return 0;
    }
    lastOne->bits = myRecs.back().GetBits();
    return 1;
}

int Page::GetFirst(Record *firstOne)
{

    //	// move to the first record
    //	myRecs->MoveToStart();
    //
    //	// make sure there is data
    //	if (!myRecs->RightLength()) {
    //		return 0;
    //	}

    if (myRecs.empty()) {
        return 0;
    }

    //	// and remove it
    //	myRecs->Remove(firstOne);
    //	numRecs--;
    //
    //	char *b = firstOne->GetBits();
    //	curSizeInBytes -= ((int *) b)[0];


    firstOne->bits = myRecs.front().GetBits();
    curSizeInBytes -= ((int *) firstOne->bits)[0];
    myRecs.front().Reuse();
    myRecs.erase(myRecs.begin());
    return 1;
}

int Page::Append(Record *addMe)
{
    char *b = addMe->GetBits();

    // first see if we can fit the record
    if (curSizeInBytes + ((int *) b)[0] > PAGE_SIZE) {
        return 0;
    }

    //	// move to the last record
    //	myRecs->MoveToFinish();
    //
    //	// and add it
    //	curSizeInBytes += ((int *) b)[0];
    //	myRecs->Insert(addMe);
    //	numRecs++;

    curSizeInBytes += ((int *) b)[0];
    myRecs.insert(myRecs.end(), *addMe);
    addMe->Reuse();
    return 1;
}

void Page::ToBinary(char *bits)
{

    // first write the number of records on the page
    //	((int *) bits)[0] = numRecs;

    ((int *) bits)[0] = myRecs.size();

    char *curPos = bits + sizeof (int);

    //	// and copy the records one-by-one
    //	myRecs->MoveToStart();
    //	for (int i = 0; i < numRecs; i++) {
    //		char *b = myRecs->Current(0)->GetBits();

    //		// copy over the bits of the current record
    //		memcpy(curPos, b, ((int *) b)[0]);
    //		curPos += ((int *) b)[0];
    //
    //		// and traverse the list
    //		myRecs->Advance();
    //	}

    for (list<Record>::iterator it = myRecs.begin(); it != myRecs.end(); ++it) {
        char *b = (*it).GetBits();
        memcpy(curPos, b, ((int *) b)[0]);
        curPos += ((int *) b)[0];
    }

}

void Page::FromBinary(char *bits)
{

    // first read the number of records on the page
    int numRecs = ((int *) bits)[0];

    // sanity check
    if (numRecs > 1000000 || numRecs < 0) {
        cerr << "This is probably an error.  Found " << numRecs
                << " records on a page.\n";
        exit(1);
    }

    // and now get the binary representations of each
    char *curPos = bits + sizeof (int);

    //	// first, empty out the list of current records
    //	myRecs->MoveToStart();
    //	while (myRecs->RightLength()) {
    //		Record temp;
    //		myRecs->Remove(&temp);
    //	}

    myRecs.clear();

    // now loop through and re-populate it
    Record *temp = new (std::nothrow) Record();
    if (temp == NULL) {
        cout << "ERROR : Not enough memory. EXIT !!!\n";
        exit(1);
    }

    curSizeInBytes = sizeof (int);
    for (int i = 0; i < numRecs; i++) {

        // get the length of the current record
        int len = ((int *) curPos)[0];
        curSizeInBytes += len;

        // create the record
        temp->CopyBits(curPos, len);

        //		// add it
        //		myRecs->Insert(temp);

        // TODO ? anything?
        myRecs.insert(myRecs.end(), *temp);
        temp->Reuse();

        //		// and move along
        //		myRecs->Advance();
        curPos += len;
    }

    delete temp;
}

File::File()
{
}

File::~File()
{
}

void File::GetPage(Page *putItHere, off_t whichPage)
{

    if (whichPage >= curLength) {
        cerr << "whichPage " << whichPage << " length " << curLength << endl;
        cerr << "BAD: you tried to read past the end of the file\n";
        exit(1);
    }

    // read in the specified page
    char *bits = new (std::nothrow) char[PAGE_SIZE];
    if (bits == NULL) {
        cout << "ERROR : Not enough memory. EXIT !!!\n";
        exit(1);

    }
    lseek(myFilDes, PAGE_SIZE * (whichPage + 1), SEEK_SET);
    read(myFilDes, bits, PAGE_SIZE);
    putItHere->FromBinary(bits);
    delete[] bits;

}

bool File::RemovePageHead(Record *putItHere, off_t whichPage)
{
    if (whichPage >= curLength) {
        cerr << "whichPage " << whichPage << " length " << curLength << endl;
        cerr << "BAD: you tried to read past the end of the file\n";
        exit(1);
    }
    lseek(myFilDes, PAGE_SIZE * (whichPage + 1), SEEK_SET);
    int numRecs;
    read(myFilDes, &numRecs, sizeof (int));
    if (numRecs-- == 0) {
        return false;
    }

    int recLen;
    read(myFilDes, &recLen, sizeof (int));
    lseek(myFilDes, PAGE_SIZE * (whichPage + 1) + sizeof (int), SEEK_SET);
    putItHere->bits = new char[recLen];
    read(myFilDes, putItHere->bits, recLen);
    int remLen = PAGE_SIZE - sizeof (int) -recLen;
    char *remBits = new char[remLen];
    read(myFilDes, remBits, remLen);
    lseek(myFilDes, PAGE_SIZE * (whichPage + 1), SEEK_SET);
    write(myFilDes, &numRecs, sizeof (int));
    write(myFilDes, remBits, remLen);
    return true;
}

// TODO necessary to destruct each rec in the pageBuffer?

void File::AddPage(Page *addMe, off_t whichPage)
{
    // because the first page has no actual record data but page count info
    // thus we make whichPage and curLength transparent to the user
    // so that we need to increment index by 1 when calculating write position

    // if we are trying to add past the end of file, then zero all the skipping pages
    if (whichPage >= curLength) {
        int foo = 0;
        for (off_t i = curLength; i < whichPage; i++) {
            lseek(myFilDes, PAGE_SIZE * (i + 1), SEEK_SET);
            write(myFilDes, &foo, sizeof (int));
        }
        // set the size
        curLength = whichPage + 1;
    }

    // now write the page
    char *bits = new (std::nothrow) char[PAGE_SIZE];
    if (bits == NULL) {
        cout << "ERROR : Not enough memory. EXIT !!!\n";
        exit(1);
    }

    addMe->ToBinary(bits);
    lseek(myFilDes, PAGE_SIZE * (whichPage + 1), SEEK_SET);
    write(myFilDes, bits, PAGE_SIZE);
    delete[] bits;
    addMe->Reuse();

    // LAST VERSION
    //	// this is because the first page has no data
    //	whichPage++;
    //
    //	// if we are trying to add past the end of the file, then
    //	// zero all of the pages out
    //	if (whichPage >= curLength) {
    //
    //		// do the zeroing
    //		for (off_t i = curLength; i < whichPage; i++) {
    //			int foo = 0;
    //			lseek(myFilDes, PAGE_SIZE * i, SEEK_SET);
    //			write(myFilDes, &foo, sizeof(int));
    //		}
    //
    //		// set the size
    //		curLength = whichPage + 1;// when adding 0th page we'll change curLength from 0 to 2
    //	}
    //
    //	// now write the page
    //	char *bits = new (std::nothrow) char[PAGE_SIZE];
    //	if (bits == NULL) {
    //		cout << "ERROR : Not enough memory. EXIT !!!\n";
    //		exit(1);
    //	}
    //
    //	addMe->ToBinary(bits);
    //	lseek(myFilDes, PAGE_SIZE * whichPage, SEEK_SET);
    //	write(myFilDes, bits, PAGE_SIZE);
    //	delete[] bits;

#ifdef F_DEBUG
    cerr << " File: curLength " << curLength << " whichPage " << whichPage << endl;
#endif

}

void File::Open(int fileLen, char *fName)
{

    // figure out the flags for the system open call
    int mode;
    if (fileLen == 0)
        mode = O_TRUNC | O_RDWR | O_CREAT;
    else
        mode = O_RDWR;

    // actually do the open
    myFilDes = open(fName, mode, S_IRUSR | S_IWUSR);

#ifdef verbose
    cout << "Opening file " << fName << " with " << curLength << " pages.\n";
#endif

    // see if there was an error
    if (myFilDes < 0) {
        cerr << "BAD!  Open did not work for " << fName << "\n";
        exit(1);
    }

    // read in the buffer if needed
    if (fileLen != 0) {

        // read in the first few bits, which is the page size
        lseek(myFilDes, 0, SEEK_SET);
        read(myFilDes, &curLength, sizeof (off_t));

    } else {
        curLength = 0;
    }

}

off_t File::GetLength()
{
    return curLength;
}

int File::Close()
{

    // write out the current length in pages
    lseek(myFilDes, 0, SEEK_SET);
    write(myFilDes, &curLength, sizeof (off_t));

    // close the file
    close(myFilDes);

    // and return the size
    return curLength;

}

