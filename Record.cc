#include "Record.h"

#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <iostream>

Record::Record()
{
    bits = NULL;
}

Record::~Record()
{
    if (bits != NULL) {
        delete [] bits;
    }
    bits = NULL;
}

int Record::SuckNextRecord(Schema *mySchema, FILE *textFile)
{
    // TODO bind a schema and textFile first and then suck suck
    // this is temporary storage
    char *space = new (std::nothrow) char[PAGE_SIZE];
    if (space == NULL) {
        cout << "ERROR : Not enough memory. EXIT !!!\n";
        exit(1);
    }

    char *recSpace = new (std::nothrow) char[PAGE_SIZE];
    if (recSpace == NULL) {
        cout << "ERROR : Not enough memory. EXIT !!!\n";
        exit(1);
    }

    // TODO Here is why your bits are cleared from memory: you didn't set it to NULL as Consume() did
    // clear out the present record
    if (bits != NULL)
        delete[] bits;
    bits = NULL;

    int n = mySchema->GetNumAtts();
    Attribute *atts = mySchema->GetAtts();

    // this is the current position (in bytes) in the binary
    // representation of the record that we are dealing with
    int currentPosInRec = sizeof (int) * (n + 1);

    // loop through all of the attributes
    for (int i = 0; i < n; i++) {

        // first we suck in the next attribute value
        int len = 0;
        while (1) {
            int nextChar = getc(textFile);
            if (nextChar == '|')
                break;
            else if (nextChar == EOF) {
                delete[] space;
                delete[] recSpace;
                return 0;
            }

            space[len] = nextChar;
            len++;
        }

        // set up the pointer to the current attribute in the record
        ((int *) recSpace)[i + 1] = currentPosInRec;

        // null terminate the string
        space[len] = 0;
        len++;

        // then we convert the data to the correct binary representation
        if (atts[i].myType == Int) {
            *((int *) &(recSpace[currentPosInRec])) = atoi(space);
            currentPosInRec += sizeof (int);

        } else if (atts[i].myType == Double) {

            // make sure that we are starting at a double-aligned position;
            // if not, then we put some extra space in there
            while (currentPosInRec % sizeof (double) != 0) {
                currentPosInRec += sizeof (int);
                ((int *) recSpace)[i + 1] = currentPosInRec;
            }

            *((double *) &(recSpace[currentPosInRec])) = atof(space);
            currentPosInRec += sizeof (double);

        } else if (atts[i].myType == String) {

            // align strings to the size of an integer if needed
            if (len % sizeof (int) != 0) {
                len += sizeof (int) - (len % sizeof (int));
            }

            strcpy(&(recSpace[currentPosInRec]), space);
            currentPosInRec += len;

        }

    }

    // the last thing is to set up the pointer to just past the end of the reocrd
    ((int *) recSpace)[0] = currentPosInRec;

    // and copy over the bits
    bits = new (std::nothrow) char[currentPosInRec];
    if (bits == NULL) {
        cout << "ERROR : Not enough memory. EXIT !!!\n";
        exit(1);
    }

    memcpy(bits, recSpace, currentPosInRec);

    delete[] space;
    delete[] recSpace;

    return 1;
}

int Record::SpitNextRecord(Schema *mySchema, FILE *textFile)
{
    // record is written into this already opened file's current pointer position

    if (bits == NULL)
        return 0;

    int n = mySchema->GetNumAtts();
    Attribute *atts = mySchema->GetAtts();

    // loop through all of the attributes
    for (int i = 0; i < n; i++) {
        char *curBytePtr = &bits[((int *) bits)[i + 1]];
        switch (atts[i].myType) {
        case Int:
            fprintf(textFile, "%d|", *((int *) curBytePtr));
            break;
        case Double:
            fprintf(textFile, "%f|", *((double *) curBytePtr));
            break;
        case String:
            fprintf(textFile, "%s|", curBytePtr);
            break;
        default:
            cerr << "Incorrect record format.\n";
            return 0;
            break;
        }
        fputc('\r', textFile);
    }
    return 1;
}

void Record::SetBits(char *bits)
{
    // TODO ?
    delete[] this->bits;
    this->bits = bits;
}

char* Record::GetBits(void)
{
    return bits;
}

void Record::CopyBits(char *bits, int b_len)
{
    if (this->bits != NULL) {
        delete[] this->bits;
    }
    this->bits = new (std::nothrow) char[b_len];
    if (this->bits == NULL) {
        cout << "ERROR : Not enough memory. EXIT !!!\n";
        exit(1);
    }

    memcpy(this->bits, bits, b_len);
}

int Record::GetLength()
{
    return ((int *) bits)[0];
}

// TODO change the memory operations in Record to avoid this OR change to a better name
// Reuse this record and keep the existing bits in memory for other use

void Record::Reuse()
{
    bits = NULL;
}

void Record::Consume(Record *fromMe)
{
    if (bits != NULL) {
        delete[] bits;
    }
    bits = fromMe->bits;
    fromMe->bits = NULL;
}

void Record::Copy(Record *copyMe)
{
    // this is a deep copy, so allocate the bits and move them over!
    if (bits != NULL) {
        delete[] bits;
        bits = NULL;
    }
    bits = new (std::nothrow) char[((int *) copyMe->bits)[0]];
    if (bits == NULL) {
        cout << "ERROR : Not enough memory. EXIT !!!\n";
        exit(1);
    }

    memcpy(bits, copyMe->bits, ((int *) copyMe->bits)[0]);

}

void Record::Project(int *attsToKeep, int numAttsToKeep, int numAttsNow)
{
    // TODO number of attributes now could be got w/o a schema so why provided by parameter
    // the correctness of padding is maintained since the record bits is margined to integer
    // TODO make another version for the case when attrs' relative pos not changed

    // first, figure out the size of the new record
    int totSpace = sizeof (int) * (numAttsToKeep + 1);

    for (int i = 0; i < numAttsToKeep; i++) {
        // if we are keeping the last record, be careful!
        if (attsToKeep[i] == numAttsNow - 1) {
            // in this case, take the length of the record and subtract the start pos
            totSpace += ((int *) bits)[0] - ((int *) bits)[attsToKeep[i] + 1];
        } else {
            // in this case, subtract the start of the next field from the start of this field
            totSpace += ((int *) bits)[attsToKeep[i] + 2]
                    - ((int *) bits)[attsToKeep[i] + 1];
        }
    }

    // now, allocate the new bits
    char *newBits = new (std::nothrow) char[totSpace];
    if (newBits == NULL) {
        cout << "ERROR : Not enough memory. EXIT !!!\n";
        exit(1);
    }

    // record the total length of the record
    *((int *) newBits) = totSpace;

    // and copy all of the fields over
    int curPos = sizeof (int) * (numAttsToKeep + 1);
    for (int i = 0; i < numAttsToKeep; i++) {
        // this is the length (in bytes) of the current attribute
        int attLen;

        // if we are keeping the last record, be careful!
        if (attsToKeep[i] == numAttsNow - 1) {
            // in this case, take the length of the record and subtract the start pos
            attLen = ((int *) bits)[0] - ((int *) bits)[attsToKeep[i] + 1];

        } else {
            // in this case, subtract the start of the next field from the start of this field
            attLen = ((int *) bits)[attsToKeep[i] + 2]
                    - ((int *) bits)[attsToKeep[i] + 1];
        }

        // set the start position of this field
        ((int *) newBits)[i + 1] = curPos;

        // and copy over the bits
        memcpy(&(newBits[curPos]), &(bits[((int *) bits)[attsToKeep[i] + 1]]),
                attLen);

        // note that we are moving along in the record
        curPos += attLen;
    }

    // kill the old bits
    delete[] bits;

    // and attach the new ones
    bits = newBits;

}

// consumes both the the left and the right record (deep copy in all cases)
// dividing line of l/r records comes from the parameter int startOfRight

void Record::MergeRecords(Record *left, Record *right, int numAttsLeft,
        int numAttsRight, int *attsToKeep, int numAttsToKeep,
        int startOfRight)
{
    if (bits != NULL) {
        delete[] bits;
        bits = NULL;
    }

    // if one of the records is empty, new record is non-empty record
    if (numAttsLeft == 0) {
        Copy(right);
        return;

    } else if (numAttsRight == 0) {
        Copy(left);
        return;
    }

    // first, figure out the size of the new record
    int totSpace = sizeof (int) * (numAttsToKeep + 1);

    int numAttsNow = numAttsLeft;
    char *rec_bits = left->bits;

    for (int i = 0; i < numAttsToKeep; i++) {
        if (i == startOfRight) {
            numAttsNow = numAttsRight;
            rec_bits = right->bits;
        }
        // if we are keeping the last record, be careful!
        if (attsToKeep[i] == numAttsNow - 1) {
            // in this case, take the length of the record and subtract the start pos
            totSpace += ((int *) rec_bits)[0]
                    - ((int *) rec_bits)[attsToKeep[i] + 1];
        } else {
            // in this case, subtract the start of the next field from the start of this field
            totSpace += ((int *) rec_bits)[attsToKeep[i] + 2]
                    - ((int *) rec_bits)[attsToKeep[i] + 1];
        }
    }

    // now, allocate the new bits
    bits = new (std::nothrow) char[totSpace + 1];
    if (bits == NULL) {
        cout << "ERROR : Not enough memory. EXIT !!!\n";
        exit(1);
    }

    // record the total length of the record
    *((int *) bits) = totSpace;

    numAttsNow = numAttsLeft;
    rec_bits = left->bits;

    // and copy all of the fields over
    int curPos = sizeof (int) * (numAttsToKeep + 1);
    for (int i = 0; i < numAttsToKeep; i++) {
        if (i == startOfRight) {
            numAttsNow = numAttsRight;
            rec_bits = right->bits;
        }

        // this is the length (in bytes) of the current attribute
        int attLen;

        // if we are keeping the last record, be careful!
        if (attsToKeep[i] == numAttsNow - 1) {
            // in this case, take the length of the record and subtract the start pos
            attLen = ((int *) rec_bits)[0]
                    - ((int *) rec_bits)[attsToKeep[i] + 1];
        } else {
            // in this case, subtract the start of the next field from the start of this field
            attLen = ((int *) rec_bits)[attsToKeep[i] + 2]
                    - ((int *) rec_bits)[attsToKeep[i] + 1];
        }

        // set the start position of this field
        ((int *) bits)[i + 1] = curPos;

        // and copy over the bits
        memmove(&(bits[curPos]),
                &(rec_bits[((int *) rec_bits)[attsToKeep[i] + 1]]), attLen);

        // note that we are moving along in the record
        curPos += attLen;

    }
}

int Record::getNumAttrs()
{
    return ((int*) bits)[1] / sizeof (int) - 1;
}

// TODO toString

string Record::ToString(Schema* mySchema)
{
    string ret;
    if (bits != NULL) {
        int n = mySchema->GetNumAtts();
        Attribute *atts = mySchema->GetAtts();

        // loop through all of the attributes
        for (int i = 0; i < n; i++) {

            // print the attribute name
            ret += atts[i].name;
            ret += ": ";

            // use the i^th slot at the head of the record to get the
            // offset to the correct attribute in the record
            int pointer = ((int *) bits)[i + 1];

            // here we determine the type, which given in the schema;
            // depending on the type we then print out the contents
            ret.append("[");
            char *buf = new char[50];
            // first is integer
            if (atts[i].myType == Int) {
                int *myInt = (int *) &(bits[pointer]);
                sprintf(buf, "%d", *myInt);
                ret += buf;
                // then is a double
            } else if (atts[i].myType == Double) {
                double *myDouble = (double *) &(bits[pointer]);
                sprintf(buf, "%f", *myDouble);
                ret += buf;

                // then is a character string
            } else if (atts[i].myType == String) {
                char *myString = (char *) &(bits[pointer]);
                ret.append(myString);
            }

            ret.append("]");
            // print out a comma as needed to make things pretty
            if (i != n - 1) {
                ret.append(", ");
            }
        }
    }
    return ret;
}

void Record::Print(Schema *mySchema)
{
    cout << ToString(mySchema) << endl;
}


