#ifndef DBFILE_H
#define DBFILE_H

#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"

typedef enum {
	heap, sorted, tree
} fType;

// stub DBFile header..replace it with your own DBFile.h

class DBFile {
private:
	File data;
	Page buffer;
	char *path;
	char *metapath;
	void *startup;
	bool dirty;		// show if last op is a write or read indirectly
	// show if writing buffer is fetched from the last page of disk or a new one
	// used case: consecutive Add()s (e.g. Load()) generates new page to write
	// (opening(last page) differs from creating(new page), but not branched by contdWrite)
	// value only got in Clean(), and only set in invoking Add()
	bool contdWrite;
	// show if reading buffer is alreading postioned to current index page or not
	// used case: consecutive GetNext()s need not repositioning
	// value only got in GetNext(), and only set in Add() and GetNext()
	bool fetchedRead;
	// show if GetNext() could get back to the first position of the file when it meets the end
	// which is equivalent to the state of indexes' persistence 
	// i.e. true iff the indexes are stored in the metafile when closing
	// default value is false
	// bool cycleNext;
	fType type;
//	enum STATE {
//		w, r
//	} state;
	struct INDEX {
		int page;
		int rec;
	} index;	// read index  both from 0 to n-1

public:
	DBFile();
	~DBFile();
	int Create(char *fpath, fType file_type, void *startup);
	int Open(char *fpath);
	int Close();
	void Load(Schema &myschema, char *loadpath);
	void MoveFirst();
	void Add(Record &addme);
	int GetNext(Record &fetchme);
	int GetNext(Record &fetchme, CNF &cnf, Record &literal);
	void Clean();
};
#endif
