#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "Defs.h"
#include <iostream>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>

// stub file .. replace it with your own DBFile.cc
// TODO multiple threads read and write   should this be applied in concurrency control

DBFile::DBFile() {
    //	path = (char *) malloc(sizeof(char) * 25);
    //	metapath = (char *) malloc(sizeof(char) * 25);
    path = new char[25];
    metapath = new char[25];
    startup = NULL;
}

DBFile::~DBFile() {
    delete[] path;
    delete[] metapath;
}

int DBFile::Create(char *f_path, fType type, void *startup) {
    char *strbuf = new char[25];
    strcpy(strbuf, f_path);
    strcpy(path, strbuf);
    strcpy(metapath, strcat(strbuf, ".meta"));
    delete[] strbuf;
    data.Open(0, path);
    switch (type) {
	case heap:
	    myDBFile = new HeapFile(data);
	    break;
	case sorted:
	    myDBFile = new SortedFile(data);
	    break;
	case btree:
	    break;
	default:
	    break;
    }
    FILE *meta = fopen(metapath, "wb");
    fwrite(&type, sizeof (int), 1, meta); // TODO write contdWrite and index
    fclose(meta);
    return 1;
}

int DBFile::Open(char *f_path) {
    // TODO check if the dbfile is already in use if so close it first
    //	char *buf = (char *) malloc(sizeof(char) * 25);
    char *strbuf = new char[25];
    strcpy(strbuf, f_path);
    strcpy(path, strbuf);
    strcpy(metapath, strcat(strbuf, ".meta"));
    delete[] strbuf;
    data.Open(1, path);
    FILE *metafile = fopen(metapath, "rb");
    int metadata;
    fread(&metadata, sizeof (int), 1, metafile);
    fclose(metafile);
    switch (metadata) {
	case 0:
	    myDBFile = new HeapFile(data);
	    break;
	case 1:
	    myDBFile = new SortedFile(data);
	    break;
	case 2:
	    //			myDBFile = new BTreeFile();
	    break;
	default:
	    myDBFile = NULL;
	    break;
    }
    return 1;
}

GenericDBFile::GenericDBFile(File& _data, void* startup) : data(_data) {
    MoveFirst();
    // TODO record the values of enum file type to determine this initialization
    dirty = false;
    contdWrite = false; // TODO ?
    fetchedRead = false;
}

SortedFile::SortedFile(File& _data, void* startup) : GenericDBFile(_data, startup) {
}

int SortedFile::Close() {

};

void SortedFile::Load(Schema &myschema, char *loadpath) {

};

void SortedFile::MoveFirst() {

};

void SortedFile::Add(Record &addme) {

};

int SortedFile::GetNext(Record &fetchme) {

};

int SortedFile::GetNext(Record &fetchme, CNF &cnf, Record &literal) {

};

void SortedFile::Clean() {

};

HeapFile::HeapFile(File& _data) : GenericDBFile(_data) {
}

void HeapFile::Load(Schema &f_schema, char *loadpath) {
    // TODO check if it is reading?
    Record addMe;
    FILE *bulk = fopen(loadpath, "r");
    while (addMe.SuckNextRecord(&f_schema, bulk) == 1) {
	Add(addMe);
    }
    fclose(bulk);
}

int HeapFile::Close() {
    // TODO write contdWrite???
    // TODO clean char* s
    // TODO only closes when file switching
    // TODO check if page is and write the dirty stuff and reset the attributes (store in metafile if needed)
    // maybe there were only read operations and no need to clean (dirty check included in Clean())
    printf("DBFile Closing.\n");
    Clean();
    int curLength = data.Close();
    //	FILE *meta = fopen(metapath, "rb+");
    //	printf("Index Value: %d %d\n", index.page, index.rec);
    //	fseek(meta, sizeof (int), SEEK_SET);
    //	fwrite(&index, sizeof (int), 2, meta);
    //	rewind(meta);
    //	int buf[3];
    //	fread(buf, sizeof (int), 3, meta);
    //	printf("Metafile: %d %d %d\n", buf[0], buf[1], buf[2]);
    //	fclose(meta);
    //	delete this;
    return 1;
}

// clean the nasty stufff! write dirty data in the buffer to disk

void HeapFile::Clean() {
    if (dirty) {
	//		if (data.GetLength() > 0) {
	if (contdWrite) {
	    data.AddPage(&buffer, data.GetLength() - 1);
	} else {
	    data.AddPage(&buffer, data.GetLength());
	}
	//		} else {
	//			data.AddPage(&buffer, 0);
	//		}
	dirty = false;
    }
    //	if (dirty) {
    //		if (data.GetLength() > 0) {
    //			if (contdWrite) {
    //				data.AddPage(&buffer, data.GetLength() - 1);
    //			} else {
    //				data.AddPage(&buffer, data.GetLength());
    //			}
    //		} else {
    //			data.AddPage(&buffer, 0);
    //		}
    //		dirty = false;
    //	}
}

void HeapFile::Add(Record &rec) {
    if (!dirty) {
	// when just switched from reading to writing or just opened the file for writing
	// if in the process of reading, need to empty the buffer (done by data.GetPage())
	// when adding the first record, no pages exist on disk to fetch
	if (data.GetLength() > 0) {
	    data.GetPage(&buffer, data.GetLength() - 1);
	    contdWrite = true;
	    fetchedRead = false;
	}
    }
    // in the process of writing, no need to fetch the last page to buffer
    if (buffer.Append(&rec) == 0) {
	// if buffer was almost full after fetchedRead from disk, then maybe no need to clean
	// so there is a dirty check inside Clean()
	Clean();
	contdWrite = false;
	// ignore the extreme circumstance when record size > page size
	buffer.Append(&rec);
    }
    dirty = true;

    // LAST VERSION
    //	if (buffer.Append(&rec) == 0 && dirty == true) {
    //		data.AddPage(&buffer, data.GetLength() - 1);
    //		buffer.EmptyItOut();
    //		dirty = false;
    //		buffer.Append(&rec);// ignore the extreme circumstance when record size > page size
    //	}
    //	dirty = true;
}

int HeapFile::GetNext(Record &fetchme) {
    // when just switched from writing to reading or just opened the file for reading
    // need to reposition to current index TODO any adjust after INDEX setting changes?
    if (!fetchedRead) {
	Clean();
	data.GetPage(&buffer, index.page);
	for (int i = 0; i < index.rec - 1; ++i) {
	    buffer.GetFirst(&fetchme);
	}
	fetchedRead = true;
    }
    // get next record
    //	while (buffer.GetLength() == 0) {
    while (buffer.GetFirst(&fetchme) == 0) {
	index.page++;
	index.rec = 0;
	if (index.page == data.GetLength()) {
	    return 0;
	}
	data.GetPage(&buffer, index.page);
    }
    index.rec++; // do boundry check at next read by GetLength()
    return 1;

    //	if (!dirty) {
    //		if (buffer != NULL) {
    //		} else {
    //			// just switched from writing to reading or just opened the file
    //			buffer = new Page();
    //			data.GetPage(&buffer, index.page);
    //			for (int i = 0; i < index.rec - 1; ++i) {
    //				buffer.GetFirst(&fetchme);
    //			}
    //		}
    //	} else {
    //		if (contdWrite) {
    //			data.AddPage(&buffer, data.GetLength() - 2);
    //		} else {
    //			data.AddPage(&buffer, data.GetLength() - 1);
    //		}
    //		dirty = false;
    //		buffer.EmptyItOut();
    //		data.GetPage(&buffer, index.page);
    //		for (int i = 0; i < index.rec - 1; ++i) {
    //			buffer.GetFirst(&fetchme);
    //		}
    //	}

    // LAST VERSION
    //	if (buffer.GetLength() > 0) {
    //	} else {
    //		index.page++;
    //		index.rec = 0;
    //		if (index.page == data.GetLength() - 1) {
    //			index.page = 0;
    //		}
    //		data.GetPage(&buffer, index.page);
    //		buffer.GetFirst(&fetchme);
    //	}
    //	buffer.GetFirst(&fetchme);
    //	index.rec++;	// do boundry check at next read
}

int HeapFile::GetNext(Record &fetchme, CNF &cnf, Record &literal) {
    ComparisonEngine engine;
    do {
	if (!GetNext(fetchme)) {
	    return 0;
	}
    } while (engine.Compare(&fetchme, &literal, &cnf) == 0);
    return 1;
}
