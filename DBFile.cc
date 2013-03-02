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
    MoveFirst();
    // TODO record the values of enum file type to determine this initialization
    type = heap;
    dirty = false;
    contdWrite = false; // TODO ?
    fetchedRead = false;
}

DBFile::~DBFile() {
    delete[] path;
    delete[] metapath;
}

int DBFile::Create(char *f_path, fType _type, void *startup) {
    char *strbuf = new char[25];
    strcpy(strbuf, f_path);
    strcpy(path, strbuf);
    strcpy(metapath, strcat(strbuf, ".meta"));
    type = _type;
    data.Open(0, path);

    FILE *meta = fopen(metapath, "wb");
    fwrite(&type, sizeof (int), 1, meta); // TODO write contdWrite and index
    fclose(meta);
    delete[] strbuf;
    return 1;
}

void DBFile::Load(Schema &f_schema, char *loadpath) {
    // TODO check if it is reading?
    Record addMe;
    FILE *bulk = fopen(loadpath, "r");
    while (addMe.SuckNextRecord(&f_schema, bulk) == 1) {
	Add(addMe);
    }
    fclose(bulk);
}

int DBFile::Open(char *f_path) {
    // TODO check if the dbfile is already in use if so close it first
    //	char *buf = (char *) malloc(sizeof(char) * 25);
    char *buf = new char[25];
    strcpy(buf, f_path);
    strcpy(path, buf);
    strcpy(metapath, strcat(buf, ".meta"));
    //	FILE *meta = fopen(strcat(path, ".meta"), "r");
    //	int *buf = (int*) malloc(sizeof(int));
    FILE *metafile = fopen(metapath, "rb");
    int metadata[3];
    fread(metadata, sizeof (int), 3, metafile);
    switch (metadata[0]) {
	case 0:
	    type = heap;
	    break;
	default:
	    break;
    }
    // index = {metadata[1], metadata[2]};
    fclose(metafile);
    MoveFirst();
    switch (type) {
	case heap:
	    data.Open(1, path);
	    break;
	case sorted:
	    break;
	case tree:
	    break;
	default:
	    break;
    }
    return 1;
}

void DBFile::MoveFirst() {
    index.page = 0;
    index.rec = 0;
}

int DBFile::Close() {
    // TODO write contdWrite???
    // TODO clean char* s
    // TODO only closes when file switching
    // TODO check if page is and write the dirty stuff and reset the attributes (store in metafile if needed)
    // maybe there were only read operations and no need to clean (dirty check included in Clean())
    printf("DBFile Closing.\n");
    Clean();
    int curLength = data.Close();
    FILE *meta = fopen(metapath, "rb+");
    printf("Index Value: %d %d\n", index.page, index.rec);
    fseek(meta, sizeof (int), SEEK_SET);
    fwrite(&index, sizeof (int), 2, meta);
    rewind(meta);
    int buf[3];
    fread(buf, sizeof (int), 3, meta);
    printf("Metafile: %d %d %d\n", buf[0], buf[1], buf[2]);
    fclose(meta);
    //	delete this;
    return 1;
}

// clean the nasty stufff! write dirty data in the buffer to disk
void DBFile::Clean() {
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

void DBFile::Add(Record &rec) {
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

int DBFile::GetNext(Record &fetchme) {
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

int DBFile::GetNext(Record &fetchme, CNF &cnf, Record &literal) {
    ComparisonEngine engine;
    do {
	if (!GetNext(fetchme)) {
	    return 0;
	}
    } while (engine.Compare(&fetchme, &literal, &cnf) == 0);
    return 1;
}
