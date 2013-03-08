CC = g++ -g -Wno-deprecated 

ifdef SystemRoot
   RM = del /Q
   tag = -i
else
   ifeq ($(shell uname), Linux)
      RM = rm -f
      tag = -n
   endif
endif

test3.out: Record.o Comparison.o ComparisonEngine.o Schema.o File.o GenericDBFile.o DBFile.o HeapFile.o SortedFile.o BigQ.o Pipe.o y.tab.o lex.yy.o test3.o
	$(CC) -o test.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o GenericDBFile.o DBFile.o HeapFile.o SortedFile.o BigQ.o Pipe.o y.tab.o lex.yy.o test3.o -lfl -lpthread

test2.out: Record.o Comparison.o ComparisonEngine.o Schema.o File.o GenericDBFile.o DBFile.o HeapFile.o SortedFile.o BigQ.o Pipe.o y.tab.o lex.yy.o test2.o
	$(CC) -o test.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o GenericDBFile.o DBFile.o HeapFile.o SortedFile.o BigQ.o Pipe.o y.tab.o lex.yy.o test2.o -lfl -lpthread
	
test1.out: Record.o Comparison.o ComparisonEngine.o Schema.o File.o GenericDBFile.o DBFile.o HeapFile.o SortedFile.o y.tab.o lex.yy.o test1.o
	$(CC) -o test.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o GenericDBFile.o DBFile.o HeapFile.o SortedFile.o y.tab.o lex.yy.o test1.o -lfl -lpthread
	
main: Record.o Comparison.o ComparisonEngine.o Schema.o File.o y.tab.o lex.yy.o main.o
	$(CC) -o main Record.o Comparison.o ComparisonEngine.o Schema.o File.o y.tab.o lex.yy.o main.o -lfl -lpthread
	
test1.o: test1.cc
	$(CC) -c test1.cc
	
test2.o: test2.cc
	$(CC) -c test2.cc

test3.o: test3.cc
	$(CC) -c test3.cc
	
main.o: main.cc
	$(CC) -c main.cc
	
Pipe.o: Pipe.cc
	$(CC) -c Pipe.cc

BigQ.o: BigQ.cc
	$(CC) -c BigQ.cc

Comparison.o: Comparison.cc
	$(CC) -c Comparison.cc
	
ComparisonEngine.o: ComparisonEngine.cc
	$(CC) -c ComparisonEngine.cc
	
HeapFile.o: HeapFile.cc
	$(CC) -c HeapFile.cc

SortedFile.o: SortedFile.cc
	$(CC) -c SortedFile.cc
	
GenericDBFile.o: GenericDBFile.cc
	$(CC) -c GenericDBFile.cc
	
DBFile.o: DBFile.cc
	$(CC) -c DBFile.cc

File.o: File.cc
	$(CC) -c File.cc

Record.o: Record.cc
	$(CC) -c Record.cc

Schema.o: Schema.cc
	$(CC) -c Schema.cc
	
y.tab.o: Parser.y
	yacc -d Parser.y -o y.tab.c
	sed $(tag) y.tab.c -e "s/  __attribute__ ((__unused__))$$/# ifndef __cplusplus\n  __attribute__ ((__unused__));\n# endif/" 
	g++ -c y.tab.c

lex.yy.o: Lexer.l
	lex  Lexer.l
	gcc  -c lex.yy.c

clean: 
	$(RM) *.o
	$(RM) *.out
	$(RM) y.tab.c
	$(RM) lex.yy.c
	$(RM) y.tab.h
	
cleanAll: clean
	$(RM) *.sr
	$(RM) *.bin*
	

all: Record.o Comparison.o ComparisonEngine.o Schema.o File.o y.tab.o lex.yy.o main.o
	$(CC) -o main Record.o Comparison.o ComparisonEngine.o Schema.o File.o y.tab.o lex.yy.o main.o -lfl