CC = g++ -g -Wno-deprecated -std=c++0x
#   CC = g++ -O2

ifdef SystemRoot
   RM = del /Q
   tag = -i
else
   ifeq ($(shell uname), Linux)
      RM = rm -f
      tag = -n
   endif
endif

test41.out: Record.o Comparison.o ComparisonEngine.o Schema.o File.o GenericDBFile.o DBFile.o HeapFile.o SortedFile.o BigQ.o Pipe.o Statistics.o y.tab.o lex.yy.o test41.o
	$(CC) -o test.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o GenericDBFile.o DBFile.o HeapFile.o SortedFile.o BigQ.o Pipe.o Statistics.o y.tab.o lex.yy.o test41.o -lfl -lpthread

test3.out: Record.o Comparison.o ComparisonEngine.o Schema.o File.o GenericDBFile.o DBFile.o HeapFile.o SortedFile.o BigQ.o Pipe.o RelationalOp.o SelectPipe.o SelectFile.o Project.o Join.o DuplicateRemoval.o Sum.o GroupBy.o WriteOut.o Function.o y.tab.o lex.yy.o yyfunc.tab.o lex.yyfunc.o test3.o
	$(CC) -o test.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o GenericDBFile.o DBFile.o HeapFile.o SortedFile.o BigQ.o Pipe.o RelationalOp.o SelectPipe.o SelectFile.o Project.o Join.o DuplicateRemoval.o Sum.o GroupBy.o WriteOut.o Function.o y.tab.o lex.yy.o yyfunc.tab.o lex.yyfunc.o test3.o -lfl -lpthread

test22.out: Record.o Comparison.o ComparisonEngine.o Schema.o File.o GenericDBFile.o DBFile.o HeapFile.o SortedFile.o BigQ.o Pipe.o y.tab.o lex.yy.o test22.o
	$(CC) -o test.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o GenericDBFile.o DBFile.o HeapFile.o SortedFile.o BigQ.o Pipe.o y.tab.o lex.yy.o test22.o -lfl -lpthread

test21.out: Record.o Comparison.o ComparisonEngine.o Schema.o File.o GenericDBFile.o DBFile.o HeapFile.o SortedFile.o BigQ.o Pipe.o y.tab.o lex.yy.o test21.o
	$(CC) -o test.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o GenericDBFile.o DBFile.o HeapFile.o SortedFile.o BigQ.o Pipe.o y.tab.o lex.yy.o test21.o -lfl -lpthread
	
test1.out: Record.o Comparison.o ComparisonEngine.o Schema.o File.o GenericDBFile.o DBFile.o HeapFile.o SortedFile.o BigQ.o Pipe.o y.tab.o lex.yy.o test1.o
	$(CC) -o test.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o GenericDBFile.o DBFile.o HeapFile.o SortedFile.o BigQ.o Pipe.o y.tab.o lex.yy.o test1.o -lfl -lpthread
	
main: Record.o Comparison.o ComparisonEngine.o Schema.o File.o y.tab.o lex.yy.o main.o
	$(CC) -o main Record.o Comparison.o ComparisonEngine.o Schema.o File.o y.tab.o lex.yy.o main.o -lfl -lpthread
	
test1.o: test1.cc
	$(CC) -c test1.cc
	
test21.o: test21.cc
	$(CC) -c test21.cc

test22.o: test22.cc
	$(CC) -c test22.cc
	
test3.o: test3.cc
	$(CC) -c test3.cc

test41.o: test41.cc
	$(CC) -c test41.cc
	
main.o: main.cc
	$(CC) -c main.cc

Statistics.o: Statistics.cc
	$(CC) -c Statistics.cc
	
RelationalOp.o: RelationalOp.cc
	$(CC) -c RelationalOp.cc

SelectPipe.o: SelectPipe.cc
	$(CC) -c SelectPipe.cc
	
SelectFile.o: SelectFile.cc
	$(CC) -c SelectFile.cc

DuplicateRemoval.o: DuplicateRemoval.cc
	$(CC) -c DuplicateRemoval.cc

Project.o: Project.cc
	$(CC) -c Project.cc
	
Join.o: Join.cc
	$(CC) -c Join.cc
	
Function.o: Function.cc
	$(CC) -c Function.cc

Sum.o: Sum.cc
	$(CC) -c Sum.cc
	
GroupBy.o: GroupBy.cc
	$(CC) -c GroupBy.cc
	
WriteOut.o: WriteOut.cc
	$(CC) -c WriteOut.cc

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
	yacc -d Parser.y
	sed $(tag) y.tab.c -e "s/  __attribute__ ((__unused__))$$/# ifndef __cplusplus\n  __attribute__ ((__unused__));\n# endif/" 
	g++ -c y.tab.c
		
yyfunc.tab.o: ParserFunc.y
	yacc -p "yyfunc" -b "yyfunc" -d ParserFunc.y
	#sed $(tag) yyfunc.tab.c -e "s/  __attribute__ ((__unused__))$$/# ifndef __cplusplus\n  __attribute__ ((__unused__));\n# endif/" 
	g++ -c yyfunc.tab.c
	
lex.yy.o: Lexer.l
	lex Lexer.l
	gcc  -c lex.yy.c

lex.yyfunc.o: LexerFunc.l
	lex -Pyyfunc LexerFunc.l
	gcc  -c lex.yyfunc.c

clean: 
	$(RM) *.o
	$(RM) *.out
	$(RM) *.tab.*
	$(RM) lex.yy*.c
	
cleanAll: clean
	$(RM) *.sr
	$(RM) *.bin*
	$(RM) *.tmp
	

all: Record.o Comparison.o ComparisonEngine.o Schema.o File.o y.tab.o lex.yy.o main.o
	$(CC) -o main Record.o Comparison.o ComparisonEngine.o Schema.o File.o y.tab.o lex.yy.o main.o -lfl
