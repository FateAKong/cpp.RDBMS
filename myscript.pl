#!/usr/bin/perl
#--Created by Satish Menedi(smenedi@cise.ufl.edu)--

open (TESTDATA, "<testdata.txt") or die $!;
my @lines = <TESTDATA>;

$linescount = @lines;
print "$lines";
foreach (@lines) {
	#print("This is a test.\n");
	print "\n".$_; 
	@lineelements = split(',',$_);
	open (TESTINPUT, ">testinput.txt") or die $!;
	foreach (@lineelements) {
		print TESTINPUT $_."\n";
	}
	close (TESTINPUT);
	`./test.out < testinput.txt | grep "out of"`;	
}
close TESTDATA;

