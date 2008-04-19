.PHONY: $(shell ls)
.SILENT: 

#   00_load.t         01b_pod.t           01c_coverage.t  
#
 
MODULES  =  -MData::Dumper -MCarp  
MODULES +=  -MTest::More   -Mblib=../blib

all: 01c_coverage.t
#all: 00_load.t

0*:
	perl -w -Mstrict -Mblib  $(MODULES)   $@  
pod:
	perldoc  ../blib/lib/Pg/Pcurse.pm
pode:
	perldoc  ../script/pcurse
cp:
	cp  ../blib/lib/Pg/Pcurse.pm    ../lib/Pg
	cp  ../blib/lib/Pg/Pcurse/*.pm  ../lib/Pg/Pcurse/
w:
	chmod -R u+w  ../blib/lib/Pg
vi: w
	vim  ../blib/lib/Pg/Pcurse.pm  
viq: w
	vim  ../blib/lib/Pg/Pcurse/Query1.pm  
vip: w
	vim  ../script/pcurse
vim: w
	vim  ../blib/lib/Pg/Pcurse/Misc.pm  
