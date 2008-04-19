.PHONY: $(shell ls)
.SILENT:

#args= -n 3  #-dv   # -c /bin/touch  -p pid

MODULES  =  -MData::Dumper    -Mstrict -Mblib=../blib

OPTS     =  -lw

#all: try
#all: pcurse
#all: pcurseSuper
#all: podp
all: coot
#all: puffy

demo try e*:
	./perl $(OPTS) $(MODULES) $@
pcurse:
	./pcurse --host  localhost --user ioannis --passwd silver 
pcurseSuper:
	./pcurse --host  localhost --user postgres --passwd reG1roup
 
puffy:
	./pcurse --dbname  hiphop --user devro --passwd mania  --host puffy
coot:
	./pcurse  --user ioannis --passwd silver  --host $@

w:
	chmod -R u+w  ../blib/lib/Pg
vip: w
	vim  ../blib/lib/Pg/Pcurse.pm  
viq: w
	vim  ../blib/lib/Pg/Pcurse/Query1.pm  
viw: w
	vim  ../blib/lib/Pg/Pcurse/Widget.pm  
vi: 
	vim  pcurse
cp:
	cp  ../blib/lib/Pg/Pcurse.pm    ../lib/Pg
	cp  ../blib/lib/Pg/Pcurse/*.pm  ../lib/Pg/Pcurse/

podp:
	perldoc pcurse
pod:
	perldoc ../blib/lib/Pg/Pcurse.pm
