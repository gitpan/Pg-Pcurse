use Test::More;


my $dir  = $ENV{PWD} =~ m#\/t$#  ? '../' : '';
my @files =   (    "${dir}blib/lib/Pg/Pcurse.pm" ,
		   "${dir}script/pcurse",
);
plan  tests=> scalar @files;

eval 'use Test::Pod' ;


SKIP: {        
		skip  'no Test::Pod', scalar @files    if $@ ;
		pod_file_ok( $_,  $_)   for @files;
};

