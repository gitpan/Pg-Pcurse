use 5.008008;
use ExtUtils::MakeMaker;


WriteMakefile(
    PL_FILES          => {},
    NAME              => 'Pg::Pcurse',
    VERSION_FROM      => 'lib/Pg/Pcurse.pm',
    PREREQ_PM         => { 
	                   #'File::Slurp'        => '9999.12',
	                   #'FindBin'            => '1.47',
	                   #'Log::Log4perl'      => '1.06',
	                   'DBIx::Abstract'      => '0',
	                   'Data::Dumper'        => '0',
	                   'Carp::Assert'        => '0',
	                   'Curses'              => '0',
	                   'Curses::Widgets'     => '0',
	                   'Getopt::Compact'     => '0',
                         },
    EXE_FILES         =>  [ 'script/pcurse'],
    ABSTRACT_FROM     => 'lib/Pg/Pcurse.pm',
    AUTHOR            => 'Ioannis Tambouras <ioannis@cpan.org>',
);

print  "\nIt takes 2 seconds. This time read the README file!\n\n";
