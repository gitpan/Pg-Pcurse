use 5.008008;
use ExtUtils::MakeMaker;


WriteMakefile(
    PL_FILES          => {},
    NAME              => 'Pg::Pcurse',
    VERSION_FROM      => 'lib/Pg/Pcurse.pm',
    PREREQ_PM         => { 
	                   #'FindBin'            => '1.47',
	                   #'Log::Log4perl'      => '1.06',
			   #'Text::Autoformat'   => '0',
	                   'DBIx::Abstract'      => '1.006',
	                   'Data::Dumper'        => '2.121_08',
	                   'Carp::Assert'        => '0.18',
	                   'Curses'              => '1.23',
	                   'Curses::Widgets'     => '1.997',
	                   'Getopt::Long'        => '2.35',
	                   'File::Slurp'         => '9999.12',
	                   'User::pwent'         => '1.00',
                           'Test::Pod::Coverage' => '1.0',
                           'Test::Pod'           => '1.0',
                         },
    EXE_FILES         =>  [ 'script/pcurse'],
    ABSTRACT_FROM     => 'lib/Pg/Pcurse.pm',
    AUTHOR            => 'Ioannis Tambouras <ioannis@cpan.org>',
);

#print  "\nIt takes 2 seconds. This time read the README file!\n\n";
