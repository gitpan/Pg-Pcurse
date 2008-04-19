use Module::Build;
my $b= Module::Build->new(
        module_name        => 'Pg::Pcurse',
        dist_version_from  => 'lib/Pg/Pcurse.pm',
        dist_author        => 'Ioannis Tambouras<ioannis@cpan.org>',
        requires           => {  perl                 => '5.8',
				#'File::Slurp'        => '9999.12',
                                #'FindBin'            => '1.47',
                                #'Log::Log4perl'      => '1.06',
                                'Carp::Assert'        => '0',
                                'Curses'              => '0',
                                'Curses::Widgets'     => '0',
                                'Getopt::Compact'     => '0',
                              },
        recommends         => { 'Test::More'          => 0,
                                'Test::Pod'           => 1.20,
                                #'Test::Exception'    => 0.25,
                                #'Test::Pod::Coverage'=> '1.0',
                              },
        script_files       =>  [ 'script/pcurse' ],
        license            => 'gpl',
        #create_readme     => 1,
);

$b->create_build_script;

print  "\nIt takes 2 seconds. This time read the README file!\n\n";

