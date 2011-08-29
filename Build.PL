use Module::Build;
my $b= Module::Build->new(
        module_name        => 'Pg::Pcurse',
        dist_version_from  => 'lib/Pg/Pcurse.pm',
        dist_author        => 'Ioannis Tambouras<ioannis@cpan.org>',
        requires           => {  perl                 => '5.008',
                                #'FindBin'            => '1.47',
                                #'Text::Autoformat'   => '0',
                                'Log::Log4perl'      => '1.06',
                                'DBIx::Abstract'      => '1.006',
                                'Data::Dumper'        => '2.121_08',
                                'Carp::Assert'        => '0.18',
                                'Curses'              => '1.23',
                                'Curses::Widgets'     => '1.997',
                                'Getopt::Long'        => '2.35',
                                'File::Slurp'         => '9999.12',
                                'User::pwent'         => '1.00',
                              },
        recommends         => { 'Test::Pod'           =>  1.0,
                                'Test::Pod::Coverage' => '1.0',
                                #'Test::Exception'    => 0.25,
                              },
        script_files       =>  [ 'script/pcurse' ],
        license            => 'gpl',
        #create_readme     => 1,
);

$b->create_build_script;

#print  "\nIt takes 2 seconds. This time read the README file!\n\n";

