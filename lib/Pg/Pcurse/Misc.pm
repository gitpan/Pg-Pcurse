# Copyright (C) 2008 Ioannis Tambouras <ioannis@cpan.org>. All rights reserved.
# LICENSE:  GPLv3, eead licensing terms at  http://www.fsf.org .
package Pg::Pcurse::Misc;
use Carp::Assert;
use Getopt::Compact;
use base 'Exporter';
our $VERSION = '0.03';

@EXPORT = qw(
	get_getopt
	process_options
);

sub get_getopt {
	new Getopt::Compact  modes  => [qw( verbose  )],
                             struct => [ ['dbname', 'dbname', ':s'],
                                         ['host', 'hostname', ':s'],
                                         ['user', 'user',     ':s'],
                                         ['passwd', 'passwd', ':s'],
                                         ['port',   'port',   ':s'],
                                        ],
}

sub process_options {
	my $o = shift;
	assert( ref$o, 'HASH' );
	$o->{user}   =  getlogin   unless $o->{user};
	$o->{passwd} =  undef      unless $o->{passwd};
	$o->{host}   = 'localhost' unless $o->{host};
	$o->{dbname} = 'template1' unless $o->{dbname};
	$o->{port}   =  5432       unless $o->{port};
	$o->{verbose}=  0          unless $o->{verbose};
	$o;
}


1;
