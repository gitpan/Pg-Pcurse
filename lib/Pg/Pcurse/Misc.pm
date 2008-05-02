# Copyright (C) 2008 Ioannis Tambouras <ioannis@cpan.org>. All rights reserved.
# LICENSE:  GPLv3, eead licensing terms at  http://www.fsf.org .
package Pg::Pcurse::Misc;
use Carp::Assert;
use Getopt::Compact;
use base 'Exporter';
our $VERSION = '0.06';
use Data::Dumper;

@EXPORT = qw(
	get_getopt
	process_options
	$bucardo_defaults
	schema_sorter
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
sub schema_sorter($$) {
	my ($aa,$bb) =  @_;
	($aa) = $aa =~ /(\w+)/g;
         $aa eq 'public'    and return -1;
         $aa =~ /^pg_/o     and return  1;
         $aa =~ /^inform/o  and return  1;
         -1;
}

our $bucardo_defaults = {
	ctl_checkabortedkids_time => 30  ,
	ctl_checkonkids_time      => 10  ,
	ctl_createkid_time        => 0.5 , 
	ctl_nothingfound_sleep    => 1.0 ,
	ctl_nothingfound_sleep    => 1.0 ,
	ctl_pingtime              => 600 ,
	default_email_from        => 'nobody@example.com',
	default_email_to          => 'nobody@example.com',
	endsync_sleep             =>  1.0 ,
	endsync_sleep             =>  1.0 ,
	kick_sleep                =>  0.2 ,
	kick_sleep                =>  0.2 ,
	kid_abort_limit           =>  3   ,
	kid_nodeltarows_sleep     =>  0.8 ,
	kid_nodeltarows_sleep     =>  0.8 ,
	kid_nothingfound_sleep    =>  0.1 ,
	kid_nothingfound_sleep    =>  0.1 ,
	kid_pingtime              =>  60  ,
	kid_serial_sleep          =>  10  ,
	kid_serial_sleep          =>  10  ,
	log_showline              =>  0   ,
	log_showpid               =>  0   ,
	log_showtime              =>  1   ,
	max_delete_clause         =>  200 ,
	max_select_clause         =>  500 ,
	mcp_dbproblem_sleep       =>  15  ,
	mcp_dbproblem_sleep       =>  15  ,
	mcp_loop_sleep            =>  0.1 ,
	mcp_loop_sleep            =>  0.1 ,
	mcp_pingtime              =>  60  ,
	piddir                    =>  '/var/run/bucardo',
	pidfile                   =>  'bucardo.pid',
	reason_file               =>  '/home/bucardo/restart.reason', 
	stats_script_url          =>  'http://www.bucardo.org/', 
	stopfile                  =>  'fullstopbucardo',
	syslog_facility           =>  'LOG_LOCAL1',
	tcp_keepalives_count      =>   2  ,
	tcp_keepalives_idle       =>   10 ,
	tcp_keepalives_interval   =>   5  ,
	upsert_attempts           =>   3  ,
};

1;
