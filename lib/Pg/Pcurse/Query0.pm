# Copyright (C) 2008 Ioannis Tambouras <ioannis@cpan.org>. All rights reserved.
# LICENSE:  GPLv3, eead licensing terms at  http://www.fsf.org .
package Pg::Pcurse::Query0;
use DBIx::Abstract;
use Carp::Assert;
use base 'Exporter';
use Data::Dumper;
use strict;
use warnings;
our $VERSION = '0.12';
use Pg::Pcurse::Misc;

our @EXPORT = qw( 
	form_dsn     first_word   databases      databases2 
	dbconnect    to_d         to_h           search4func 
	one_type     types2text   object_totals  object_totals_desc
	misc_system_wide 
);
sub search4func {
        my ( $o, $func, @dbs) = @_ ;
        for my $d (@dbs) {
                my $dh = dbconnect ( $o, form_dsn($o, $d) ) or next;
                my $h    = $dh->select_one_to_hashref( 'proname', 'pg_proc',
                        [ 'proname','=', $dh->quote($func) ] ) ;
                return $d if exists $h->{proname};
        }
        return ;
}


sub form_dsn {
        my ($o, $dbname) = @_;
	$dbname or $dbname = $o->{dbname};
        assert( ref$o, 'HASH' );
        "dbi:Pg:dbname=$dbname;host=$o->{host};port=$o->{port}";
}


sub first_word { split /\s/, $_[0] }

sub dbconnect  {
	my $default = 'dbi:Pg:service=nossl';
	my ($o, $dsn) = @_ ;
	$ENV{ PGSYSCONFDIR } = $ENV{ PWD };
        open STDERR, '>/dev/null';
	DBIx::Abstract->connect({   dsn  => $dsn || $default,
	                            driver    => 'Pg',
				    user      => $o->{user}   || 'ioannis', 
				    password  => $o->{passwd} || 'silver'
				},{ PrintWarn => 0, 
		     	 	    PrintError=> 0, 
                                    RaiseError=> 1 }
                                 )
        or return;
}
sub to_d {
	$_[0]= '' unless $_[0];
	return $_[0] unless /^\d/o;
	s{-}{/}g;
	s/^(.*:.{2}).*/$1/;
	chomp;
	s/^.{2}//;
	$_[0];
}
sub to_h {
        local $_ = shift|| return '';
        my ($want) = split;
        ($want)    =  $want =~ /^.{5}(.*)/gxo;
}


#sub database_sources { DBI->data_sources('Pg') }

sub databases { 
	map {s/^.*dbname=//;$_}  
	DBI->data_sources('Pg'); 
}
sub databases2 { 
	my ($o, $database)   = @_;
	$database or $database = $o->{dbname} ;
	my $dsn =  form_dsn ($o, $database);
	my $dh  = dbconnect( $o, $dsn  ) or return;
        my $st  = $dh->select( 'datname' , 'pg_database');
	sort 
        map { sprintf '%s', ${$_}[0] }  
        @{ $st->fetchall_arrayref} ;
}

sub one_type {
        my ($dh, $oid )= @_;
        my  ($ret) = $dh->select_one_to_array( << "" );
                pg_catalog.format_type( $oid, $oid )

        $ret;
}


sub types2text {
        my ($o, $str) = @_;
        my $dh = dbconnect( $o, form_dsn($o, $o->{dhname}) ) or return;
        my @oids = split /\s+/, $str||return '';
        return '' unless @oids;
        my $ret = '';
        $ret = $ret . one_type( $dh, $_) .  '   '   for @oids;
        $ret;
}
	
sub object_totals_desc { sprintf '   r   v   i   t   c  S' }

sub object_totals {
        my ($o, $database, $mode ) = @_;
        return unless $mode =~ /^ (all | shared | noshared ) $/ox ;
        my $dh = dbconnect ( $o, form_dsn($o,$database)  ) or return;
	my $st;

	if ($mode eq 'all') {
		($st = $dh->{dbh}->prepare(<<""))->execute()   
			 select relkind, count(2) from pg_class group by 1

	}else{
	 	my $shared = $mode eq 'shared' ? 'true' : 'false';
		($st=$dh->{dbh}->prepare(<<""))->execute( $shared );   
			 select relkind, count(2) from pg_class 
			 where relisshared = ?
                         group by 1

	}

        my $r ; 
        $r->{ $_->{relkind}} = $_->{count} while $_=$st->fetchrow_hashref;
        [ sprintf ' %3s %3s %3s %3s %3s %2s', $r->{r}, $r->{v}, $r->{i},
                                              $r->{t}, $r->{c}, $r->{S} 
	]

}

sub misc_system_wide {
        my ( $o ) = @_ ;
        my $dh = dbconnect ( $o, form_dsn($o,'')) or return;

	my $h0 = $dh->select_one_to_hashref(
		'pg_postmaster_start_time()::timestamp(0) as start' );

	my $h1 = $dh->select_one_to_hashref( 'txid_current()' );

	[ sprintf( '%-17s : %15s', 'postmaster start', $h0->{start}     ), 
	  sprintf( '%-17s : %15s', 'txid current', $h1->{txid_current}  ),
        ];
}

1;
__END__
