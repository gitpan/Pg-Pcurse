# Copyright (C) 2008 Ioannis Tambouras <ioannis@cpan.org>. All rights reserved.
# LICENSE:  GPLv3, eead licensing terms at  http://www.fsf.org .
package Pg::Pcurse::Query0;
use DBIx::Abstract;
use Carp::Assert;
use base 'Exporter';
use Data::Dumper;
use strict;
use warnings;
our $VERSION = '0.06';
use Pg::Pcurse::Misc;

our @EXPORT = qw( 
	form_dsn     first_word   databases    databases2 
	dbconnect    to_d         to_h
	one_type     types2text
);



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
	

1;
__END__
