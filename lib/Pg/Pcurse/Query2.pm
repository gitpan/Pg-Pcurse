# Copyright (C) 2008 Ioannis Tambouras <ioannis@cpan.org>. All rights reserved.
# LICENSE:  GPLv3, eead licensing terms at  http://www.fsf.org .
package Pg::Pcurse::Query2;
use DBIx::Abstract;
use Carp::Assert;
use base 'Exporter';
use Data::Dumper;
use strict;
use warnings;
our $VERSION = '0.06';
use Pg::Pcurse::Query0;


our @EXPORT = qw( 
	tables_brief_desc     tables_brief 
	actual_tables_rows    estimated_tables_rows

);


sub actual_tables_rows {
	# Output: a hashref like { tablename => row_size, ... }
	#         {tblname=>undef} when permissions inhibit read access
	#         {tblname=>0}     when table contains no rows

	my ($o, $database , $schema ) = @_;
	#return ['system'] if $schema =~ /^ (pg_ | information_schema) /xo;
	my $dh  = dbconnect ( $o, form_dsn($o, $database ) ) or return;
	my $st  = $dh->select( 'tablename as name', 
                               'pg_tables',
                               ['schemaname','=', $dh->quote($schema)]) 
                                        or return {} ;
                                        
	my $rows ;	
	for my $relname ( map {@$_} @{ $dh->fetchall_arrayref} ) { 
		eval{($rows) = $dh->select_one_to_array('count(1)',$relname )};
		$_->{ $relname } =  $@ ? undef: $rows ;
	}
	$_ ;

}

sub estimated_tables_rows {
	# Output: a hashref like { tablename => row_size, ... }
	#         {tblname=>undef} when permissions inhibit read access
	#         {tblname=>0}     when table contains no rows

        my ($o, $database , $schema) = @_;
        $database or $database = $o->{dbname} ;
        my $dh = dbconnect ( $o, form_dsn($o,$database)  ) or return;
        $schema = $dh->quote($schema);
        my $st = $dh->select({
                     fields => [qw( relname   reltuples)],
                     tables => 'pg_class,pg_namespace',
                     join   => 'pg_class.relnamespace =  pg_namespace.oid',
                     where  =>  ['pg_namespace.nspname', '=', $schema ,
                                'and', 'relkind', '=', q('r') ],
                   });
        $_ = { map {  @{$_}[0..1] } @{$st->fetchall_arrayref}  };
}


sub tables_brief_desc {
	 sprintf '%-26s %12s %12s    %8s %8s', 'NAME', 'tupl', 'est_tupl',
                                            'analyze', 'vacuum';
}

sub tables_brief {

	my ($o, $database , $schema) = @_;
        $database or $database = $o->{dbname} ;
	my $dh = dbconnect ( $o, form_dsn($o,$database)  ) or return;
        (my $st  = $dh->{dbh}->prepare(<<""))->execute($schema) or return ;
	select relname,
		case
		when (last_vacuum is null)and(last_autovacuum is null) then null
		when (last_vacuum is null) then last_autovacuum
		when (last_autovacuum is null) then last_vacuum
		when age(last_vacuum,last_autovacuum)>'1 second'then last_vacuum
		else last_autovacuum
		end as vacuum,
		case
		when(last_analyze is null)and(last_autoanalyze is null)then null
		when (last_analyze is null) then last_autoanalyze
		when (last_autoanalyze is null) then last_analyze
		when age(last_analyze, last_autoanalyze)>'1 second' then last_analyze
		else last_autoanalyze
		end as analyze
	from pg_stat_all_tables
	where schemaname= ?
	order by 1

        my $actual    = actual_tables_rows( $o, $database , $schema) ;
        my $estimated = estimated_tables_rows( $o, $database , $schema) ;
	my @arr;
	while ( my $h = $st->fetchrow_hashref) {
                my $relname = $h->{relname};	

		push @arr, sprintf '%-26s %12s %12s   %8s   %8s', 
                                       $h->{relname}, 
		(defined $actual->{$relname} ? $actual->{$relname} 
                                            : 'not_read'),
		(defined $estimated->{$relname} ? $estimated->{$relname} 
                                            : 'no_estima'),
					to_h( $h->{analyze}), 
                                        to_h( $h->{vacuum }) ;
        }
        [ @arr ];
}


1;
__END__ 
