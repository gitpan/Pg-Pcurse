# Copyright (C) 2008 Ioannis Tambouras <ioannis@cpan.org>. All rights reserved.
# LICENSE:  GPLv3, eead licensing terms at  http://www.fsf.org .
package Pg::Pcurse::Query1;
use DBIx::Abstract;
use Carp::Assert;
use base 'Exporter';
use Data::Dumper;
use strict;
use warnings;
our $VERSION = '0.05';

our @EXPORT = qw( 
	form_dsn       first_word      dbconnect 
	databases      databases2      tables_vacuum 
        to_d           get_table       all_settings   
        get_proc_desc  get_proc        tables_vacuum2
        get_setting    table_buffers   over_dbs

	get_tables2_desc         tables_vacuum_desc 
        pgbuffercache            proc_of
	get_nspacl               view_of
        types2text               rule_of

        table_stat_desc          table_stat
	all_databases_desc       all_databases 
	get_database2_desc       get_database2 
	get_schemas              get_schemas2 
	get_tables_all_desc      get_tables_all
	get_views_all_desc       get_views_all
	index2_desc              index2 
	get_index_desc           get_index 
	table_stats_desc         table_stats 
	table_stats2_desc        table_stats2 
        statsoftable_desc        statsoftable 
        rules_desc               rules
);


sub form_dsn {
        my ($o, $dbname) = @_;
	$dbname or $dbname = $o->{dbname};
        assert( ref$o, 'HASH' );
        "dbi:Pg:dbname=$dbname;host=$o->{host};port=$o->{port}";
}


sub first_word {
	split /\s/, $_[0];
}

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
		

sub all_databases_desc {
          sprintf '%-10s %8s  %8s%8s%8s%10s%12s%8s', 'NAME', 'BENDS','COMMIT',
                              'ROLL','READ','HIT', 'AGE', '';
}
sub all_databases {
	my $o = shift;
	my $dsn =  form_dsn ($o, '');
	my $dh  = dbconnect( $o, $dsn  ) or return;
        my $st  = $dh->select({
                fields=> [qw( pg_database.datname numbackends 
                              xact_commit         xact_rollback
                              blks_read           blks_hit 
                              age(datfrozenxid)
                              pg_catalog.pg_encoding_to_char(encoding) 
                          )],
                table=>'pg_stat_database,pg_database',
                join=>'pg_stat_database.datname=pg_database.datname',
                });
       [ sort map { sprintf '%-10s%9s%10s%7s%9s%12s%10s  %-15s', @{$_}[0..7] }  
               @{ $st->fetchall_arrayref} ];
}

sub get_database2_desc {
          sprintf '%-10s%8s', '  ', 'age'
}
sub get_database2 {
#        pg_catalog.pg_encoding_to_char(encoding) as encoding,
	my ($o, $database)   = @_;
	$database or $database = $o->{dbname} ;
	my $dsn =  form_dsn ($o, $database);
	my $dh  = dbconnect( $o, $dsn  ) or return;
        my $st  = $dh->select({
                fields=> [qw( datname   rolname  datistemplate  datallowconn 
                              datconnlimit age(datfrozenxid)
                          )],
                table=>'pg_database,pg_roles',
                join=>'pg_database.datdba=pg_roles.oid',
                where  =>  ['datname', '=', $dh->quote($database) ]
                });
          map { sprintf '%-10s%8s', @{$_}[0,5] }  
               @{ $st->fetchall_arrayref};
}
sub get_schemas {
	my $database = shift;
	my $dh = dbconnect ( 'dbi:Pg:dbname='. $database  ) or return;
	my $h = $dh->select_all_to_hashref(
                   [qw(nspname nspowner nspacl)], 'pg_namespace');
	[keys %$h];
}
sub get_schemas2 {
	my ($o, $database)   = @_;
	$database or $database = $o->{dbname} ;
	my $dsn =  form_dsn ($o, $database);
	my $dh = dbconnect ( $o, $dsn ) or return;
        my $st = $dh->select({
                fields=> [qw(nspname rolname)],
                table=>'pg_namespace,pg_roles',
                join=>'pg_namespace.nspowner=pg_roles.oid',
                });
	[  
		sort  { $a !~ /^public/}
		sort  { $a gt $b}
           map { sprintf '%-20s%-10s', @{$_}[0..1] }  
               @{ $st->fetchall_arrayref}
        ];
}


sub get_tables2_desc {
          sprintf '%-24s%-8s%4s',  'NAME',  'OWNER', '   I R T';
}
sub get_tables_all_desc {
  sprintf '%-24s%30s','NAME',
          'pages    tup  idx  att  ch  tr  fk ref  pk  ru sub';
}
sub get_tables_all { 
	my ($o, $database , $schema) = @_;
        $database or $database = $o->{dbname} ;
	my $dh = dbconnect ( $o, form_dsn($o,$database)  ) or return;
	$schema = $dh->quote($schema);
        my $st = $dh->select({
                     fields => [qw( relname      relpages    reltuples
				    relhasindex  relnatts    relchecks  
                                    reltriggers  relfkeys    relrefs 
                                    relhaspkey   relhasrules relhassubclass
                                   )],
                     tables => 'pg_class,pg_namespace',
	             join   => 'pg_class.relnamespace =  pg_namespace.oid',
                     where  =>  ['pg_namespace.nspname', '=', $schema ,
                                'and', 'relkind', '=', q('r') ],
	           });
	[ sort map { sprintf '%-25s%5s%6s%6s%4s%4s%4s%4s%4s%4s%4s%4s', @{$_}[0..11]}
	       @{$st->fetchall_arrayref} ];

;
}
sub index2_desc {
     sprintf '%-30s%-10s%-10s %-10s %-10s','NAME', 'RELNAME',
                    'idx_scan', 'idx_tup_read','idx_tup_fetch'
}
sub index2 {
	my ($o, $database , $schema) = @_;
	my $dh = dbconnect ( $o, form_dsn($o, $database ) ) or return;
	$schema = $dh->quote( $schema );
	my $h = $dh->{dbh}->selectall_arrayref( <<"");
			select indexrelname, relname,  idx_scan,
                               idx_tup_read,  idx_tup_fetch, indexrelid
                         from pg_stat_user_indexes
  	                 where schemaname = $schema
	                 order by 3, 2

        [ map { sprintf '%-30s  %-15s  %8s %8s %8s %90s', @{$_}[0..5] }
	      @$h ]

}
sub get_proc_desc {
	sprintf '%-25s%-9s%10s%8s%6s%6s%9s','NAME','LANG', 'strict', 'setof',
                                 'volit', 'nargs','type'
}
sub get_proc {
	my ( $o, $database , $schema) = @_;
        $database or $database = $o->{dbname} ;
	my $dh = dbconnect ( $o, form_dsn($o,$database)  ) or return;
	$schema = $dh->quote($schema);
        my $h = $dh->{dbh}->selectall_arrayref(<<"" );
	select proname,           lanname,
	       proisstrict as s,  proretset as set,  provolatile as v,
	       pronargs as nargs, typname, p.oid
	from pg_proc  p
             join pg_namespace n on (pronamespace=n.oid)
	     join pg_language  l on (prolang=l.oid)
	     join pg_roles     r on (proowner=r.oid)
	     join pg_type      t on (prorettype=t.oid)
	where nspname=$schema
	order by 1

	[  map { sprintf '%-25s%-9s%7s%7s%9s%7s%9s%40s', @{$_}[0..7]} 
           @$h ];
}

sub tables_vacuum_desc {
	 sprintf '%-22s%22s%22s', 'NAME', 'vacuum', 'analyze'
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
sub tables_vacuum {
	my ($o, $database , $schema) = @_;
        $database or $database = $o->{dbname} ;
	my $dh = dbconnect ( $o, form_dsn($o,$database)  ) or return;
	$schema = $dh->quote($schema);
        my $h  = $dh->{dbh}->selectall_arrayref(<<"");
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
	where schemaname=$schema
	order by 2, 3, 1

        for my $i (@$h) { $_=to_d($_)   for @$i; }
	[ map { sprintf '%-22s%22s%22s', @{$_}[0..2]}
	       @{$h} ];
}

sub table_buffers { 
	my $o = shift;
	my $dh = dbconnect ( $o, form_dsn($o, '')  ) or return;
        my $h  = $dh->{dbh}->selectall_arrayref(<<"");
	select 'checkpoints_timed',(select checkpoints_timed 
                                                from pg_stat_bgwriter)
	union
	select 'checkpoints_req',(select checkpoints_req  from pg_stat_bgwriter)
	union
	select 'checkpoints Total',(select checkpoints_timed+checkpoints_req  
                                                from pg_stat_bgwriter)
	union
	select 'Pages/ck',
	(select buffers_checkpoint/(checkpoints_timed+checkpoints_req)  
                                                from pg_stat_bgwriter)
	union
	select 'buffers_alloc',   (select buffers_alloc   from pg_stat_bgwriter)
	union
	select 'buffers_backend', (select buffers_backend from pg_stat_bgwriter)
	union
	select 'buffers_backend', (select buffers_backend from pg_stat_bgwriter)
	union
	select 'buffers_clean',   (select buffers_clean from pg_stat_bgwriter)
	union
	select 'checkpoint_buffers', (select buffers_checkpoint from pg_stat_bgwriter)
	union
	select 'maxwritten_clean', (select maxwritten_clean from pg_stat_bgwriter)
	union
	select name, setting::int
	from pg_settings
	where name ~ 'buffer'
	order by 1

	[ map { sprintf '%-25s%10s', @{$_}[0..1]}
		       @{$h} ];
}
sub tables_vacuum2 { 
	my $o = shift;
	my $dh = dbconnect ( $o, form_dsn($o, '')  ) or return;
        my $h  = $dh->{dbh}->selectall_arrayref(<<"");
	select 'max database age',
	      (select max(age(datfrozenxid)) from pg_database)::text
	union
	select name , setting
	from pg_settings
	where name~'vacuum'

	[ map { sprintf '%-35s%20s', @{$_}[0..1]}
		       @{$h} ];
}
sub table_stats_desc {
     sprintf '%-20s%15s%15s%13s','NAME','seq-scan','idx_scan', 'ndead_tup', 
}
sub table_stats {
	my ($o, $database , $schema) = @_;
	my $dh = dbconnect ( $o, form_dsn($o, $database ) ) or return;
	my $st = $dh->select( [qw(  relname     seq_scan  
                                    idx_scan    n_dead_tup
                               )],
                              'pg_stat_user_tables',
                              ['schemaname', '=', $dh->quote($schema) ]);
	my $h = $st->fetchall_arrayref;
        for my $i (@$h) { 
              $_ || ($_= 0 )   for @$i; 
        }
        [ sort map { sprintf '%-20s%15s%15s%13s', @{$_}[0..3] } 
	      @{ $h } ];

}
sub table_stats2_desc {
     sprintf '%-25s%8s%9s%9s%9s%8s%8s','NAME',  'inserts','updates',
                        'deletes','hot-upd', 'live','dead'
}
sub table_stats2 {
	my ($o, $database , $schema) = @_;
	my $dh = dbconnect ( $o, form_dsn($o, $database ) ) or return;
	my $st = $dh->select( [qw(  relname     
                                    n_tup_ins      n_tup_upd   n_tup_del    
                                    n_tup_hot_upd  n_live_tup  n_dead_tup
                               )],
                              'pg_stat_user_tables',
                              ['schemaname', '=', $dh->quote($schema) ]);
	my $h = $st->fetchall_arrayref;
        for my $i (@$h) { 
              $_ || ($_= 0 )   for @$i; 
        }
        [ sort map { sprintf '%-25s%8s%9s%9s%9s%8s%8s', @{$_}[0..6] }
	      @{ $h } ];

}
sub get_table {
	#TODO it crashes when we dont' have permission to the table
        return ['needs work'];
	my ($o, $database , $schema, $table) = @_;
	my $dh = dbconnect ( $o, form_dsn($o, $database ) ) or return;
	my $st = $dh->select('*', "$schema.$table" )  or return;
	[ map {"@$_"}  @{$st->fetchall_arrayref}]  ;
}
sub all_settings {
	my $o = shift;
	my $dh = dbconnect ( $o, form_dsn($o,'') ) or return;
	my $st = $dh->select([qw(name setting unit)], 'pg_settings')  or return;
	[ map { sprintf '%-34s%19s%10s', $_->[0], $_->[1]||'', $_->[2]||'' }
          @{$st->fetchall_arrayref} ];
} 
sub get_setting {
	my ($o,$name) = @_;
	return unless $name;
	my $dh = dbconnect ( $o, form_dsn($o,'') ) or return;
	my $h  = $dh->select_one_to_hashref(
                        [qw( name category context min_val max_val
			     short_desc extra_desc vartype unit setting 
                        )], 'pg_settings', 
                        ['name', '=', $dh->quote($name) ])  or return;
        [ sprintf( '%-s', $h->{name}), '',
          sprintf( '%-9s : %s', 'vartype' , $h->{vartype} || ''),
          sprintf( '%-9s : %s', 'min_val' , $h->{min_val} || ''),
          sprintf( '%-9s : %s', 'max_val' , $h->{max_val} || ''),
          sprintf( '%-9s : %s', 'setting' , $h->{setting} || ''),
          sprintf( '%-9s : %s', 'units'   , $h->{units}   || ''),
          sprintf( '%-9s : %s', 'context' , $h->{context} || ''),
          sprintf( '%-9s : %s', 'sourse'  , $h->{sourse}  || ''),
	  sprintf( '%-9s : %s', 'category', $h->{category}|| ''), 
	  '',
	  sprintf( '%-70s', $h->{short_desc}||''),
	  '',
        ] 
} 

sub get_index_desc {
        sprintf('%-14s%-10s',  'NAME',  'u  p c v r xmin');
}
sub get_index {
        my ($o, $database ,  $indexrelid) = @_;
        my $dh   =  dbconnect ( $o, 'dbi:Pg:dbname='. $database  ) or return;
        my $qin  =  $dh->quote( $indexrelid );

        my $h  = $dh->select_one_to_hashref(
                 [  qw( indexrelid::regclass 
                     indrelid        indnatts   indisunique   indisprimary 
                     indisclustered  indisvalid indcheckxmin  indisready 
                     indkey indclass indoption  indexprs      indpred
                  )],
                 'pg_index',
                  [ 'indexrelid','=', $qin]);

        [ sprintf( '%-14s : %s', 'name' ,       $h->{indexrelid} || ''),
          sprintf( '%-14s : %s', 'indexrelid' , $indexrelid),
          sprintf( '%-14s : %s', 'indrelid' ,   $h->{indrelid}),
          sprintf( '%-14s : %s', 'indnatts' ,   $h->{indnatts}),
          sprintf( '%-14s : %s', 'indisunique' , $h->{indisunique} ),
          sprintf( '%-14s : %s', 'indisprimary' , $h->{indisprimary} ),
          sprintf( '%-14s : %s', 'indisclustered' , $h->{indisclustered} ),
          sprintf( '%-14s : %s', 'indisvalid' , $h->{indisvalid} ),
          sprintf( '%-14s : %s', 'indcheckxmin' , $h->{indcheckxmin} ),
          sprintf( '%-14s : %s', 'indisready' , $h->{indisready} ),
          sprintf( '%-14s : %s', 'indkey' , $h->{indkey} ),
          sprintf( '%-14s : %s', 'indkey' , $h->{indkey} ),
          sprintf( '%-14s : %s', 'indclass' , $h->{indclass} ),
          sprintf( '%-14s : %s', 'indoption' , $h->{indoption} ),
          sprintf( '%-14s : %s', 'indexprs' , $h->{indexprs} ),
          sprintf( '%-14s : %s', 'indpred' , $h->{indpred} ),
        ]

}
sub table_stat_desc {
     sprintf '%-30s%-10s%-10s %-10s %-10s','NAME', 'RELNAME',
                    'idx_scan', 'idx_tup_read','idx_tup_fetch'
}
sub table_stat {
	my ($o, $database , $schema, $table) = @_;
	my $dh = dbconnect ( $o, form_dsn($o, $database ) ) or return;
	(my $st = $dh->{dbh}->prepare( <<""))->execute( $table, $schema);
	select relname, rolname,  c.oid as coid, relfilenode,
	       pg_column_size( c.oid )       as col, 
               relhasoids, age(relfrozenxid) as age,
	       relpages,
	       pg_size_pretty( pg_relation_size(c.oid))       as rsize,
	       pg_size_pretty( pg_total_relation_size(c.oid)) as trsize,
	       relacl, reloptions
	from pg_class c join pg_namespace n on (relnamespace= n.oid)
	     join pg_roles r on ( relowner = r.oid)
	where relname = ? and nspname = ?

        my $h = $st->fetchrow_hashref  ;

	[ sprintf( '%-14s : %s', 'name' ,        $h->{relname}     ),
	  sprintf( '%-14s : %s', 'owner',        $h->{rolname}     ),
	  sprintf( '%-14s : %s', 'oid',          $h->{coid}        ),
	  sprintf( '%-14s : %s', 'filenode',     $h->{relfilenode} ),
	  sprintf( '%-14s : %s', 'column size',  $h->{col}         ),
	  sprintf( '%-14s : %s', 'hasoids',      $h->{relhasoids}  ),
	  sprintf( '%-14s : %s', 'age',          $h->{age}         ),
	  sprintf( '%-14s : %s', 'pages',        $h->{relpages}    ),
	  sprintf( '%-14s : %s', 'pages',        $h->{relpages}    ),
	  sprintf( '%-14s : %s', 'size',         $h->{rsize}       ),
	  sprintf( '%-14s : %s', 'total relsize',$h->{trsize}    ),
	  sprintf( '%-14s : %s', 'acl',      
                                 $h->{relacl} ? "@{ $h->{relacl} }": ''),
	  sprintf( '%-14s : %s', 'options',      $h->{reloptions}),
        ]
}
sub over_dbs {
	my ($o, $database )= @_;
	my $dh = dbconnect ( $o, form_dsn($o, $database ) ) or return;
	$database = $dh->quote( $database );
        my $st = $dh->select({
                fields => [qw( datname       rolname       encoding  
                              datistemplate  datallowconn  datconnlimit   
                              datlastsysoid  age(datfrozenxid)
                              dattablespace  datconfig     datacl
                              pg_size_pretty(pg_database_size(datname))
                         )],
                table =>'pg_database,pg_roles',
                join  =>'pg_database.datdba=pg_roles.oid',
                where => ['datname', '=', $database] });

        my $h = $st->fetchrow_hashref  ;

	[ sprintf( '%-15s : %s', 'datname' ,   $h->{datname}       ),
	  sprintf( '%-15s : %s', 'rolname',    $h->{rolname}       ),
	  sprintf( '%-15s : %s', 'encoding',   $h->{encoding}      ),
	  sprintf( '%-15s : %s', 'istemplate', $h->{datistemplate} ),
	  sprintf( '%-15s : %s', 'allowconn',  $h->{datallowconn}  ),
	  sprintf( '%-15s : %s', 'connlimit',  $h->{datconnlimit}  ),
	  sprintf( '%-15s : %s', 'lastsysoid', $h->{datlastsysoid} ),
	  sprintf( '%-15s : %s', 'tablespace', $h->{dattablespace} ),
	  sprintf( '%-15s : %s', 'Size',       $h->{pg_size_pretty} ),
	  sprintf( '%-15s : %s', 'config',      
                                 $h->{datconfig} ? "@{ $h->{datconfig} }": ''),
	  sprintf( '%-15s : %s', 'relacl',      
                                 $h->{relacl} ? "@{ $h->{relacl} }": ''),
        ];
}
sub statsoftable_desc {
     sprintf '%-25s%8s%9s%9s%9s%8s%8s','NAME',  'inserts','updates',
                        'deletes','hot-upd', 'live','dead'
}
sub statsoftable {
        my ($o, $database, $schema, $table )= @_;
        my $dh = dbconnect ( $o, form_dsn($o, $database ) ) or return;

	my $h  = $dh->select_one_to_hashref([qw(  relname    seq_scan
                                 seq_tup_read   idx_scan      idx_tup_fetch
				 n_tup_ins      n_tup_upd     n_tup_del
                                 n_tup_hot_upd  n_live_tup    n_dead_tup
                                 last_vacuum    last_autovacuum 
                                 last_analyze   last_autoanalyze
                               )],
                              'pg_stat_user_tables',
                              [     'schemaname', '=', $dh->quote($schema),
                                'and', 'relname', '=', $dh->quote($table)]);

	[ sprintf( '%-18s : %s', 'relname' ,      $h->{relname}      ),
	  sprintf( '%-18s : %s', 'seq_scan',      $h->{seq_scan}     ),
	  sprintf( '%-18s : %s', 'seq_tup_read',  $h->{seq_tup_read} ),
	  sprintf( '%-18s : %s', 'idx_scan',      $h->{idx_scan}     ),
	  sprintf( '%-18s : %s', 'idx_tup_fetch', $h->{idx_tup_fetch}),
	  sprintf( '%-18s : %s', 'n_tup_ins',     $h->{n_tup_ins}    ),
	  sprintf( '%-18s : %s', 'n_tup_upd',     $h->{n_tup_upd}    ),
	  sprintf( '%-18s : %s', 'n_tup_del',     $h->{n_tup_del}    ),
	  sprintf( '%-18s : %s', 'n_tup_hot_upd', $h->{n_tup_hot_upd}),
	  sprintf( '%-18s : %s', 'n_live_tup',    $h->{n_live_tup}   ),
	  sprintf( '%-18s : %s', 'n_dead_tup',    $h->{n_dead_tup}   ),
	  sprintf( '%-18s : %s', 'last_vacuum',   $h->{last_vacuum}       ),
	  sprintf( '%-18s : %s', 'last_autovacuum',$h->{last_autovacuum}  ),
	  sprintf( '%-18s : %s', 'last_analyze',   $h->{last_analyze}     ),
	  sprintf( '%-18s : %s', 'last_autoanalyze',$h->{last_autoanalyze}),
        ];
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

sub proc_of {
        my ($o, $database, $oid )= @_;
        my $dh = dbconnect ( $o, form_dsn($o, $database ) ) or return;

        (my $st = $dh->{dbh}->prepare( <<""))->execute( $oid );
	         select proname  , nspname , rolname as owner,
                        lanname  , procost , prorows       , proisagg ,
                        prosecdef, proisstrict , proretset , provolatile , 
                        pronargs , typname     , proallargtypes, 
                        proargtypes ,
                        prosrc   , proargmodes , proargnames, probin ,
                        proacl   , proconfig   
		from pg_proc p 
                     join pg_namespace n on (pronamespace= n.oid)
		     join pg_language  l on (prolang = l.oid)
		     join pg_roles     r on (proowner=r.oid)
		     join pg_type      t on (prorettype=t.oid)
                where p.oid = ?


        my $h             = $st->fetchrow_hashref  ;
	$h->{proargtypes} = types2text( $o, $h->{proargtypes} );

	[ sprintf( '%-12s : %s', 'name',     $h->{proname}     ),
          sprintf( '%-12s : %s', 'oid',         $oid              ),
	  sprintf( '%-12s : %s', 'namespace',$h->{nspname}     ),
	  sprintf( '%-12s : %s', 'owner',    $h->{owner}       ),
	  sprintf( '%-12s : %s', 'lang',     $h->{lanname}     ),
	  sprintf( '%-12s : %s', 'cost',     $h->{procost}     ),
	  sprintf( '%-12s : %s', 'rows',     $h->{prorows}     ),
	  sprintf( '%-12s : %s', 'isagg',    $h->{proisagg}    ),
	  sprintf( '%-12s : %s', 'secdef',   $h->{prosecdef}   ),
	  sprintf( '%-12s : %s', 'isstrict', $h->{proisstrict} ),
	  sprintf( '%-12s : %s', 'retset',   $h->{proretset}   ),
	  sprintf( '%-12s : %s', 'volatile', $h->{provolatile} ),
	  sprintf( '%-12s : %s', 'nargs',    $h->{pronargs}    ),
	  sprintf( '%-12s : %s', 'rettype',  $h->{typname}  ),
	  sprintf( '%-12s : %s', 'argtypes', $h->{proargtypes} ),
	  sprintf( '%-12s : %s', 'argmodes', $h->{proargmodes} ),
	  #printf( '%-12s : %s', 'argnames', ($h->{proargnames})
                             #?  $h->{proargnames}[0] : ''),
	  #sprintf( '%-12s : %s', 'bin',      $h->{probin}      ),
	  sprintf( '%-12s : %s', 'config',   $h->{proconfig}   ),
	  sprintf( '%-12s : %s', 'acl',      $h->{proacl}      ),
	  sprintf( '%-12s : %s', 'src',      $h->{prosrc}||''  ),
	]
}
sub pgbuffercache_old {
        my ($o, $database )= @_;
        my $dh = dbconnect ( $o, form_dsn($o, $database ) ) or return;

	my $user = $dh->quote('postgres');
        my $h = $dh->select_one_to_hashref( "user = $user as who" );

        return [ q(Must be user "postgres" to view buffer data.) ]
                     unless $h->{who}; 

        my $v = $dh->quote('pg_buffercache') ;

        $h = $dh->{dbh}->selectrow_array(<<""); 
	                      select viewname from pg_views
				where viewname = $v

        return [ q(No public.pg_buffercache in this database.) ]   unless $h;

        (my $st = $dh->{dbh}->prepare( <<""))->execute();
		select relfilenode::regclass as name, count(1)
			from pg_buffercache
			where relfilenode is not null
			group by 1
			order by 2 desc
			;

	my @ret;
        while( $h = $st->fetchrow_hashref) {
		push @ret, sprintf('%-35s : %9s', $h->{name}, $h->{count});
        } 
	return \@ret;
}   
      
sub pgbuffercache {
        my ($o, $database )= @_;
        my $dh = dbconnect ( $o, form_dsn($o, $database ) ) or return;

	my $user = $dh->quote('postgres');
        my $h = $dh->select_one_to_hashref( "user = $user as who" );

        return [ q(Must be user "postgres" to view buffer data.) ]
                     unless $h->{who}; 

        my $v = $dh->quote('pg_buffercache') ;
        $h    = $dh->{dbh}->selectrow_array(<<""); 
		      select viewname from pg_views   where viewname = $v

        return [ q(No public.pg_buffercache in this database.) ]   unless $h;

        $h = $dh->{dbh}->selectall_arrayref( <<"");
	  	select relfilenode::regclass as name, count(1)
		from   pg_buffercache
		where  relfilenode is not null
		group  by 1
		order  by 2 desc
		;

	[ map { sprintf '%-35s : %9s', @{$_}[0..1] }  @$h ]
}   


sub get_nspacl {
	my ($o, $database, $schema) = @_;
        my $dh  = dbconnect ( $o, form_dsn($o, $database ) ) or return;
        $schema = $dh->quote( $schema );
	my $h   = $dh->select_one_to_hashref({
                   fields => 'nspacl', 
                   table  =>  'pg_namespace',
		   where  => [ 'nspname','=', $schema ] 
        });
   

	[ sprintf "%s", $h->{nspacl} ? "@{ $h->{nspacl} }": '' ];
}
sub get_views_all_desc {
	sprintf '%-35s %10s','NAME', 'OWNER' ;
} 
sub get_views_all {
	my ($o, $database , $schema) = @_;
        $database or $database = $o->{dbname} ;
	my $dh  = dbconnect ( $o, form_dsn($o,$database)  ) or return;
	$schema = $dh->quote($schema);
        my $st  = $dh->select(  [qw( viewname viewowner )],
	                       'pg_views',
                               ['schemaname', '=', $schema ] );
	[ sort map { sprintf '%-35s %10s', @{$_}[0..1]}
	       @{$st->fetchall_arrayref} ];
}
sub view_of {
	my ($o, $database , $schema, $view) = @_;

        $database or $database = $o->{dbname} ;
	my $dh  = dbconnect ( $o, form_dsn($o,$database)  ) or return;
	$view   = $dh->quote($view)  ;
	$schema = $dh->quote($schema);
        my $h   = $dh->select_one_to_hashref( 'definition',
	                                      'pg_views',
                                             ['schemaname' , '=', $schema, 
                                              'and viewname','=', $view ]);
	 $h->{definition} ;
}
sub rules_desc {
	sprintf '%-35s','NAME';
} 
sub rules {
	my ($o, $database , $schema) = @_;
        $database or $database = $o->{dbname} ;
	my $dh  = dbconnect ( $o, form_dsn($o,$database)  ) or return;
	$schema = $dh->quote($schema);
        my $st  = $dh->select(  [qw( rulename )],
	                       'pg_rules',
                               ['schemaname', '=', $schema ] );
	[ sort map { sprintf '%-35s', ${$_}[0]}
	       @{$st->fetchall_arrayref} ];
}
sub rule_of {
	my ($o, $database , $schema, $rule) = @_;
        $database or $database = $o->{dbname} ;
	my $dh  = dbconnect ( $o, form_dsn($o,$database)  ) or return;
	$schema = $dh->quote($schema);
	$rule   = $dh->quote($rule);
        my $h   = $dh->select_one_to_hashref (  
                                'definition', 'pg_rules',
                               ['schemaname', '=', $schema , 
                                'and', 'rulename', '=', $rule ]) ;

	sprintf '%-35s',  $h->{definition} ;
}

1;
__END__
	  sprintf( '%-14s : %s', 'acl',      
