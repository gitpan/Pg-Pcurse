# Copyright (C) 2008 Ioannis Tambouras <ioannis@cpan.org>. All rights reserved.
# LICENSE:  GPLv3, eead licensing terms at  http://www.fsf.org .
package Pg::Pcurse::Query3;
use 5.008008;
use DBIx::Abstract;
use Carp::Assert;
use base 'Exporter';
use Data::Dumper;
use strict;
use warnings;
our $VERSION = '0.14';
use Pg::Pcurse::Misc;
use Pg::Pcurse::Query0;
use Pg::Pcurse::Defaults;

#*pg_default = *Pg::Pcurse::Defaults::pg_default;

our @EXPORT = qw( 
	bucardo_conf_of  user_of
        proc_of		 view_of
        rule_of		 tbl_data_of              
	trg_of           tables_of_db      tables_of_db_desc
        statsoftable_desc        statsoftable 
	all_settings     get_setting
	dict_desc        dict
);

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

	[ sprintf( '%-18s : %s', 'relname' ,       $h->{relname}      ),
	  sprintf( '%-18s : %s', 'seq_scan',       $h->{seq_scan}     ),
	  sprintf( '%-18s : %s', 'idx_scan',       $h->{idx_scan}     ),
	  sprintf( '%-18s : %s', '% read/idx', 
	            calc_read_ratio( $h->{seq_scan},$h->{idx_scan})    ),
	  sprintf( '%-18s : %s', 'n_live_tup',      $h->{n_live_tup}   ),
	  sprintf( '%-18s : %s', 'n_dead_tup',      $h->{n_dead_tup}   ),
	  sprintf( '%-18s : %s', 'seq_tup_read',   $h->{seq_tup_read} ),
	  sprintf( '%-18s : %s', 'idx_tup_fetch',  $h->{idx_tup_fetch}),
	  sprintf( '%-18s : %s', 'n_tup_ins',       $h->{n_tup_ins}    ),
	  sprintf( '%-18s : %s', 'n_tup_upd',       $h->{n_tup_upd}    ),
	  sprintf( '%-18s : %s', 'n_tup_del',       $h->{n_tup_del}    ),
	  sprintf( '%-18s : %s', 'n_tup_hot_upd',   $h->{n_tup_hot_upd}),
	  sprintf( '%-18s : %s', 'last_vacuum',     $h->{last_vacuum}  ),
	  sprintf( '%-18s : %s', 'last_autovacuum' ,$h->{last_autovacuum}),
	  sprintf( '%-18s : %s', 'last_analyze',    $h->{last_analyze}  ),
	  sprintf( '%-18s : %s', 'last_autoanalyze',$h->{last_autoanalyze}),
        ];
}

sub beautify_src {
	my $src = shift||return'';
($src) =~ /\S.*\S/gs;
$src =~ s/^\s*//mg;
#$src =~ s/\n/ /smg;
 Curses::Widgets::textwrap($src, 40);
}
sub proc_of {
        my ($o, $database, $oid )= @_;
        my $dh = dbconnect ( $o, form_dsn($o, $database ) ) or return;

        (my $st = $dh->{dbh}->prepare( <<""))->execute( $oid, 'pg_proc', $oid);
	         select proname  , nspname , pg_get_userbyid(proowner) as owner,
                        lanname  , procost , prorows       , proisagg ,
                        prosecdef, proisstrict , proretset , provolatile , 
                        pronargs , prorettype::regtype     , proallargtypes, 
                        proargtypes ,
                        prosrc   , proargmodes , proargnames, probin ,
                        proacl   , proconfig   ,
		        pg_catalog.obj_description( ?, ? )  as desc
		from pg_proc p 
                     join pg_namespace n on (pronamespace= n.oid)
		     join pg_language  l on (prolang = l.oid)
                where p.oid = ?

        my $h = $st->fetchrow_hashref  ;
	$h->{proargtypes} = types2text( $o, $h->{proargtypes} );
	[ sprintf( '%-12s : %s', 'name',     $h->{proname}     ),
          sprintf( '%-12s : %s', 'oid',         $oid              ),
	  sprintf( '%-12s : %s', 'namespace',$h->{nspname}     ),
	  sprintf( '%-12s : %s', 'owner',    $h->{owner}       ),
	  sprintf( '%-12s : %s', 'lang',     $h->{lanname}     ),
	  sprintf( '%-12s : %s', 'desc',     $h->{desc}        ),
	  sprintf( '%-12s : %s', 'rows',     $h->{prorows}     ),
	  sprintf( '%-12s : %s', 'isagg',    $h->{proisagg}    ),
	  sprintf( '%-12s : %s', 'secdef',   $h->{prosecdef}   ),
	  sprintf( '%-12s : %s', 'isstrict', $h->{proisstrict} ),
	  sprintf( '%-12s : %s', 'retset',   $h->{proretset}   ),
	  sprintf( '%-12s : %s', 'nargs',    $h->{pronargs}    ),
	  sprintf( '%-12s : %s', 'rettype',  $h->{prorettype}  ),
	  sprintf( '%-12s : %s', 'argtypes', $h->{proargtypes} ),
	  sprintf( '%-12s : %s', 'argmodes', $h->{proargmodes} ),
	  sprintf( '%-12s : %s', 'acl',      $h->{proacl}      ),
	  #printf( '%-12s : %s', 'argnames', ($h->{proargnames})
                             #?  $h->{proargnames}[0] : ''),
          #sprintf( '%-12s : %s', 'bin',     $h->{probin}      ),
	  sprintf( '%-12s : %s', 'volatile', $h->{provolatile} ),
	  sprintf( '%-12s : %s', 'config',   $h->{proconfig}   ),
	  sprintf( '%-12s : %s', 'cost',     $h->{procost}     ),
	  '',
          beautify_src( $h->{prosrc} ),
	];
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
sub max_length_keys {
	my $max=0;
	for (@_) {
		if (length$_ > $max) { $max = length$_};
	}
	$max;
}
sub bucardo_conf_of {
        my ($o, $setting) = (@_);
        my $dh   = dbconnect ( $o, form_dsn($o, 'bucardo')  ) or return;
	my $h    = $dh->select_one_to_hashref(
                        [qw( setting value about cdate )], 
		        'bucardo.bucardo_config',
		        ['setting','=', $dh->quote( $setting) ] );


        [  sprintf( '%-25s : %-s', 'setting', $h->{setting}  ),
           sprintf( '%-25s : %-s', 'value'  , $h->{value}    ),
           sprintf( '%-25s : %-s', 'default', $bucardo_defaults->{$setting} ), 
           sprintf( '%-25s : %-s', 'cdate'  , $h->{cdate}    ),
	   '',
#%TODO
          Curses::Widgets::textwrap($h->{about},30),
        ]
}

sub trg_of {
	my ($o, $database , $schema, $tgoid ) = @_;
	return [ 'invalid oid '] unless $tgoid =~ /^\d+$/;
        $database or $database = $o->{dbname} ;
	my $dh  = dbconnect ( $o, form_dsn($o,$database)  ) or return;
	$schema = $dh->quote($schema);
        #my $h   = $dh->select_one_to_hashref (
        my $h   = $dh->select_one_to_hashref ( 
                                [qw( tgname  tgrelid::regclass 
	                             tgfoid  tgenabled tgtype
                                     tgisconstraint tgconstrname
                                     tgconstrrelid::regclass   
                                     tgconstraint
                                     tgdeferrable    tginitdeferred 
                                     tgnargs  tgattr tgargs
                                    ),
				     "pg_get_triggerdef($tgoid) as def",
                                 ],   
                                'pg_trigger',
                                 [ 'oid', '=', $tgoid] );

	[ sprintf( '%-15s : %-s',  'name'         ,  $h->{tgname}         ),
	  sprintf( '%-15s : %-s',  'relid'        ,  $h->{tgrelid}        ),
	  sprintf( '%-15s : %-s',  'foid'         ,  $h->{tgfoid}         ),
	  sprintf( '%-15s : %-s',  'type'         ,  $h->{tgtype}         ),
	  sprintf( '%-15s : %-s',  'enabled'      ,  $h->{tgenabled}      ),
	  sprintf( '%-15s : %-s',  'isconstraint' ,  $h->{tgisconstraint} ),
	  sprintf( '%-15s : %-s',  'constrname'   ,  $h->{tgconstrname}   ),
	  sprintf( '%-15s : %-s',  'constrrelid'  ,  $h->{tgconstrrelid}  ),
	  sprintf( '%-15s : %-s',  'constraint'   ,  $h->{tgconstraint}   ),
	  sprintf( '%-15s : %-s',  'deferrable'   ,  $h->{tgdeferrable}   ),
	  sprintf( '%-15s : %-s',  'initdeferred' ,  $h->{tginitdeferred} ),
	  sprintf( '%-15s : %-s',  'nargs'        ,  $h->{tgnargs}        ),
	  sprintf( '%-15s : %-s',  'attr'         ,  $h->{tgattr}         ),
	  sprintf( '%-15s : %-s',  'args'         ,  $h->{tgargs}         ),
	  '',
	  Curses::Widgets::textwrap($h->{def},70),
        ]
}


sub user_of {
	my ($o, $user ) = @_;
	return [ 'invalid user '] unless $user;
	my $dh  = dbconnect ( $o, form_dsn($o,'')  ) or return;

	$user   = $dh->quote('postgres');
        my $h   = $dh->select_one_to_hashref( "user = $user as who" );
        return [ q(Must be user "postgres" to view authid data.) ]
                     unless $h->{who}; 

        $h      = $dh->select_one_to_hashref ( 
                    [qw( rolname      rolsuper      rolinherit   rolcreaterole 
                         rolcreatedb  rolcatupdate  rolcanlogin  rolconnlimit
                         rolpassword  rolvaliduntil rolconfig
                    )],
                    'pg_authid',
	            ['rolname','=', $user] );

        [  sprintf( '%-14s : %-s', 'name'      , $h->{rolname}        ),
           sprintf( '%-14s : %-s', 'super'     , $h->{value}          ),
           sprintf( '%-14s : %-s', 'inherit'   , $h->{rolinherit}     ),
           sprintf( '%-14s : %-s', 'createrole', $h->{rolcreaterole}  ),
           sprintf( '%-14s : %-s', 'createdb'  , $h->{rolcreatedb}    ),
           sprintf( '%-14s : %-s', 'catupdate' , $h->{rolcatupdate}   ),
           sprintf( '%-14s : %-s', 'canlogin'  , $h->{rolcanlogin}    ),
           sprintf( '%-14s : %-s', 'connlimit' , $h->{rolconnlimit}   ),
           sprintf( '%-14s : %-s', 'password'  , $h->{rolpassword}    ),
           sprintf( '%-14s : %-s', 'validuntil', $h->{rolvaliduntil}  ),
#TODO
        #  sprintf( '%-14s : %-s', 'config'    , $h->{rolconfig}      ),
	]
}
sub tbl_data_of {
	my ($o, $database , $schema, $table) = @_;
        $database or $database = $o->{dbname} ;
	my $dh  = dbconnect ( $o, form_dsn($o,$database)  ) or return;
	(my $st  = $dh->{dbh}->prepare(<<""))->execute;
			select age(xmin), * from  $schema.$table
			order by age(xmin) desc
			limit 20

	my ($i,@ret) = 0;
	while ( my $h= $st->fetchrow_hashref ) {	
		push @ret,
		sprintf( '-[ RECORD  %3s ]-------------------------', $i++),
		sprintf '%-20s : %s', 'age(xmin)', $h->{age} ; 
		while( my ($k,$v) = each %$h) {
			next if $k eq 'xmin';
			push @ret,
		        sprintf '%-20s : %s', $k, $v ; 
		}
		last if $i>20;
	}
	return [ @ret ];
}

sub formatrule {
	my $all = shift;
	my ($l1, $rest)  = $all =~ m/^(.*AS\s*)(\bON\b.*)/sxgi;
	my ($l2, $more)  = $rest =~ m/^(.*)(\bDO\b.*)/xgsi;
        [ $l1, $l2, Curses::Widgets::textwrap($more,60)];
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

	 formatrule( $h->{definition} )  ;
}
sub tables_of_db_desc {
         sprintf '%-32s  %-17s', 'Table', 'Age (Million)';
}
sub tables_of_db {
	my ($o, $database ) = @_;
	my $dh  = dbconnect ( $o, form_dsn($o,$database)  ) or return;
        my $h   = $dh->{dbh}->selectall_arrayref( <<"");
		select  nspname||'.'||relname, age( relfrozenxid )
		from pg_class c
			 join pg_namespace n on ( n.oid= c.relnamespace)
		where relkind = 'r'
			and nspname not like 'pg_%'
			and nspname not like 'information_schema'
		order by 2 desc

        [ map { sprintf '%-40s  %5.3f', ${$_}[0], ${$_}[1]/1_000_000 } @$h ]
;

}
sub are_equal {
	my ($actual, $default) = @_;
	return 1  if ($actual eq'60s' and $default eq '1min');
	return 1  if ($actual eq'1024kB' and $default eq '1MB');
	return 1  if ($actual eq'2048kB' and $default eq '2MB');
	return 1  if ($actual eq'16384kB' and $default eq '16MB');
	return 1  if ($actual eq'10240kB' and $default eq '10MB');
	return 1  if ($actual eq'300s' and $default eq '5min');
	return 1  if ($actual eq'1000ms' and $default eq '1s');
	return 1  if ($actual eq'1440min' and $default eq '1d');
	return 1  if ($actual eq'88kB' and $default eq '64kB');
	return 1  if ($actual eq'-1kB' and $default eq '-1');
	return 1  if ($actual eq'-1ms' and $default eq '-1');
	return 1  if ($actual eq'10248kB' and $default eq '8MB');
	return 1  if ($actual eq'163848kB' and $default eq '128MB');
	return;
}

sub all_settings {
	my ($o,undef,undef, $context)= @_;
	my $dh = dbconnect ( $o, form_dsn($o,'') ) or return;
	my $st;
	if ($context =~ /^all/xoi ) {
		$st = $dh->select([qw(name setting unit)], 'pg_settings');
	}elsif ($context =~ /^changed/xoi) {
		$st = $dh->select([qw(name setting unit)], 'pg_settings');
        }else{
		$st = $dh->select([qw(name setting unit)], 
                             'pg_settings', ['context', 'ilike', 
                                    $dh->quote($context)])  or return;
	}
	if ($context !~ /^changed/xoi ) {
	   [ map { sprintf '%-34s%19s%10s',$_->[0], $_->[1]||'',$_->[2]||''}
             @{$st->fetchall_arrayref} ];
	}else{
		my @res ;
		for ( @{$st->fetchall_arrayref}) {
			my ( $name,$val,$unit) = ($_->[0], 
                                                 $_->[1]||'', $_->[2]||'') ;
	                my $default=$Pg::Pcurse::Defaults::pg_default->{$name};
			next  unless $default;
			next  if  ($val.$unit) eq $default;
			next  if  are_equal($val.$unit, $default);
		       push @res, 
		       sprintf '%-34s%19s%10s',$name, $val,$unit,
		
		}
		return  [ @res ] ;
	}
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
          sprintf( '%-9s : %s', 'setting' , $h->{setting} || ''),
          sprintf( '%-9s : %s', 'default' , 
                   $Pg::Pcurse::Defaults::pg_default->{ $h->{name}} ),
          sprintf( '%-9s : %s', 'vartype' , $h->{vartype} || ''),
          sprintf( '%-9s : %s', 'min_val' , $h->{min_val} || ''),
          sprintf( '%-9s : %s', 'max_val' , $h->{max_val} || ''),
          sprintf( '%-9s : %s', 'units'   , $h->{units}   || ''),
          sprintf( '%-9s : %s', 'context' , $h->{context} || ''),
          sprintf( '%-9s : %s', 'sourse'  , $h->{sourse}  || ''),
	  sprintf( '%-9s : %s', 'category', $h->{category}|| ''), 
	  '',
	  Curses::Widgets::textwrap($h->{short_desc},75),
	  '',
	  Curses::Widgets::textwrap($h->{extra_desc},75),
	  ($h->{name} eq 'log_line_prefix') && 
		sprintf 'REMEMBER: pgfouine expects   %s', '%t [%p]: [%l-1]'
        ] 
} 
sub dict_desc {
	 sprintf'%-20s %-10s         %-10s', 'Dict', 'Owner', 'template';
}
sub dict {
        my ($o, $database, $schema, $table )= @_;
        my $dh  = dbconnect ( $o, form_dsn($o, $database ) ) or return;
        $schema = $dh->quote( $schema );
        my $h   = $dh->{dbh}->selectall_arrayref( <<"" );
	 	select dictname,  pg_get_userbyid( dictowner), tmplname
		from      pg_ts_dict     d
	             join pg_namespace   n  on (dictnamespace=n.oid)
	             join pg_ts_template t  on (d.dicttemplate=t.oid)
	        where nspname = $schema

	[ map { sprintf('%-20s %-10s         %-10s', @{$_}[0..2] )} 
	     @$h
        ]

}

1;
__END__
