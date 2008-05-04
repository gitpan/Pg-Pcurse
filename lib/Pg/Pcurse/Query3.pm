# Copyright (C) 2008 Ioannis Tambouras <ioannis@cpan.org>. All rights reserved.
# LICENSE:  GPLv3, eead licensing terms at  http://www.fsf.org .
package Pg::Pcurse::Query3;
use DBIx::Abstract;
use Carp::Assert;
use base 'Exporter';
use Data::Dumper;
use strict;
use warnings;
our $VERSION = '0.07';
use Pg::Pcurse::Misc;
use Pg::Pcurse::Query0;


our @EXPORT = qw( 
	bucardo_conf_of  user_of
        proc_of		 view_of
        rule_of		 tbl_data_of              
	trg_of
        statsoftable_desc        statsoftable 
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

        my $h = $st->fetchrow_hashref  ;
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
	  sprintf( '%-12s : %s', 'src',      $h->{prosrc}      ),
	  #sprintf( '%-12s : %s', 'src',      ($h->{prosrc})
          #?  "@{[ Curses::Widgets::textwrap($h->{prosrc}, 40)]}"
          #: ''  ),
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
sub max_length_keys {
	my $max=0;
	for (@_) {
		if (length$_ > $max) { $max = length$_};
	}
	$max;
}
sub tbl_data_of {
	my ($o, $database , $schema, $table) = @_;
        $database or $database = $o->{dbname} ;
	my $dh  = dbconnect ( $o, form_dsn($o,$database)  ) or return;
	my $st  = $dh->select('*', "$schema.$table" );
	#my $len = max_length_keys( keys %$h);
	my ($i, @ret) = (0);
	while (my $h   = $st->fetchrow_hashref) {
		push @ret,
		sprintf '-[ RECORD  %3s ]-------------------------', $i++;
		while( my ($k,$v) = each %$h) {
			push @ret,
		        sprintf '%-20s : %s', $k, $v ; 
		}
	}
	return [ @ret ];
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

1;
__END__
