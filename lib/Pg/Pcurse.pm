# Copyright (C) 2008 Ioannis Tambouras <ioannis@cpan.org>. All rights reserved.
# LICENSE:  GPLv3, eead licensing terms at  http://www.fsf.org .

package Pg::Pcurse;

use 5.008008;
use strict;
use warnings;

require Exporter;

use Curses;
use Curses::Widgets;
use Carp::Assert;
use Curses::Widgets::Menu;
#use Curses::Widgets::Label;
#use Curses::Widgets::ButtonSet;
#use Curses::Widgets::Listbox;
use strict;
use Pg::Pcurse::Query0;
use Pg::Pcurse::Query1;
use Pg::Pcurse::Query2;
use Pg::Pcurse::Query3;

our $VERSION = '0.06';

our $opt;

use base 'Exporter';

our @EXPORT = qw( 
	execute_mode       retrieve_context   capital_context
	$opt               retrieve_permit    update_big_display
	analyze            reindex            vacuum   
);

*secondary_listbox = *main::secondary_listbox;
*big_listbox       = *main::big_listbox;

#########################################################################
## Main Dispatcher
sub execute_mode {
        my $mode = shift;
        ({  
	    tables     =>  sub { show_tables()    },
	    views      =>  sub { show_views()     },
            overview   =>  sub { show_overview()  },
            vacuum     =>  sub { show_vacuum()    },
            stats      =>  sub { show_stats()     },
            buffers    =>  sub { show_buffers()   },
            indexes    =>  sub { show_indexes()   },
            procedures =>  sub { show_procedu()   },
            rules      =>  sub { show_rules()     },
            settings   =>  sub { show_settings()  },
            bucardo    =>  sub { show_bucardo()   },
            triggers   =>  sub { show_triggers()  },
            users      =>  sub { show_users()     },
         } -> {$mode})->();
}

##### The following Tables are ready for creation
sub first_cinema {
        my ($desc, $fun) = @_ ;
        $::tab   =   $fun->( $opt) or return;
        $::big   =   big_listbox( $desc, $::tab, 11, 0);
        $::big->execute($::mwh,0);
}
sub show_overview { first_cinema( all_databases_desc() , \& all_databases )}
sub show_settings { first_cinema( '', \& all_settings  )}
sub show_buffers  { first_cinema( '', \& table_buffers )} 
sub show_bucardo  { first_cinema( bucardo_conf_desc(), \& bucardo_conf)} 

#########################################################################
##### These Tables will first need a Schema table, then will show result
sub update_schema_display {
        #  Schema Table
        $::she     = get_schemas2( $opt, $::dbname) or return;
        $::schemas = secondary_listbox('Schemas', $::she, 2,37);
        $::schemas->draw($::mwh,0);
        $::schemas->execute($::mwh,0);
        $::schemas->draw($::mwh,0);
}

sub update_big_display {
        # Result Table ( like relevant tables, indexes, objects, etc,. )
        ($::sname) = first_word( $::she->[ $::schemas->getField('VALUE')] );
        $::desc    = $::desc->();
        $::tab     = $::actual->( $opt, $::dbname, $::sname);
        $::big     = big_listbox( $::desc, $::tab, 11, 0);
        $::big->execute($::mwh,0);
}
sub same_movie {
        ($::desc, $::actual) = @_ ;
	update_schema_display;
	update_big_display;
}

sub show_stats   { same_movie( \&table_stats_desc, \&table_stats       )  }
sub show_vacuum  { same_movie( \&tables_vacuum_desc, \&tables_vacuum   )  }
#sub show_tables  { same_movie( \&get_tables_all_desc, \&get_tables_all)  }
sub show_tables  { same_movie( \&tables_brief_desc, \&tables_brief     )  }
sub show_views   { same_movie( \&get_views_all_desc, \&get_views_all   )  }
sub show_procedu { same_movie( \&get_proc_desc, \&get_proc             )  }
sub show_indexes { same_movie( \&index2_desc, \&index2                 )  }
sub show_rules   { same_movie( \&rules_desc, \&rules                   )  }
sub show_triggers{ same_movie( \&schema_trg_desc, \&schema_trg         )  }
sub show_users   { same_movie( \&get_users_desc, \&get_users           )  }


## Another dispatcher
sub retrieve_permit {
        #my $index = $::schemas->getField('VALUE');
        #my $schema = $::schemas->getField('VALUE');
        my ($sna) = first_word( $::she->[ $::schemas->getField('VALUE')] );
	get_nspacl($opt, $::dbname, $sna) ;
}

## Another dispatcher
sub retrieve_context {
	#return if $::mode eq 'bucardo' ;
        ({  
	    tables     => \& tstat    ,
	   #tables     => \& statsof  ,
            views      => \& viewof   ,
           #vacuum     => \& vacuum2  ,
            vacuum     => \& vacuumof ,
            overview   => \& over2    ,
            stats      => \& statsof  ,
            settings   => \& settingof,
            procedures => \& procof   ,
	    buffers    => \& bufferca ,
	    rules      => \& ruleof   ,
            indexes    => \& indexof  ,
            bucardo    => \& bucardoof,
            triggers   => \& trgof    ,
            users      => \& userof   ,

         }->{$::mode||return})->(@_) ;
}

### The following functions are colled from the above dispatcher
sub stats2 { [table_stats2_desc,  @{table_stats2($opt, $::dbname, $::sname )}]}
sub vacuum2{ [@{tables_vacuum2( $opt, $::dbname)  }]}
sub bufferca{ [@{ pgbuffercache( $opt, $::dbname) }]}

#sub indexes{ [index2_desc,  @{index2($opt, $::dbname, $::sname )}] }
#sub procedu{ [get_proc_desc,  @{get_proc($opt, $::dbname, $::sname )}] }
#sub buffers { [ @{table_buffers( $opt, '')}]}

sub vacuumof {
        my $index = $::big->getField('VALUE');
        my ($f)   = first_word( $::tab->[$index] );
        vacuum_per_table( $opt, $::dbname, $::sname, $f ) or return [];
}
sub userof {
        my $index = $::big->getField('VALUE');
        my ($f)   = first_word( $::tab->[$index] );
        user_of( $opt, $f ) or return [];
}
sub trgof {
        my $index = $::big->getField('VALUE');
        my ($l)   = last_word( $::tab->[$index] );
        trg_of( $opt, $::dbname, $::sname, $l ) or return [];
}

sub ruleof {
        my $index = $::big->getField('VALUE');
        my ($f)   = first_word( $::tab->[$index] );
        my $text  = rule_of( $opt, $::dbname, $::sname, $f ) or return [];
	[  textwrap($text, 50) ];
}

sub over2  { 
        my $index = $::big->getField('VALUE');
        my ($f) = first_word( $::tab->[$index] );
        over_dbs( $opt, $f) ;
} 
sub bucardoof  { 
        my $index = $::big->getField('VALUE');
        my ($f) = first_word( $::tab->[$index] );
        bucardo_conf_of( $opt, $f) ;
} 


sub statsof  { 
        my $index = $::big->getField('VALUE');
        my ($f) = first_word( $::tab->[$index] );
        statsoftable( $opt, $::dbname, $::sname,  $f) ;
} 
sub sql_formater {
	local $_ = shift or return '';
	return $_  if /^(select|from)/i ;
	return "    $_";
}
sub viewof  { 
        my $index =  $::big->getField('VALUE');
        my ($f)   =  first_word( $::tab->[$index] );
        my $text  =  view_of( $opt, $::dbname, $::sname,  $f) ;
	[  '', 
	   map { sql_formater( $_ ) }
	   map { my @parts  = split  /\bFROM\b/i, $_ ,2  ;
		 (@parts>1) ?  ($parts[0], 'FROM '. $parts[1])  : $parts[0] 
                }
		Curses::Widgets::textwrap($text, 50) 
        ];
} 

sub settingof {
        my $index = $::big->getField('VALUE');
        my ($f) = first_word( $::tab->[$index] );
        get_setting( $opt, $f ) or return [];
}

sub last_word { local $_ = shift; my ($last) =  / \w+$/xg; $last; }

sub procof {
        my $index = $::big->getField('VALUE')   ; 
        my $last = last_word( $::tab->[$index] )   or return ['selection?'] ;
        proc_of( $opt, $::dbname, $last );
}

sub indexof {
        my $index = $::big->getField('VALUE')   ; 
        my $last = last_word( $::tab->[$index] )   or return ['selection?'] ;
        get_index( $opt, $::dbname, $last ) or return [];
}
sub tstat {
        my $index = $::big->getField('VALUE');
        my ($f) = first_word( $::tab->[$index] );
        table_stat( $opt, $::dbname, $::sname, $f ) or return [];
}
sub tableof {
        my $index = $::big->getField('VALUE');
        my ($f) = first_word( $::tab->[$index] );
        get_table( $opt, $::dbname, $::sname, $f ) or return [];
}

##########################################################################
## Another dispatcher
sub capital_context {
	return  unless $::mode eq 'tables';
        ({  tables     => \& tdataof  ,
         }->{$::mode||return})->(@_) ;
}

	    
sub tdataof {
        my $index = $::big->getField('VALUE');
        my ($f) = first_word( $::tab->[$index] );
        tbl_data_of( $opt, $::dbname, $::sname, $f ) or return [];
}

##########################################################################
## Other dispatchers

sub display_keyword {
        my $keyword = shift||return;
        my ($y,$x)  = (9,1) ;
        $::mwh->addstr( $y,$x, $keyword);
        $::mwh->refresh;
        sleep 1;
        $::mwh->addstr( $y,$x, ' ' x length$keyword);
        $::mwh->refresh;
}
sub update_big_d {
        # Result Table ( like relevant tables, indexes, objects, etc,. )
        ($::sname) = first_word( $::she->[ $::schemas->getField('VALUE')] );
        #$::desc    = $::desc->();
        $::tab     = $::actual->( $opt, $::dbname, $::sname);
        $::big     = big_listbox( $::desc, $::tab, 11, 0);
        $::big->execute($::mwh,0);
}

sub analyze  {
        return unless $::mode =~ qr/^(tables|stats|overview)/o;
        ({ tables    => \&do_analyze_tbl,
           stats     => \&do_analyze_tbl,
           overview  => \&do_analyze_db,
        } -> {$::mode})->() ? display_keyword 'ANALYZE'
                            : display_keyword 'failed';
        #update_big_d;
}
sub vacuum  {
        return unless $::mode =~ qr/^(tables|stats|overview)/o;
        ({ tables    => \&do_vacuum_tbl,
           stats     => \&do_vacuum_tbl,
           overview  => \&do_vacuum_db,
        } -> {$::mode})->() ? display_keyword 'VACUUM' 
                            : display_keyword 'failed' ;
}

sub reindex  {
	return unless $::mode eq 'indexes';
        ({  tables     => \& do_reindex_tbl ,
            overview   => \& do_reindex_db  ,
        }->{$::mode||return})->(@_) ? display_keyword 'REINDEX' 
                                    : display_keyword 'failed' ;
}
sub do_reindex_tbl {
        my $index = $::big->getField('VALUE');
        my ($f) = first_word( $::tab->[$index] );
        reindex_tbl( $opt, $::dbname, $::sname, $f ) or return ;
}
sub do_reindex_db {
        my $index = $::big->getField('VALUE');
        my ($f) = first_word( $::tab->[$index] );
        reindex_db( $opt, $::dbname ) or return ;
}

sub do_analyze_tbl {
        my $index = $::big->getField('VALUE');
        my ($f) = first_word( $::tab->[$index] );
        analyze_tbl( $opt, $::dbname, $::sname, $f ) or return ;
}
sub do_analyze_db {
        my $index = $::big->getField('VALUE');
        my ($f) = first_word( $::tab->[$index] );
        analyze_db( $opt, $::dbname ) or return ;
}
sub do_vacuum_tbl {
        my $index = $::big->getField('VALUE');
        my ($f) = first_word( $::tab->[$index] );
        vacuum_tbl( $opt, $::dbname, $::sname, $f ) or return ;
}
sub do_vacuum_db {
        my $index = $::big->getField('VALUE');
        my ($f) = first_word( $::tab->[$index] );
        vacuum_db( $opt, $::dbname ) or return ;
}



1;
__END__
# Below is stub documentation for your module. You'd better edit it!

=head1 NAME

Pg::Pcurse - Monitors a Postgres cluster


=head1 SYNOPSIS

  use Pg::Pcurse;

=head1 DESCRIPTION

Library functions for pcurse(1) which monitors Postgres databases



=head1 SEE ALSO

pcurse(1)


=head1 AUTHOR

Ioannis Tambouras, E<lt>ioannis@cpan.org<gt>

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2008 by Ioannis Tambouras

This library is free software; you can redistribute it and/or modify
it under the same terms of GPLv3


=cut
