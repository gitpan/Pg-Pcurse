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
use Pg::Pcurse::Query1;

our $VERSION = '0.04';

our $opt;

use base 'Exporter';

our @EXPORT = qw( 
	execute_mode       retrieve_context
	$opt               retrieve_permit
);

*secondary_listbox = *main::secondary_listbox;
*big_listbox       = *main::big_listbox;

#########################################################################
## Main Dispatcher
sub execute_mode {
        my $mode = shift;
        ({  
	    tables   => sub { show_tables()    },
	    views    => sub { show_views()     },
            overview => sub { show_overview()  },
            vacuum   => sub { show_vacuum()    },
            stats    => sub { show_stats()     },
            buffers  => sub { show_buffers()   },
            indexes  => sub { show_indexes()   },
            procedu  => sub { show_procedu()   },
            rules    => sub { show_rules()     },
            settings => sub { show_settings()  },
         } -> {$mode})->();
}

##### The following Tables are ready for creation
sub first_cinema {
        my ($desc, $fun) = @_ ;
        $::tab   =   $fun->( $opt) or return;
        $::big   =   big_listbox( $desc, $::tab, 11, 0);
        $::big->execute($::mwh,0);
        $::big->draw($::mwh,0);
}
sub show_overview { first_cinema( all_databases_desc() , \& all_databases )}
sub show_settings { first_cinema( '', \& all_settings  )}
sub show_buffers  { first_cinema( '', \& table_buffers )} 

#########################################################################
##### These Tables will first need a Schema table, then will show result
sub same_movie {
        my ($desc, $actual) = @_ ;

        #  Schema Table
        $::she     = get_schemas2( $opt, $::dbname) or return;
        $::schemas = secondary_listbox('Schemas', $::she, 2,37);
        $::schemas->draw($::mwh,0);
        $::schemas->execute($::mwh,0);
        $::schemas->draw($::mwh,0);

        # Display Resust ( like relevant tables, indexes, objects, etc,. )
        ($::sname) = first_word( $::she->[ $::schemas->getField('VALUE')] );
        $desc      = $desc->();
        $::tab     = $actual->( $opt, $::dbname, $::sname);
        $::big     = big_listbox( $desc, $::tab, 11, 0);
        $::big->execute($::mwh,0);
        $::big->draw($::mwh,0);
}

sub show_stats   { same_movie( \&table_stats_desc, \&table_stats       )  }
sub show_vacuum  { same_movie( \&tables_vacuum_desc, \&tables_vacuum   )  }
sub show_tables  { same_movie( \&get_tables_all_desc, \&get_tables_all )  }
sub show_views   { same_movie( \&get_views_all_desc, \&get_views_all   )  }
sub show_procedu { same_movie( \&get_proc_desc, \&get_proc             )  }
sub show_indexes { same_movie( \&index2_desc, \&index2                 )  }
sub show_rules   { same_movie( \&rules_desc, \&rules                   )  }


## Another dispatcher
sub retrieve_permit {
        #my $index = $::schemas->getField('VALUE');
        #my $schema = $::schemas->getField('VALUE');
        my ($sna) = first_word( $::she->[ $::schemas->getField('VALUE')] );
	get_nspacl($opt, $::dbname, $sna) ;
}

## Another dispatcher
sub retrieve_context {
        ({  tables     => \& tstat    ,
            views      => \& viewof   ,
            vacuum     => \& vacuum2  ,
            overview   => \& over2    ,
            stats      => \& statsof  ,
            settings   => \& settingof,
            procedu    => \& procof   ,
	    buffers    => \& bufferca ,
	    rules      => \& ruleof   ,
            indexes    => \& indexof  ,

         }->{$::mode||return})->(@_) ;
}

### The following functions are colled from the above dispatcher
sub stats2 { [table_stats2_desc,  @{table_stats2($opt, $::dbname, $::sname )}]}
sub vacuum2{ [@{tables_vacuum2( $opt, $::dbname)  }]}
sub bufferca{ [@{ pgbuffercache( $opt, $::dbname) }]}

#sub indexes{ [index2_desc,  @{index2($opt, $::dbname, $::sname )}] }
#sub procedu{ [get_proc_desc,  @{get_proc($opt, $::dbname, $::sname )}] }
#sub buffers { [ @{table_buffers( $opt, '')}]}


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

sub statsof  { 
        my $index = $::big->getField('VALUE');
        my ($f) = first_word( $::tab->[$index] );
        statsoftable( $opt, $::dbname, $::sname,  $f) ;
} 
sub viewof_old  { 
        my $index =  $::big->getField('VALUE');
        my ($f)   =  first_word( $::tab->[$index] );
        my $text  =  view_of( $opt, $::dbname, $::sname,  $f) ;
	my @all   =   Curses::Widgets::textwrap($text, 50) ;
	my @ret  ;
	for (@all) {
		my @parts =split /\bFROM\b/i, $_ ,2 ;
		(@parts>1) ?  push( @ret, $parts[0], 'FROM '. $parts[1] )
			   :  push( @ret, @parts );
	};
	[ @ret ];
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
	[  map { sql_formater( $_ ) }
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

1;

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
