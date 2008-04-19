# Copyright (C) 2008 Ioannis Tambouras <ioannis@cpan.org>. All rights reserved.
# LICENSE:  GPLv3, eead licensing terms at  http://www.fsf.org .
package Pg::Pcurse::Widget;
use Curses;
use Curses::Widgets;
use Carp::Assert;
use Curses::Widgets::Menu;
#use Curses::Widgets::Label;
#use Curses::Widgets::ButtonSet;
#use Curses::Widgets::Listbox;
use strict;
use warnings;
use Pg::Pcurse;

use base 'Exporter';

our @EXPORT = qw( 
	          init_screen
	          create_root
		  create_commentbox
	          create_menu
	          create_botton
	          main_listbox  secondary_listbox  big_listbox
		  form_dbmenu
);

sub miniscan {
	noecho();
        my $mwh = shift;
        my $key = -1;
        while ($key eq -1) {
                $key = $mwh->getch;
                if($key eq "j")  { return KEY_DOWN    };
                if($key eq "k")  { return KEY_UP      };
                if($key eq "h")  { return "\n"        };
                if($key eq ' ')  { return "\e"        };
                if($key eq 'm')  { return KEY_RIGHT   };
                if($key eq 'n')  { return KEY_LEFT    };
                if($key eq 'q')  { exit 0             };
        }
        return $key;
}

sub _Database_Menu_Choice {
        my $dbs = shift;
        my $ret ;
        for  my $i ( @$dbs)  { $ret->{$i}= sub{ $::db=$i} }
        $ret->{ITEMORDER} = $dbs ;
        $ret;
}
sub form_dbmenu {
        my $dbs = shift;
	assert( ref $dbs, 'ARRAY') if DEBUG;
        my $menus = { MENUORDER => [qw( Databases Mode About ) ],
                      Databases => _Database_Menu_Choice ($dbs),
                      Hide      =>{ ITEMORDER => [ 'System' ],
                                System    => sub { $::hid{system}++} },
                      Mode      =>{ ITEMORDER => [qw( Vacuum   Stats
                                                    Procedures Tables    
                                                    Overview   Buffers 
                                                    Indexes   Settings
                                                  )],
                                Vacuum     => sub { $::mode = 'vacuum'    },
                                Tables     => sub { $::mode = 'tables'    },
                                Stats      => sub { $::mode = 'stats'     },
                                Indexes    => sub { $::mode = 'indexes'   },
                                Overview   => sub { $::mode = 'overview'  },
                                Procedures => sub { $::mode = 'procedu'   },
                                Buffers    => sub { $::mode = 'buffers'   },
                                Settings   => sub { $::mode = 'settings'  },
				   },	
                      About      =>{ ITEMORDER => [ 'Ioannis Tambouras (C)' ],
                                     'Ioannis Tambouras (C)'=> sub {1} },
                    };
        new Curses::Widgets::Menu {
                FOREGROUND  => 'black',
                BACKGROUND  => 'red',
                BORDER      => 1,
                FOCUSSWITCH => "\tl",
	        INPUTFUNC   => \&miniscan  ,
                CURSORPOS   => [qw(Databases)],
                MENUS       => $menus,
         }
}


sub init_screen {
	halfdelay(5);
	curs_set(0);
	leaveok(1);
}

sub create_root {
	my $mwh = new Curses;
	$mwh->erase();
	$mwh->keypad(1);
	$mwh->syncok(1);
	$mwh->attrset(COLOR_PAIR(select_colour(qw(red black))));
	$mwh->box(0,0);
	$mwh->attrset(0);
	$mwh->standout();
	$mwh->standend();
	$mwh;
}


sub create_menu {
	new Curses::Widgets::Menu {
		FOREGROUND  => 'yellow',
		BACKGROUND  => 'green',
		BORDER      => 1,
		CURSORPOS   => [qw(File)],
		MENUS       => { MENUORDER  => [qw(File Help)],
		                 File       => {ITEMORDER=>[qw(Open Save Exit)],
	                                        Open      => sub { 1 },
	                                        Save      => sub { 1 },
	                                        Exit      => sub { exit 0 },
	                       },
		Help    => { ITEMORDER => [qw(Help About)],
		             Help      => sub { 1 },
		             About     => sub { 1 },
		           },
	    },

	  };
}
sub create_botton {
	  new Curses::Widgets::ButtonSet {
		  Y           => 2,
		  X           => 2,
		  FOREGROUND  => 'white',
		  BACKGROUND  => 'black',
		  BORDER      => 0,
		  LABELS      => [ qw( OK CANCEL HELP ) ],
		  LENGTH      => 8,
		  HORIZONTAL  => 1,
	  };
}

sub jscan {
	noecho();
        my $mwh = shift;
        my $key = -1;
        while ($key eq -1) {
                $key = $mwh->getch;
                if($key eq 'd') { got_h( $mwh ) }
                if($key eq 'h') { got_h( $mwh ) }
                if($key eq 'j')  { return KEY_DOWN};
                if($key eq 'k')  { return KEY_UP};
                if($key eq ' ')  { return "\n"  };
                if($key eq 'q')  { exit 0       };
        }
        return $key;
}


sub main_listbox {
	my ($title, $list, $y, $x, $lines) = @_;
	$lines or $lines = @$list;
	assert( ref($list), 'ARRAY') if DEBUG;
	new Curses::Widgets::ListBox {
		  Y           => $y,
		  X           => $x,
		  COLUMNS     => 10,
		  LINES       => $lines,
		  LISTITEMS   => $list,
		  MULTISEL    => 0,
		  VALUE       => 0,
		  INPUTFUNC   => \&miniscan,
		  SELECTEDCOL => 'green',
		  CAPTION     => $title,
		  CAPTIONCOL  => 'yellow',
		  FOCUSSWITCH => "\tl",
		  INPUTFUNC   => \&jscan,
	  };
}
sub secondary_listbox {
	my ($title, $list, $y, $x, $lines) = @_;
	$lines or $lines = @$list;
	assert( ref($list), 'ARRAY') if DEBUG;
	new Curses::Widgets::ListBox {
		  Y           => $y,
		  X           => $x,
		  COLUMNS     => 65,
		  COLUMNS     => 40,
		  LINES       => 7,
		  LISTITEMS   => $list,
		  MULTISEL    => 0,
		  INPUTFUNC   => \&miniscan,
		  FOCUSSWITCH => "\tl",
		  SELECTEDCOL => 'green',
		  CAPTION     => $title,
		  CAPTIONCOL  => 'yellow',
		  VALUE       => 0,
	  };
}
sub big_listbox {
	my ($title, $list, $y, $x, $lines) = @_;
	$lines or $lines = @$list;
	assert( ref($list), 'ARRAY') if DEBUG;
	new Curses::Widgets::ListBox {
		  Y           => $y,
		  X           => $x,
		  COLUMNS     => 77,
		  LINES       => 12,
		  LISTITEMS   => $list,
		  MULTISEL    => 0,
		  VALUE       => 0,
		  INPUTFUNC   => \&jscan,
		  FOCUSSWITCH => "\tl",
		  SELECTEDCOL => 'green',
		  CAPTION     => $title,
		  CAPTIONCOL  => 'yellow',
	  };
}
#####################################################################
sub create_mini_root {
        my $mwh = new Curses @_;
        $mwh->erase();
        $mwh->keypad(1);
        $mwh->syncok(1);
        $mwh->attrset(COLOR_PAIR(select_colour(qw(red black))));
        #$mwh->box(0,0);
        $mwh->attrset(0);
        $mwh->standout();
        $mwh->standend();
        $mwh;
}
my $win_secret = create_mini_root ( 20,81,4,0);

sub got_h {
        my $mwh = shift;
        #my $val = $::big->getField('VALUE');
        my $lb_secret  = listbox5 (18,78,0,0)  or return;
        $lb_secret->draw($win_secret,0);
        $lb_secret->execute($win_secret);
        $mwh->refresh;
}


sub listbox5 {
        my ( $lines, $cols, $y,$x) = @_;
	#my $content = retrieve_context ( $::tab, $::mode) or return;
	my $content = retrieve_context ( ) or return;
        new Curses::Widgets::ListBox {
                  Y           => $x||1,
                  X           => $y||3,
                  COLUMNS     => $cols||25,
                  LISTITEMS   => $content,
                  MULTISEL    => 0,
                  LINES       => $lines||5,
                  INPUTFUNC   => \&miniscan,
                  SELECTEDCOL => 'white',
                  CAPTIONCOL  => 'yellow',
                  FOCUSSWITCH =>  "\tdl\n",
                  BORDER      => 0,
                  FOREGROUND  => 'white',
                  BACKGROUND  => 'blue',
                  SELECTEDCOL => 'white',
                  VALUE       =>  0,
          };

}

1;
