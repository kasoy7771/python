#! /usr/bin/perl

use Getopt::Std;
my %opts;

# Usage
# cat /var/log/srv1cv8/rphost_*/* | perl contexts_by_events.pl -e DBPOSTGRS

parse_args();
parse_strings();
print_rez();

sub print_rez {
    if ($opts{a}) {
        print_rez_avg();
    }
    else {
        print_rez_default();
        }
}

sub parse_strings {
    while (<>) {
            if (/\d\d:\d\d\.\d+/)
            { if ($e=~/\d\d:\d\d\.\d+-(\d+),$opts{e},/i) {
                  #print $e;
                  my $multilline = $e =~ /Context='/;
                  my $dur, $cont;
                  if ($multilline) {
                    ($dur, $cont) = ($e =~ /\d\d:\d\d\.\d+-(\d+).+Context=(.+)/sg)[0,1];
                  }
                  else {
                    ($dur, $cont) = ($e =~ /\d\d:\d\d\.\d+-(\d+).+Context=([^,]+)/sg)[0,1];
                    # print "$_\n";
                    # print "dur is $dur cont is $cont\n";
                  }
                  @lines = split /\n/, $cont;
                  $last_cont = $lines[$#lines];
                  $last_cont =~ s/^\s+//;
                  $rez{$last_cont}{count}++;
                  $rez{$last_cont}{dur}+=$dur;}
              $e="";}
            $e.=$_;
    }
}

sub print_rez_default {      
    printf("%16s \t %16s \t %16s \t %50s\n", "Суммарная длительность", "Количество", "Срднее время", "Событие", $_); 
    foreach (sort {$rez{$b}{dur} <=> $rez{$a}{dur}} keys %rez) {
            printf("%16d \t %16d \t %50s\n", $rez{$_}{dur}, $rez{$_}{count}, $_);
    }
}

sub print_rez_avg {     
    printf("%16s \t %16s \t %16s \t %50s\n", "Суммарная длительность", "Количество", "Срднее время", "Событие", $_); 
    foreach ( sort {$rez{$b}{dur} <=> $rez{$a}{dur}} 
              # sort {$rez{$b}{dur}/$rez{$b}{count} <=> $rez{$a}{dur}/$rez{$a}{count}}
             keys %rez) {
            printf("%16d \t %16d \t %16d \t %50s\n", $rez{$_}{dur}, $rez{$_}{count}, $rez{$_}{dur}/$rez{$_}{count}, $_);
    }
}

sub parse_args {
    getopts('he:a',\%opts);
    if ($opts{h}) {
        print ("-h print this message\n");
        print ("-e event type. Example: contexts.pl -e SDBL\n");
        print ("-a average output format. \n");
        exit(0);
    }
   
}
