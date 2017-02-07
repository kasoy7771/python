#!/usr/bin/perl
use Data::Dumper;
use Getopt::Std;
use POSIX qw(strftime);




#my $str = '07:39.109005-1,SCALL,6,process=rphost,p:processName=ea_5,t:clientID=189,t:applicationName=BackgroundJob,t:computerName=SRV1C5-1,t:connectID=21996,SessionID=12565,Usr=DefUser,ClientID=137,Interface=2ebdaa8c-4a75-48f7-94bf-8206623aa9bb,IName=IClusterLogMngr,Method=0,CallID=41271,MName=writeLogEntryData,Context=\'
# ОбщийМодуль.ОчередьЗаданийСлужебный.Модуль : 680 : ЗапланироватьЗадание(Выборка);
#         ОбщийМодуль.ОчередьЗаданийСлужебный.Модуль : 1559 : НСтр("ru = \'\'Исполняющее задание было принудительно завершено\'\'"));
#                 ОбщийМодуль.ОчередьЗаданийСлужебный.Модуль : 1426 : + ?(ОбщегоНазначенияПовтИсп.ЭтоРазделеннаяКонфигурация() И ОбщегоНазначения.ЭтоРазделенныйОбъектМетаданных(ЗаписьЗадания.Метаданные().ПолноеИмя(),\'
# ';

#my @a = ($str =~ /((?:[\w:]+)=(?:(?:"(?:(?:""|[^"])*)")|(?:'(?:(?:''|[^'])*)')|(?:[^,]*)))/g);





# Инциилизируем необходимые переменные
# Хэш с именами всех свойств ТЖ
my %property_names;
# Получаем хэш с именами всех свойств ТЖ
%property_names = get_property_names();

# Парсим параметры
my %opts;
parse_params();

# Начинаем анализ событий ТЖ
start_analyze();

sub start_analyze {
    # На вход скрипту попадает список глоб файлов тех журнала
    my $i = 0;
    foreach my $glob (@ARGV) {
        #Получаем файлы по каждой глобе
        my @files = glob $glob;
        foreach $file (@files) {
            #Для каждого файла из глобы анализируем кажду строчку
            open(my $fm, $file) or die "can't open file $file $!";
            while (<$fm>){
                #Условие начала нового события по регулярке.
                #Если это начало нового события, то значит предыдущее закончилось, начинаем его анализировать.
                #Накопительную переменную событий обнуляем
                if (substr($_,0,25) =~ /\d\d:\d\d\.\d+-\d+/o) {
                    process_event($e);
                    $e=''}
                $e .= $_;
                $i++;
                if (($i % 10000) == 0) {
                    print $i."\n";
                }
                
            }
        }
    }
}
#Анализируем последнее событие
process_event($e);

sub process_event($) {
    # функция по анализу событий
    my ($str_event) = @_;
    return unless $str_event;
    
    #Получаем из строчного представления хэш со всеми свойствами
    $event = parse_event($str_event);
    logger(Dumper($event), 'd');
    
}

sub parse_event {
    #Хитрожопая функция которая из набора строк из ТЖ делает массив со свойствами события
    
    my ($str_event) = @_;
    chomp $str_event;
    logger("$str_event\n\n", "d");   
    
    my ($key, $value);
    my $event = {};
    
    ($event->{date}, my $other) = split /-/, $str_event, 2;
    ($event->{dur}, $event->{type}, $event->{nesting_level}, $other) = split /,/, $other, 4;
    
    #my @groups = ($other =~ /((?:[\w:]+)=(?:(?:"(?:(?:""|[^"])*)")|(?:'(?:(?:''|[^'])*)')|(?:[^,]*)))/go);
    #
    #foreach my $group (@groups) {
    #    ($key, $value) = split /=/, $group, 2;
    #    $event->{$key} = $value;
    #}
    
    $event
}

#while (<>) {
#    print "$_\n\n";
#    @lines = split /,('|")?/, $_;
#    foreach $line (@lines) {
#        print "$line\n";
#    }
#}

sub parse_params {
     getopts('hd', \%opts);
     if ($opts{h}) {
        print("-d - debug mode");
        print("-v - verbose mode");
     }
     
}

sub logger {
    #Выводит лог в стдаут
    #$level может быть 4х видов.
    #a (all) по умолчанию - выводить всегда
    #d (debug) выводить, когда передан параметр -d
    #v (verbose) выводить, когда передан параметр -v
    #e (extension = verbose + debug) выводить, когда передан параметр -v или -d
    my ($msg, $level) = @_;
    my $print_msg = sub {print( strftime("%a %b %e %H:%M:%S %Y", localtime) . ": $msg")} ;
    if ($level eq 'a' or not $level) {
        $print_msg->();
    }
    elsif ($level eq 'v' and $opts{v} ) {
        $print_msg->();
    }
    elsif ($level eq 'd' and $opts{d} ) {
        $print_msg->();
    }
    elsif ($level eq 'e' and ($opts{v} or $opts{d}) ) {
        $print_msg->();
    }
}

sub get_property_names {
return ('All' => 1,
'AppID' => 1,
'ApplicationExt' => 1,
'Attempts' => 1,
'AvMem' => 1,
'AvgExceptions' => 1,
'Body' => 1,
'BodyText' => 1,
'Class' => 1,
'Component' => 1,
'Context' => 1,
'CurExceptions' => 1,
'Dbms' => 1,
'DBConnStr' => 1,
'DBUsr' => 1,
'DBConnID' => 1,
'dbpid' => 1,
'DeadlockConnectionIntersections' => 1,
'Descr' => 1,
'dumpError' => 1,
'DumpFile' => 1,
'Duration' => 1,
'Durationus' => 1,
'Err' => 1,
'Event' => 1,
'Exception' => 1,
'Finish' => 1,
'FindByString' => 1,
'File' => 1,
'Folder' => 1,
'Files' => 1,
'FilesCount' => 1,
'FilesTotalSize' => 1,
'Func' => 1,
'Headers' => 1,
'Method' => 1,
'Interface' => 1,
'ClientID' => 1,
'CallID' => 1,
'Host' => 1,
'Ib' => 1,
'IBLimit' => 1,
'IName' => 1,
'Info' => 1,
'InBytes' => 1,
'Level' => 1,
'Line' => 1,
'Locks' => 1,
'Method' => 1,
'Memory' => 1,
'MemoryPeak' => 1,
'MessageUid' => 1,
'Name' => 1,
'ATTN' => 1,
'NParams' => 1,
'MaxMemSize' => 1,
'MemSize' => 1,
'MName' => 1,
'MyVer' => 1,
'NeedResync' => 1,
'OSException' => 1,
'OutBytes' => 1,
'PID' => 1,
'Phrase' => 1,
'planSQLText' => 1,
'Process' => 1,
'process' => 1,
'p:processName' => 1,
'Port' => 1,
'ProcessName' => 1,
'Ref' => 1,
'Reason' => 1,
'Regions' => 1,
'Report' => 1,
'Result' => 1,
'Rows' => 1,
'RowsAffected' => 1,
'RunAs' => 1,
'Sdbl' => 1,
'Separation' => 1,
'SepId' => 1,
'ServerComputerName' => 1,
'ServiceName' => 1,
'SessionID' => 1,
'Status' => 1,
'State' => 1,
'srcProcessName' => 1,
'Sql' => 1,
'SrcVer' => 1,
'SrcURL' => 1,
'SyncPort' => 1,
'sz' => 1,
'szd' => 1,
't:applicationName' => 1,
't:clientID' => 1,
't:computerName' => 1,
't:connectID' => 1,
'Time' => 1,
'Timeout' => 1,
'Trans' => 1,
'Txt' => 1,
'URI' => 1,
'Usr' => 1,
'IB' => 1,
'Nmb' => 1,
'Val' => 1,
'Word' => 1,
'WaitConnections' => 1,)
}