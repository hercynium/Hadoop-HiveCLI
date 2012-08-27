#!/usr/bin/env perl
use strict;
use warnings;
use feature ':5.10.0';
use lib 'lib';
use AnyEvent;
use Data::Dumper;
use Hadoop::HiveCLI;

my $query = q{select 1 from t_foo};

# set up a hive-runner
my $hive = Hadoop::HiveCLI->new( conf => { foo => 'bar' } );

# run the query, using the parameters provided in both the runner
# and in this method-call
my $task = $hive->run(
  conf  => { 'mapred.job.queue.name' => 'default' },
  hql => $query,
);

my $cv = AnyEvent->condvar;
# spit out some status info every 10 seconds until the task is done
my $t; $t = AnyEvent->timer(after => 1, interval=> 10, cb => sub {
    $cv->send if $task->state eq 'finished';
    say Dumper($task->info);
});
$task->wait;
$cv->recv;
say "DONE";
