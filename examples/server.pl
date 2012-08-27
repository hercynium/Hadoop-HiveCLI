#!/usr/bin/env perl
use feature ':5.10.0';
use strict;
use warnings;
use lib 'lib';
use Hadoop::HiveCLI;
use AnyEvent::HTTPD;
use JSON;
use Data::Dumper;

my $port = shift || 9090;
my $httpd = AnyEvent::HTTPD->new(port => $port);

my %hive_procs;

my $runner = Hadoop::HiveCLI->new();

my $hql = q{select 1 from t_foo};

$httpd->reg_cb(
  '/create-job' => sub {
    my ($httpd, $req) = @_;
    $hql = $req->parm('hql') // $hql;
    my $proc = $runner->run( hql => $hql );
    my $pid = $proc->pid; 
    $hive_procs{$pid} = $proc;
    my $res_json = encode_json { pid => $pid, status => $proc->state };
    $req->respond({ content => [ 'application/json', "$res_json\n" ] } );
  },
  '/jobs' => sub {
    my ($httpd, $req) = @_;
    my $res_json = encode_json { pids => [ keys %hive_procs ] };
    $req->respond({ content => [ 'application/json', "$res_json\n" ] } );
  },
  '/job' => sub {
    my ($httpd, $req) = @_;
    my $pid = ($req->url->path_segments)[-1];
    my $proc = $hive_procs{$pid} or do { 
      return $req->respond( res_json_404( 'pid not found', {} ) );
    };
    my $res_json = encode_json $proc->info;
    $req->respond( { content => [ 'application/json', "$res_json\n" ] } );
  },
  '/kill-job' => sub {
    my ($httpd, $req) = @_;
    my $pid = ($req->url->path_segments)[-1];
    delete $hive_procs{$pid} or do { 
      return $req->respond( res_json_404( 'pid not found', {} ) );
    };
    my $res_json = encode_json {};
    $req->respond( { content => [ 'application/json', "$res_json\n" ] } );
  },
  '/clean-job' => sub {
    my ($httpd, $req) = @_;
    my $pid = ($req->url->path_segments)[-1]; 
    delete $hive_procs{$pid} or do { 
      return $req->respond( res_json_404( 'pid not found', {} ) );
    };
    my $res_json = encode_json {};
    $req->respond( { content => [ 'application/json', "$res_json\n" ] } );
  },
  '' => sub {
    my ($httpd, $req) = @_;
    $req->respond( res_json_404('nothing to see here', {}) );
  },
);

sub res_json_404 {
  my ($msg, $resp) = @_;
  return [404, $msg, { 'Content-Type' => 'application/json' }, encode_json($resp)."\n" ];
}

$httpd->run;
