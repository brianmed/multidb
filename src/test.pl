#!/usr/bin/perl

use strict;
use warnings;

use v5.16;

use lib "$ENV{HOME}/perl5/lib/perl5";

use Test::More;
use File::Temp;
use File::Slurp;
use IPC::Run qw(run timeout);

my ($dirname) = File::Temp::tempdir("multidb_XXXX", CLEANUP => 0, TMPDIR => 1);

say($dirname);
END {
    say($dirname);
}

my $run = RunSQL->new;

$ENV{MULTIDB_PREFIX} = "$dirname/";

my $sql = read_file("../t/create_joy.sql");
my @cmd = (
    "./cli_multidb",
    "--sql_create",
    $sql
);
my $sql_file;

my ($in, $out, $err, $ret, $code, $cb);

###
say("./cli_multidb -> create_joy.sql");
$ret = run(\@cmd, \$in, \$out, \$err, timeout(10), "create_joy.sql");
$code = $?;

ok($ret, "run create_joy.sql");
unless($ret) {
    BAIL_OUT("fail: create_joy.sql: $code");
}

is($out, "", "STDOUT create_joy.sql");
is($err, "", "STDERR create_joy.sql");

###
say("./cli_multidb -> create_joy.sql");
$ret = run(\@cmd, \$in, \$out, \$err, timeout(10), "create_joy.sql");
$code = $?;

ok(!$ret, "run create_joy.sql");
if($ret) {
    BAIL_OUT("fail: create_joy.sql: $code");
}

is($out, "", "STDOUT create_joy.sql");
like($err, qr/error: schema: joy: already exists/, "STDERR create_joy.sql");

###
$sql_file = "select_joy.sql";
$ret = $run->run_sql_file($sql_file, "select");

ok($ret, "run $sql_file.sql");
unless($ret) {
    BAIL_OUT("fail: $sql_file.sql: $code");
}

my $count = () = $out =~ m/\n/;
is($count, 1, "STDOUT $sql_file.sql");
is($err, "", "STDERR $sql_file.sql");

###
$cb = sub {
    my $this = shift;

    my $count = () = $out =~ m/\n/;
    is($count, 0, "STDOUT");
    is($err, "", "STDERR");
};

foreach my $idx (1 .. 9) {
    state $val = "";
    $val .= $idx;
    $sql = "INSERT INTO joy (id, where, updated, inserted) VALUES (0, '$val', '2014-10-06T23:01', NULL);";
    $run->run_sql($sql, "insert", $cb);
}

$sql = "SELECT id FROM joy WHERE (id > 3 AND id < 5) OR id = 7;";
$cb = sub {
    my $this = shift;
    my ($in, $out, $err) = @_;

    my $count = () = $out =~ m/\n/g;
    is($count, 3, "STDOUT");
    like($out, qr/^(4|7)$/ms, "STDOUT verification");
    is($err, "", "STDERR");
};
$run->run_sql($sql, "select", $cb);

$sql_file = "create.sql";
$cb = sub {
    my $this = shift;
    my ($in, $out, $err) = @_;

    my $count = () = $out =~ m/\n/;
    is($count, 0, "STDOUT $sql_file");
    is($err, "", "STDERR $sql_file");
};
$run->run_sql_file($sql_file, "create", $cb);

$sql_file = "create_site_value.sql";
$cb = sub {
    my $this = shift;
    my ($in, $out, $err) = @_;

    my $count = () = $out =~ m/\n/;
    is($count, 0, "STDOUT $sql_file");
    is($err, "", "STDERR $sql_file");
};
$run->run_sql_file($sql_file, "create", $cb);

$sql = "INSERT INTO site_key (id, site_key, updated, inserted) VALUES (0, 'baseDir', NULL, '2014-10-06T21:01');";
$cb = sub {
    my $this = shift;
    my ($in, $out, $err) = @_;

    my $count = () = $out =~ m/\n/g;
    is($count, 0, "STDOUT");
    like($out, qr/^$/ms, "STDOUT");
    is($err, "", "STDERR");
};
$run->run_sql($sql, "insert", $cb);

$sql = "INSERT INTO site_key (id, site_key, updated, inserted) VALUES (0, 'password', NULL, '2014-10-06T21:01');";
$cb = sub {
    my $this = shift;
    my ($in, $out, $err) = @_;

    my $count = () = $out =~ m/\n/g;
    is($count, 0, "STDOUT");
    like($out, qr/^$/ms, "STDOUT");
    is($err, "", "STDERR");
};
$run->run_sql($sql, "insert", $cb);

$sql = "INSERT INTO site_value (id, site_key_id, site_value, updated, inserted) VALUES (0, 1, '/opt/joy', NULL, '2014-10-06T21:03');";
$cb = sub {
    my $this = shift;
    my ($in, $out, $err) = @_;

    my $count = () = $out =~ m/\n/g;
    is($count, 0, "STDOUT");
    like($out, qr/^$/ms, "STDOUT");
    is($err, "", "STDERR");
};
$run->run_sql($sql, "insert", $cb);

$sql = "INSERT INTO site_value (id, site_key_id, site_value, updated, inserted) VALUES (0, 2, 's#cr!t', NULL, '2014-10-06T21:03');";
$cb = sub {
    my $this = shift;
    my ($in, $out, $err) = @_;

    my $count = () = $out =~ m/\n/g;
    is($count, 0, "STDOUT");
    like($out, qr/^$/ms, "STDOUT");
    is($err, "", "STDERR");
};
$run->run_sql($sql, "insert", $cb);

$sql = "SELECT site_key.site_key, site_value.site_value FROM site_key inner join site_value on site_key.id = site_value.site_key_id WHERE (site_key.id > 1);";
$cb = sub {
    my $this = shift;
    my ($in, $out, $err) = @_;

    my $count = () = $out =~ m/\n/g;
    is($count, 2, "STDOUT");
    like($out, qr/^'password'.*'s#cr!t'$/ms, "STDOUT");
    is($err, "", "STDERR");
};
$run->run_sql($sql, "select", $cb);

$sql = "DELETE FROM site_key WHERE id = 2;";
$cb = sub {
    my $this = shift;
    my ($in, $out, $err) = @_;

    my $count = () = $out =~ m/\n/g;
    is($count, 0, "STDOUT");
    like($out, qr/^$/ms, "STDOUT");
    is($err, "", "STDERR");
};
$run->run_sql($sql, "delete", $cb);

$sql = "SELECT site_key.site_key, site_value.site_value FROM site_key inner join site_value on site_key.id = site_value.site_key_id WHERE (site_key.id > 1);";
$cb = sub {
    my $this = shift;
    my ($in, $out, $err) = @_;

    my $count = () = $out =~ m/\n/g;
    is($count, 1, "STDOUT");
    like($out, qr/^site_key.site_key.*site_value.site_value$/ms, "STDOUT");
    is($err, "", "STDERR");
};
$run->run_sql($sql, "select", $cb);

$sql = "UPDATE site_value SET site_value = '/opt/test' WHERE id =1;";
$cb = sub {
    my $this = shift;
    my ($in, $out, $err) = @_;

    my $count = () = $out =~ m/\n/g;
    is($count, 0, "STDOUT");
    like($out, qr/^$/ms, "STDOUT");
    is($err, "", "STDERR");
};
$run->run_sql($sql, "update", $cb);

$sql = "SELECT site_key.site_key, site_value.site_value FROM site_key inner join site_value on site_key.id = site_value.site_key_id WHERE site_key.id = 1;";
$cb = sub {
    my $this = shift;
    my ($in, $out, $err) = @_;

    my $count = () = $out =~ m/\n/g;
    is($count, 2, "STDOUT");
    like($out, qr/^'baseDir'\s+'\/opt\/test'$/ms, "STDOUT");
    is($err, "", "STDERR");
};
$run->run_sql($sql, "select", $cb);

$sql = "update site_value set site_value = 1 WHERE id = 1 AND;";
$cb = sub {
    my $this = shift;
    my ($in, $out, $err) = @_;

    my $count = () = $out =~ m/\n/g;
    is($count, 0, "STDOUT");
    like($out, qr/^$/ms, "STDOUT");
    like($err, qr/^error: Incomplete AND/, "STDERR");
};
$run->run_sql($sql, "update", $cb, { run_fail => 1 });

done_testing();

package RunSQL;

use Moose;
use File::Slurp;
use IPC::Run qw(run timeout);
use Test::More;

sub run_sql_file
{
    my ($this, $sql_file, $type, $cb) = @_;

    $sql = read_file("../t/$sql_file");
    @cmd = (
        "./cli_multidb",
        "--sql_$type",
        $sql
    );
    say("./cli_multidb -> $sql_file");
    $ret = run(\@cmd, \$in, \$out, \$err, timeout(10), "$sql_file");
    $code = $?;

    ok($ret, "run $sql_file");
    unless($ret) {
        BAIL_OUT("fail: $sql_file: $code\n$out\n$err");
    }

    $cb->($this, $in, $out, $err) if $cb;

    return($ret);
}

sub run_sql
{
    my ($this, $sql, $type, $cb, $ops) = @_;

    @cmd = (
        "./cli_multidb",
        "--sql_$type",
        $sql
    );
    say("./cli_multidb -> $sql");
    $ret = run(\@cmd, \$in, \$out, \$err, timeout(10), "$sql");
    $code = $?;

    if ($ops && $$ops{run_fail}) {
        ok(!$ret, "run $sql");
        if($ret) {
            BAIL_OUT("fail[$code]: $sql: $code\n$out\n$err");
        }
    }
    else {
        ok($ret, "run $sql");
        unless($ret) {
            BAIL_OUT("fail[$code]: $sql: $code\n$out\n$err");
        }
    }

    $cb->($this, $in, $out, $err) if $cb;

    return($ret);
}
