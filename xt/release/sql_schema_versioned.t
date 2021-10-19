#!perl

use 5.010001;
use strict;
use warnings;

use App::idxdb;
use Test::More 0.98;
use Test::SQL::Schema::Versioned;
use Test::WithDB::SQLite;

sql_schema_spec_ok(
    $App::idxdb::db_schema_spec,
    Test::WithDB::SQLite->new,
);
done_testing;
