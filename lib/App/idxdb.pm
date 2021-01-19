package App::idxdb;

# AUTHORITY
# DATE
# DIST
# VERSION

use 5.010001;
use strict;
use warnings;
use Log::ger;

use File::chdir;

sub _set_args_default {
    my $args = shift;
    if (!$args->{dbpath}) {
        require File::HomeDir;
        $args->{dbpath} = File::HomeDir->my_home . '/idxdb.db';
    }
}

sub _connect_db {
    require DBI;

    my ($dbpath, $mode) = @_;

    if ($mode eq 'ro') {
        # avoid creating the database file automatically if we are only in
        # read-only mode
        die "Can't find index '$dbpath', check that path is correct. ".
            "Or maybe you should run the 'update' subcommand first to create the database.\n" unless -f $dbpath;
    }
    log_trace("Connecting to SQLite database at %s ...", $dbpath);
    DBI->connect("dbi:SQLite:database=$dbpath", undef, undef,
                 {RaiseError=>1});
}

sub _init {
    my ($args, $mode) = @_;

    unless ($App::idxdb::state) {
        _set_args_default($args);
        my $state = {
            #dbpath => $args->{dbpath},
            dbh => _connect_db($args->{dbpath}, $mode),
        };
        $App::idxdb::state = $state;
    }
    $App::idxdb::state;
}

our %SPEC;

$SPEC{':package'} = {
    v => 1.1,
    summary => 'Import data from IDX and perform queries on them',
};

our %args_common = (
    dbpath => {
        summary => 'Path for SQLite database',
        description => <<'_',

If not specified, will default to `~/idxdb.db`.

_
        schema => 'str*',
        tags => ['common'],
    },
);

our %arg0_stocks = (
    stocks => {
        'x.name.is_plural' => 1,
        'x.name.singular' => 'stock',
        schema => ['array*', of=>'idx::listed_stock_code*'], # XXX allow unlisted ones too in the future
        req => 1,
        pos => 0,
        slurpy => 1,
    },
);

$SPEC{update} = {
    v => 1.1,
    summary => 'Update data',
    description => <<'_',

Currently this routine imports from text files in the `gudangdata` repository on
the local filesystem. Functionality to import from server directly using
<pm:Finance::SE::IDX> and <pm:Finance::ID::KSEI> will be added in the future.

_
    args => {
        %args_common,
        gudangdata_path => {
            schema => 'dirname*',
            req => 1,
        },
    },
};
sub update {
    require DateTime;
    require DBIx::Util::Schema;
    require JSON::MaybeXS;

    my %args = @_;

    my $gd_path = $args{gudangdata_path};

    my $state = _init(\%args, 'rw');
    my $dbh = $state->{dbh};
    my $now = DateTime->now;

  UPDATE_META:
    {
        my $table_exists = DBIx::Util::Schema::table_exists($dbh, 'meta');
        last if $table_exists;
        $dbh->do("CREATE TABLE meta (name TEXT PRIMARY KEY, value TEXT)");
    }

    my $sth_sel_meta = $dbh->prepare("SELECT value FROM meta WHERE name=?");
    my $sth_upd_meta = $dbh->prepare("INSERT OR REPLACE INTO meta (name,value) VALUES (?,?)");

  UPDATE_STOCK:
    {
        local $CWD = "$gd_path/table/idx_stock";
        my @st = stat "data.tsv" or die "Can't stat $CWD/data.tsv: $!";
        open my $fh, "<", "data.tsv" or die "Can't open $CWD/data.tsv: $!";

        # for simplicity, we replce whole table when updating data
        my $table_exists = DBIx::Util::Schema::table_exists($dbh, 'stock');
        if (!$table_exists) {
            log_info "Creating table 'stock' ...";
            $dbh->do("CREATE TABLE stock (code VARCHAR(4) PRIMARY KEY, sector TEXT NOT NULL, name TEXT NOT NULL, listing_date TEXT NOT NULL, shares DECIMAL NOT NULL, board TEXT NOT NULL)");
        }
        $sth_sel_meta->execute("stock_table_mtime");
        my ($stock_table_mtime) = $sth_sel_meta->fetchrow_array;
        if (!$stock_table_mtime || $stock_table_mtime < $st[9]) {
            my $sth_ins_stock = $dbh->prepare("INSERT INTO stock (code,sector,name,listing_date,shares,board) VALUES (?,?,?,?,?,?)");
            log_info "Updating table 'stock' ...";
            $dbh->begin_work;
            $dbh->do("DELETE FROM stock");
            <$fh>;
            while (my $line = <$fh>) {
                chomp $line;
                $sth_ins_stock->execute(split /\t/, $line);
            }
            $sth_upd_meta->execute("stock_table_mtime", time());
            $dbh->commit;
        }
    }

  UPDATE_DAILY_TRADING_SUMMARY:
    {
        log_info "Updating daily trading summary ...";
        my $table_exists = DBIx::Util::Schema::table_exists($dbh, 'daily_trading_summary');
        my @table_fields;
        if ($table_exists) {
            @table_fields = map { $_->{COLUMN_NAME} } DBIx::Util::Schema::list_columns($dbh, 'daily_trading_summary');
        }
        local $CWD = "$gd_path/table/idx_daily_trading_summary/raw";
      YEAR:
        for my $year (reverse glob("*")) {
            local $CWD = $year;
          FILENAME:
            for my $filename (reverse glob("*.json.gz")) {
                $filename =~ /^(\d{4})(\d{2})(\d{2})/ or die;
                log_trace "Processing file $CWD/$filename ...";
                my $date = "$1-$2-${3}";
                if ($table_exists && $dbh->selectrow_array(q(SELECT 1 FROM daily_trading_summary WHERE "Date" = ?), {}, $date)) {
                    log_debug "Data for date $date already exist, skipping this date";
                    next FILENAME;
                }
                open my $fh, "gzip -cd $filename |" or die "Can't open $filename: $!";
                my $data = JSON::MaybeXS::decode_json(join("", <$fh>));
                $data = $data->[2]; $data = [] if ref $data ne 'ARRAY';
                unless ($table_exists) {
                    log_info "Creating table 'daily_trading_summary' ...";
                    my @field_defs;
                    for my $key (sort keys %{ $data->[0] }) {
                        next if $key =~ /^(No|StockName)$/;
                        my $type;
                        $type = 'DECIMAL' if $key =~ /^(OpenPrice|Close|Previous|High|Low|Change|.*Volume|Previous|FirstTrade|.*Value|.*Frequency|IndexIndividual|Offer.*|Bid.*|.*Shares|Weight.*|Foreign.*)$/;
                        $type //= 'TEXT';
                        push @table_fields, $key;
                        push @field_defs, qq("$key" $type);
                    }
                    push @table_fields, "ctime", "mtime";
                    push @field_defs  , "ctime INT NOT NULL", "mtime INT NOT NULL";
                    $dbh->do("CREATE TABLE daily_trading_summary (".join(", ", @field_defs).")");
                    $dbh->do("CREATE INDEX ix_daily_trading_summary__StockCode ON daily_trading_summary(StockCode)");
                    $dbh->do("CREATE UNIQUE INDEX ix_daily_trading_summary__Date__StockCode ON daily_trading_summary(Date,StockCode)");
                    $table_exists++;
                }

                my $sql = "INSERT INTO daily_trading_summary (".join(",", map {qq("$_")} @table_fields).") VALUES (".join(",", map {"?"} @table_fields).")";
                #log_warn $sql;
                my $sth_ins_daily_trading_summary = $dbh->prepare($sql);
                $dbh->begin_work;
                for my $row (@$data) {
                    $row->{Date} =~ s/T\d.+//;
                    $row->{ctime} = time();
                    $row->{mtime} = time();
                    $sth_ins_daily_trading_summary->execute((map { $row->{$_} } @table_fields));
                }
                $dbh->commit;
            }
        }
    } # UPDATE_DAILY_TRADING_SUMMARY

  UPDATE_OWNERSHIP:
    {
        log_info "Updating stock ownership ...";
        my $table_exists = DBIx::Util::Schema::table_exists($dbh, 'stock_ownership');
        my @table_fields;
        if ($table_exists) {
            @table_fields = map { $_->{COLUMN_NAME} } DBIx::Util::Schema::list_columns($dbh, 'stock_ownership');
        }
        local $CWD = "$gd_path/table/ksei_sec_ownership/raw";
      YEAR:
        for my $year (reverse glob("*")) {
            local $CWD = $year;
          YEARMON:
            for my $yearmon (reverse glob("*")) {
                local $CWD = $yearmon;
                my @txt_files = glob("*.txt");
                unless (@txt_files) {
                    log_debug "Directory $CWD does not contain any .txt files, skipping";
                    next YEARMON;
                }

                my $filename = $txt_files[0];
                $filename =~ /(\d{4})(\d{2})(\d{2})\./ or die;
                log_trace "Processing file $CWD/$filename ...";
                my $date = "$1-$2-${3}";
                if ($table_exists && $dbh->selectrow_array(q(SELECT 1 FROM stock_ownership WHERE "date" = ?), {}, $date)) {
                    log_debug "Data for date $date already exist, skipping this date";
                    next YEARMON;
                }
                open my $fh, "<", $filename or die "Can't open $filename: $!";
                chomp(my $line = <$fh>);
                my @fields = split /\|/, $line;
                for my $f (@fields) {
                    $f =~ s/[^A-Za-z]+//g;
                    if ($f eq 'Total') {
                        # there are two Total columns, the first one is local
                        # total, the second one is foreign total.
                        if (grep { $_ eq 'LocalTotal'} @fields) {
                            $f = 'ForeignTotal';
                        } else {
                            $f = 'LocalTotal';
                        }
                    }
                }

                unless ($table_exists) {
                    my @table_field_defs;
                    push @table_fields    , "date";
                    push @table_field_defs, "date TEXT NOT NULL";
                    for my $f (@fields) {
                        next if $f =~ /^(Date|Type|SecNum)$/;
                        my $type;
                        $type = 'TEXT' if $f =~ /^(Code)$/;
                        $type //= 'DECIMAL';
                        push @table_fields, $f;
                        push @table_field_defs, qq("$f" $type);
                    }
                    push @table_fields    , "ctime", "mtime";
                    push @table_field_defs, "ctime INT NOT NULL", "mtime INT NOT NULL";
                    my $sql = "CREATE TABLE stock_ownership (".join(", ", @table_field_defs).")";
                    #log_warn $sql;
                    $dbh->do($sql);
                    $dbh->do("CREATE INDEX ix_stock_ownership__Code ON stock_ownership(Code)");
                    $dbh->do("CREATE UNIQUE INDEX ix_stock_ownership__date__Code ON stock_ownership(date,Code)");
                    $table_exists++;
                }

                my $sql = "INSERT INTO stock_ownership (".join(",", map {qq("$_")} @table_fields).") VALUES (".join(",", map {"?"} @table_fields).")";
                #log_warn $sql;
                my $sth_ins_stock_ownership = $dbh->prepare($sql);
                $dbh->begin_work;
                while (my $line = <$fh>) {
                    chomp($line);
                    my @row = split /\|/, $line;
                    my $row = {};
                    for (0..$#fields) { $row->{ $fields[$_] } = $row[ $_ ] }
                    next unless $row->{Type} eq 'EQUITY';
                    $row->{date}  = $date;
                    $row->{ctime} = time();
                    $row->{mtime} = time();
                    $sth_ins_stock_ownership->execute((map { $row->{$_} } @table_fields));
                }
                $dbh->commit;
            }
        }
    } # UPDATE_DAILY_TRADING_SUMMARY

    [200];
}

$SPEC{table_ownership} = {
    v => 1.1,
    summary => 'Show ownership of some stock through time',
    args => {
        %arg0_stocks,
    },
};
sub table_ownership {
    my %args = @_;

    my $state = _init(\%args, 'ro');
    my $dbh = $state->{dbh};

    [200];
}

1;
# ABSTRACT:

=head1 SYNOPSIS

See the included CLI script L<idxdb>.


=head1 DESCRIPTION



=head1 SEE ALSO
