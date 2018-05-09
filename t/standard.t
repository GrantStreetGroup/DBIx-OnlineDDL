#!/usr/bin/perl

use lib qw(t/lib);
use strict;
use warnings;

use Test2::Bundle::More;
use Test2::Tools::Compare;
use Test2::Tools::Exception;
use Test2::Tools::Explain;

use DBI;
use DBIx::BatchChunker;
use DBIx::OnlineDDL;
use CDTest;

use Env qw< ONLINEDDL_TEST_DEBUG >;

############################################################

my $CHUNK_SIZE = $ENV{CDTEST_MASS_POPULATE} ? 5000 : 3;

my $dbms_name = CDTest->dbms_name;

sub onlineddl_test ($&) {
    my ($test_name, $test_code) = @_;
    subtest($test_name, sub {
        # Initialize the schema
        my $cd_schema;
        try_ok { $cd_schema = CDTest->init_schema } 'Tables created';
        die 'Schema initialization failed!' if $@;

        my $track_rsrc = $cd_schema->source('Track');
        my $track_rs   = $cd_schema->resultset('Track');

        # One of these tests removes the PK, so use a different unique index
        my $is_drop_pk = $test_name eq 'Drop PK';
        $track_rsrc->_primaries([ qw< cd position > ]) if $is_drop_pk;

        # Acquire the total number of track rows
        my $row_count = $track_rs->count;

        # NOTE: SQLite can't actually do the table drop if there's a leftover cursor mucking
        # about.  (This is more of a SQLite problem than something wrong with OnlineDDL.)
        # MySQL will keep an old snapshot of the data, but that's not a good test of multiple
        # connections hitting the table.
        #
        # So, we are only using single row statements, instead of cursors, to find existing
        # data.
        my $rs_columns = [$is_drop_pk ? () : ('trackid'), qw< cd position >];

        my $iu_track_rs = $track_rs->search(undef, {
            columns  => $rs_columns,
            order_by => { -desc => $rs_columns },
            rows => 1,
        });
        my $del_track_rs = $track_rs->search(undef, {
            columns  => $rs_columns,
            order_by => { -asc => $rs_columns },
            rows => 1,
        });

        # Overload $oddl->dbh, so that every time it's called, it will mess with the original
        # table.  OnlineDDL acquires the $dbh object in just about every method, so this will
        # best simulate real-time usage of the table.
        no warnings 'redefine';
        my $orig_dbh_sub = \&DBIx::OnlineDDL::dbh;
        local *DBIx::OnlineDDL::dbh = sub {
            my $dbh = $orig_dbh_sub->(@_);
            return $dbh unless $dbh;
            my $oddl = shift;

            my $row = $iu_track_rs->first;
            $iu_track_rs->reset;
            return $dbh unless $row;

            my $method = (caller(1))[3];

            # INSERT
            my $cdid = $row->get_column('cd');
            my $pos  = int(rand(2_000_000))+15;

            $track_rs->create({
                cd       => $cdid,
                position => $pos,
                title    => CDTest->_random_words,
            });
            $track_rs->create({
                cd       => $cdid,
                position => $pos + 1,
                title    => CDTest->_random_words,
            });
            $row_count += 2;
            note "During $method: Inserted CD $cdid & POS $pos + ".($pos+1) if $ONLINEDDL_TEST_DEBUG;

            # UPDATE
            my $id_str = $is_drop_pk ? "CD $cdid & POS ".$row->position : 'TrackID '.$row->trackid;

            $row->title( CDTest->_random_words );
            $row->update;
            note "During $method: Updated $id_str" if $ONLINEDDL_TEST_DEBUG;

            $row = $del_track_rs->first;
            $del_track_rs->reset;
            return $dbh unless $row;

            # DELETE
            $id_str = $is_drop_pk ? "CD ".$row->get_column('cd')." & POS ".$row->position : 'TrackID '.$row->trackid;

            $row->delete;
            $row_count--;
            note "During $method: Deleted $id_str" if $ONLINEDDL_TEST_DEBUG;

            # SQLite has an odd trigger bug where an UPDATE and a DELETE on the same ID (possibly
            # within the same connection) causes the UPDATE to happen on the new table, but not
            # the DELETE.  This causes some DELETEs to just get dropped on the floor.  There's a
            # good possibility that this is from https://sqlite.org/src/info/ef360601
            my $vars = $oddl->_vars;
            my $todo;
            $todo = todo 'SQLite trigger weirdness' if
                $dbms_name eq 'SQLite' && $test_name eq 'Add column + title change' && $vars->{new_table_copied}
            ;

            # Verify the row counts
            my ($new_row_count) = $dbh->selectrow_array("SELECT COUNT(*) FROM track");
            cmp_ok($new_row_count, '==', $row_count, "Row counts from 'track' are as expected ($method)");

            if ($vars->{new_table_copied} && !$vars->{new_table_swapped}) {
                my $table_name = $oddl->new_table_name;
                my ($new_row_count) = $dbh->selectrow_array("SELECT COUNT(*) FROM $table_name");
                cmp_ok($new_row_count, '==', $row_count, "Row counts from '$table_name' are as expected ($method)");
            }

            return $dbh;
        };

        # Run the tests
        eval { $test_code->($cd_schema) };
        fail 'Test died', $@ if $@;

        # Verify the row counts
        unless ($@) {
            my $todo;
            $todo = todo 'SQLite trigger weirdness' if $dbms_name eq 'SQLite' && $test_name eq 'Add column + title change';

            my $new_row_count = $track_rs->count;
            cmp_ok($new_row_count, '==', $row_count, 'Final row counts are as expected');
        }

        # Clean the schema
        try_ok { CDTest->clean_schema( $cd_schema ) } 'Tables dropped';
    });
}

############################################################

onlineddl_test 'No-op copy' => sub {
    my $cd_schema  = shift;
    my $track_rsrc = $cd_schema->source('Track');
    my $dbh        = $cd_schema->storage->dbh;

    # Constructor
    my $online_ddl = DBIx::OnlineDDL->new(
        rsrc => $track_rsrc,
        # purposely not adding any coderef_hooks

        copy_opts => {
            chunk_size => $CHUNK_SIZE,
        },
    );

    is $online_ddl->table_name,     'track',      'Figured out table_name';
    is $online_ddl->new_table_name, '_track_new', 'Figured out new_table_name';

    my $orig_table_track_sql  = $online_ddl->_create_table_sql('track');
    my $orig_table_lyrics_sql = $online_ddl->_create_table_sql('lyrics');  # has FK pointing to track

    try_ok { $online_ddl->execute } 'Execute works';

    is $online_ddl->copy_opts->{id_name}, 'trackid', 'Figured out PK';

    my $new_table_track_sql  = $online_ddl->_create_table_sql('track');
    my $new_table_lyrics_sql = $online_ddl->_create_table_sql('lyrics');

    # Remove AUTO_INCREMENT information
    $orig_table_track_sql  =~ s/ AUTO_INCREMENT=\K\d+/###/;
    $orig_table_lyrics_sql =~ s/ AUTO_INCREMENT=\K\d+/###/;
    $new_table_track_sql   =~ s/ AUTO_INCREMENT=\K\d+/###/;
    $new_table_lyrics_sql  =~ s/ AUTO_INCREMENT=\K\d+/###/;

    is $new_table_track_sql,  $orig_table_track_sql,  'New table SQL for `track` matches the old one';
    is $new_table_lyrics_sql, $orig_table_lyrics_sql, 'New table SQL for `lyrics` matches the old one';
};

onlineddl_test 'Add column' => sub {
    my $cd_schema  = shift;
    my $track_rsrc = $cd_schema->source('Track');

    # Constructor
    my $online_ddl = DBIx::OnlineDDL->new(
        rsrc => $track_rsrc,
        coderef_hooks => {
            before_triggers => sub {
                my $oddl = shift;
                my $dbh  = $oddl->dbh;
                my $name = $oddl->new_table_name;

                my $qname = $dbh->quote_identifier($name);
                my $qcol  = $dbh->quote_identifier('test_column');

                $dbh->do("ALTER TABLE $qname ADD COLUMN $qcol VARCHAR(100) NULL");
            },
        },

        copy_opts => {
            chunk_size => $CHUNK_SIZE,
        },
    );

    try_ok { $online_ddl->execute } 'Execute works';

    # Verify the column exists
    my $dbh     = $cd_schema->storage->dbh;
    my $vars    = $online_ddl->_vars;
    my $catalog = $vars->{catalog};
    my $schema  = $vars->{schema};

    my %cols = %{ $dbh->column_info( $catalog, $schema, 'track', '%' )->fetchall_hashref('COLUMN_NAME') };
    like(
        $cols{test_column},
        {
            TABLE_CAT        => $catalog,
            TABLE_SCHEM      => $schema,
            TABLE_NAME       => 'track',
            COLUMN_NAME      => 'test_column',
            COLUMN_SIZE      => 100,
            TYPE_NAME        => 'VARCHAR',
            IS_NULLABLE      => 'YES',
            NULLABLE         => 1,
            ORDINAL_POSITION => 7,
        },
        'New column exists in table',
    );
};

onlineddl_test 'Add column + title change' => sub {
    my $cd_schema  = shift;
    my $track_rsrc = $cd_schema->source('Track');

    # Constructor
    my $online_ddl = DBIx::OnlineDDL->new(
        rsrc => $track_rsrc,
        coderef_hooks => {
            before_triggers => sub {
                my $oddl = shift;
                my $dbh  = $oddl->dbh;
                my $name = $oddl->new_table_name;

                my $qname = $dbh->quote_identifier($name);
                my $qcol  = $dbh->quote_identifier('test_column');
                my $qidx  = $dbh->quote_identifier('track_cd_title');

                $dbh->do("ALTER TABLE $qname ADD COLUMN $qcol VARCHAR(100) NULL");

                # SQLite can't DROP on an ALTER TABLE, but isn't bothered by the breaking of
                # a unique index (for some reason)
                $dbh->do("ALTER TABLE $qname DROP INDEX $qidx") unless $dbms_name eq 'SQLite';
            },
            before_swap => sub {
                my $oddl = shift;
                my $dbh  = $oddl->dbh;
                my $name = $oddl->new_table_name;

                my $qname = $dbh->quote_identifier($name);

                DBIx::BatchChunker->construct_and_execute(
                    chunk_size => $CHUNK_SIZE,
                    process_past_max => 1,

                    min_sth => $dbh->prepare("SELECT MIN(trackid) FROM $qname"),
                    max_sth => $dbh->prepare("SELECT MAX(trackid) FROM $qname"),
                    sth     => $dbh->prepare( join ' ',
                        'UPDATE',
                        $dbh->quote_identifier($name),
                        'SET title =',
                        $dbh->quote('This is the song that never ends'),
                        'WHERE trackid BETWEEN ? AND ?',
                    ),
                );
            },
        },

        copy_opts => {
            chunk_size => $CHUNK_SIZE,
        },
    );

    try_ok { $online_ddl->execute } 'Execute works';

    # Verify the column exists
    my $dbh     = $cd_schema->storage->dbh;
    my $vars    = $online_ddl->_vars;
    my $catalog = $vars->{catalog};
    my $schema  = $vars->{schema};

    my %cols = %{ $dbh->column_info( $catalog, $schema, 'track', '%' )->fetchall_hashref('COLUMN_NAME') };
    like(
        $cols{test_column},
        {
            TABLE_CAT        => $catalog,
            TABLE_SCHEM      => $schema,
            TABLE_NAME       => 'track',
            COLUMN_NAME      => 'test_column',
            COLUMN_SIZE      => 100,
            TYPE_NAME        => 'VARCHAR',
            IS_NULLABLE      => 'YES',
            NULLABLE         => 1,
            ORDINAL_POSITION => 7,
        },
        'New column exists in table',
    );
};

onlineddl_test 'Drop column' => sub {
    plan skip_all => 'SQLite cannot drop columns' if $dbms_name eq 'SQLite';

    my $cd_schema  = shift;
    my $track_rsrc = $cd_schema->source('Track');

    # Constructor
    my $online_ddl = DBIx::OnlineDDL->new(
        rsrc => $track_rsrc,
        coderef_hooks => {
            before_triggers => sub {
                my $oddl = shift;
                my $dbh  = $oddl->dbh;
                my $name = $oddl->new_table_name;

                my $qname = $dbh->quote_identifier($name);
                my $qcol  = $dbh->quote_identifier('last_updated_at');

                $dbh->do("ALTER TABLE $qname DROP COLUMN $qcol");
            },
        },

        copy_opts => {
            chunk_size => $CHUNK_SIZE,
        },
    );

    try_ok { $online_ddl->execute } 'Execute works';

    # Verify the column doesn't exist
    my $dbh     = $cd_schema->storage->dbh;
    my $vars    = $online_ddl->_vars;
    my $catalog = $vars->{catalog};
    my $schema  = $vars->{schema};

    my %cols = %{ $dbh->column_info( $catalog, $schema, 'track', '%' )->fetchall_hashref('COLUMN_NAME') };

    ok(!exists $cols{last_updated_at}, 'Column dropped in table', $cols{last_updated_at});
};

onlineddl_test 'Drop PK' => sub {
    plan skip_all => 'SQLite cannot drop columns' if $dbms_name eq 'SQLite';

    my $cd_schema  = shift;
    my $track_rsrc = $cd_schema->source('Track');

    # Constructor
    my $online_ddl = DBIx::OnlineDDL->new(
        rsrc => $track_rsrc,
        coderef_hooks => {
            before_triggers => sub {
                my $oddl = shift;
                my $dbh  = $oddl->dbh;
                my $name = $oddl->new_table_name;

                my $qname = $dbh->quote_identifier($name);
                my $qcol  = $dbh->quote_identifier('trackid');

                $dbh->do("ALTER TABLE $qname DROP COLUMN $qcol");

                # Need to also drop the FK on lyrics
                my $fk_hash = $oddl->_fk_info_to_hash( $dbh->foreign_key_info(
                    $oddl->_vars->{catalog}, $oddl->_vars->{schema}, $oddl->table_name,
                    undef, undef, undef
                ) );

                $dbh->do(join ' ',
                    'ALTER TABLE',
                    $dbh->quote_identifier('lyrics'),
                    'DROP',
                    # MySQL uses 'FOREIGN KEY' on DROPs, and everybody else uses 'CONSTRAINT' on both
                    ($dbms_name eq 'MySQL' ? 'FOREIGN KEY' : 'CONSTRAINT'),
                    $dbh->quote_identifier( (values %$fk_hash)[0]->{fk_name} ),
                );
            },
        },

        copy_opts => {
            chunk_size => $CHUNK_SIZE,
        },
    );

    try_ok { $online_ddl->execute } 'Execute works';

    # Verify the column doesn't exist
    my $dbh     = $cd_schema->storage->dbh;
    my $vars    = $online_ddl->_vars;
    my $catalog = $vars->{catalog};
    my $schema  = $vars->{schema};

    my %cols = %{ $dbh->column_info( $catalog, $schema, 'track', '%' )->fetchall_hashref('COLUMN_NAME') };

    ok(!exists $cols{trackid}, 'PK column dropped in table', $cols{trackid});
};

############################################################

done_testing;
