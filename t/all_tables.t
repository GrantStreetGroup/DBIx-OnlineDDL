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

use Path::Class 'file';

use Env qw< ONLINEDDL_TEST_DEBUG CDTEST_MASS_POPULATE CDTEST_DSN CDTEST_DBUSER CDTEST_DBPASS >;

############################################################

my $FILE = file(__FILE__);
my $root = $FILE->dir->parent;
my $db_file = $root->file('t', $FILE->basename.'.db');

my $CHUNK_SIZE = 3;

# Enforce a real file SQLite DB if default
unless ($CDTEST_DSN) {
    $CDTEST_DSN    = "dbi:SQLite:dbname=$db_file";
    $CDTEST_DBUSER = '';
    $CDTEST_DBPASS = '';
    unlink $db_file if -e $db_file;
}

my $dbms_name = CDTest->dbms_name;

############################################################

sub table_based_onlineddl_test ($$&) {
    my ($test_name, $source_name, $test_code) = @_;
    subtest("$source_name: $test_name", sub {
        # Initialize the schema
        my $cd_schema;
        try_ok { $cd_schema = CDTest->init_schema } 'Tables created';
        die 'Schema initialization failed!' if $@;

        my $rsrc = $cd_schema->source($source_name);
        my $rs   = $cd_schema->resultset($source_name);
        my $table_name = $rsrc->name;

        # Acquire the total number of track rows
        my $row_count = $rs->count;

        # Run the tests
        eval { $test_code->($cd_schema) };
        fail 'Test died', $@ if $@;

        # Verify the row counts
        unless ($@) {
            my $new_row_count = $rs->count;
            cmp_ok($new_row_count, '==', $row_count, 'Final row counts are as expected');
        }

        # Clean the schema
        try_ok { CDTest->clean_schema( $cd_schema ) } 'Tables dropped';
    });
}

############################################################

my $blank_schema = CDTest->init_schema( no_connect => 1 );
my @source_names =
    grep { !$blank_schema->source($_)->isa('DBIx::Class::ResultSource::View') }  # no views
    $blank_schema->sources
;

foreach my $source_name (sort @source_names) {
    table_based_onlineddl_test 'No-op', $source_name, sub {
        my $cd_schema  = shift;
        my $rsrc       = $cd_schema->source($source_name);
        my $table_name = $rsrc->name;

        # Constructor
        my $online_ddl = DBIx::OnlineDDL->new(
            rsrc => $rsrc,
            # purposely not adding any coderef_hooks

            copy_opts => {
                chunk_size => $CHUNK_SIZE,
            },
        );

        is $online_ddl->table_name,     $table_name,          'Figured out table_name';
        is $online_ddl->new_table_name, "_${table_name}_new", 'Figured out new_table_name';

        my $helper = $online_ddl->_helper;

        my $orig_table_sql = $helper->create_table_sql($table_name);

        try_ok { $online_ddl->execute } 'Execute works';

        my $new_table_sql  = $helper->create_table_sql($table_name);

        # Remove AUTO_INCREMENT information
        $orig_table_sql =~ s/ AUTO_INCREMENT=\K\d+/###/;
        $new_table_sql  =~ s/ AUTO_INCREMENT=\K\d+/###/;

        is $new_table_sql, $orig_table_sql,  "New table SQL for `$table_name` matches the old one";
    };

    table_based_onlineddl_test 'Add column', $source_name, sub {
        my $cd_schema  = shift;
        my $rsrc       = $cd_schema->source($source_name);
        my $table_name = $rsrc->name;

        # Constructor
        my $online_ddl = DBIx::OnlineDDL->new(
            rsrc => $rsrc,
            coderef_hooks => {
                before_triggers => sub {
                    my $oddl = shift;
                    my $dbh  = $oddl->dbh;
                    my $name = $oddl->new_table_name;

                    my $qname = $dbh->quote_identifier($name);
                    my $qcol  = $dbh->quote_identifier('test_column');

                    $oddl->dbh_runner_do("ALTER TABLE $qname ADD COLUMN $qcol VARCHAR(100) NULL");
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

        my %cols = %{ $dbh->column_info( $catalog, $schema, $table_name, '%' )->fetchall_hashref('COLUMN_NAME') };
        like(
            $cols{test_column},
            {
                TABLE_CAT        => $catalog,
                TABLE_SCHEM      => $schema,
                TABLE_NAME       => $table_name,
                COLUMN_NAME      => 'test_column',
                COLUMN_SIZE      => 100,
                TYPE_NAME        => 'VARCHAR',
                IS_NULLABLE      => 'YES',
                NULLABLE         => 1,
                ORDINAL_POSITION => (scalar $rsrc->columns + 1),
            },
            'New column exists in table',
        );
    };
}

############################################################

unlink $db_file if -e $db_file;

done_testing;
