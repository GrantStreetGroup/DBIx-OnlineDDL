package DBIx::OnlineDDL;

our $AUTHORITY = 'cpan:GSG';
our $VERSION   = '0.90';

use v5.10;
use Moo;

use Types::Standard qw( Str Num Bool HashRef CodeRef InstanceOf Dict Optional );

use DBI::Const::GetInfoType;
use DBIx::BatchChunker 0.92;  # with stmt attrs
use Eval::Reversible;
use List::Util        1.33 (qw( uniq any all ));  # has any/all/etc.
use Sub::Util               qw< subname set_subname >;
use Term::ProgressBar 2.14;   # with silent option

use namespace::clean;  # don't export the above

=encoding utf8

=head1 NAME

DBIx::OnlineDDL - Run DDL on online databases safely

=head1 VERSION

version 0.90

=head1 SYNOPSIS

    use DBIx::OnlineDDL;
    use DBIx::BatchChunker;

    DBIx::OnlineDDL->construct_and_execute(
        rsrc          => $dbic_schema->source('Account'),
        ### OR ###
        dbi_connector => $dbix_connector_retry_object,
        table_name    => 'accounts',

        coderef_hooks => {
            # This is the phase where the DDL is actually ran
            before_triggers => \&drop_foobar,

            # Run other operations right before the swap
            before_swap => \&delete_deprecated_accounts,
        },

        process_name => 'Dropping foobar from accounts',

        copy_opts => {
            chunk_size => 5000,
            debug => 1,
        },
    );

    sub drop_foobar {
        my $oddl  = shift;
        my $name  = $oddl->new_table_name;
        my $qname = $oddl->dbh->quote_identifier($name);

        # Drop the 'foobar' column, since it is no longer used
        $oddl->dbh_runner_do("ALTER TABLE $qname DROP COLUMN foobar");
    }

    sub delete_deprecated_accounts {
        my $oddl = shift;
        my $name = $oddl->new_table_name;
        my $dbh  = $oddl->dbh;  # only use for quoting!

        my $qname = $dbh->quote_identifier($name);

        DBIx::BatchChunker->construct_and_execute(
            chunk_size  => 5000,

            debug => 1,

            process_name     => 'Deleting deprecated accounts',
            process_past_max => 1,

            dbic_storage => $oddl->rsrc->storage,
            min_stmt => "SELECT MIN(account_id) FROM $qname",
            max_stmt => "SELECT MAX(account_id) FROM $qname",
            stmt     => join("\n",
                "DELETE FROM $qname",
                "WHERE",
                "    account_type = ".$dbh->quote('deprecated')." AND",
                "    account_id BETWEEN ? AND ?",
            ),
        );
    }

=head1 DESCRIPTION

This is a database utility class for running DDL operations (like C<ALTER TABLE>) safely
on large tables.  It has a similar scope as L<DBIx::BatchChunker>, but is designed for
DDL, rather than DML.  It also has a similar function to other utilities like
L<pt-online-schema-change|https://www.percona.com/doc/percona-toolkit/LATEST/pt-online-schema-change.html> or
L<gh-ost|https://github.com/github/gh-ost>, but actually works properly with foreign
keys, and is written as a Perl module to hook directly into a DBI handle.

Like most online schema change tools, this works by creating a new shell table that looks
just like the old table, running the DDL changes (through the L</before_triggers> hook),
copying data to the new table, and swapping the tables.  Triggers are created to keep the
data in sync.  See L</STEP METHODS> for more information.

The full operation is protected with an L<undo stack|/reversible> via L<Eval::Reversible>.
If any step in the process fails, the undo stack is ran to return the DB back to normal.

This module uses as many of the DBI info methods as possible, along with ANSI SQL in most
places, to be compatible with multiple RDBMS.  So far, it will work with MySQL or SQLite,
but can be expanded to include more systems with a relatively small amount of code
changes.

B<DISCLAIMER:> You should not rely on this class to magically fix any and all locking
problems the DB might experience just because it's being used.  Thorough testing and
best practices are still required.

=head2 When you shouldn't use this module

=head3 Online DDL is already available in the RDBMS

If you're running MySQL 5.6+ without clustering, just use C<LOCK=NONE> for every DDL
statement.  It is seriously simple and guarantees that the table changes you make are not
going to lock the table, or it will fail right away to tell you it's an incompatible
change.

If you're running something like Galera clusters, this typically wouldn't be an option,
as it would lock up the clusters while the C<ALTER TABLE> statement is running, despite
the C<LOCK=NONE> statement.  (Galera clusters were the prime motivation for writing this
module.)

Other RDBMSs may have support for online DDL as well.  Check the documentation first.  If
they don't, patches for this tool are welcome!

=head3 The operation is small

Does your DDL only take 2 seconds?  Just do it!  Don't bother with trying to swap tables
around, wasting time with full table copies, etc.  It's not worth the time spent or risk.

=head3 When you actually want to run DML, not DDL

L<DBIx::BatchChunker> is more appropriate for running DML operations (like C<INSERT>,
C<UPDATE>, C<DELETE>).  If you need to do both, you can use the L</before_triggers> hook
for DDL, and the L</before_swap> hook for DML.  Or just run DBIx::BatchChunker after the
OnlineDDL process is complete.

=head3 Other online schema change tools fit your needs

Don't have foreign key constraints and C<gh-ost> is already working for you?  Great!
Keep using it.

=head1 ATTRIBUTES

=head2 DBIC Attributes

=head3 rsrc

A L<DBIx::Class::ResultSource>.  This will be the source used for all operations, DDL or
otherwise.  Optional, but recommended for DBIC users.

The DBIC storage handler's C<connect_info> will be tweaked to ensure sane defaults and
proper post-connection details.

=cut

has rsrc => (
    is       => 'ro',
    isa      => InstanceOf['DBIx::Class::ResultSource'],
    required => 0,
);

=head3 dbic_retry_opts

A hashref of DBIC retry options.  These options control how retry protection works within
DBIC.  Right now, this is just limited to C<max_attempts>, which controls the number of
times to retry.  The default is 20.

=cut

has dbic_retry_opts => (
    is       => 'ro',
    isa      => HashRef,
    required => 0,
    default  => sub { {} },
);

=head2 DBI Attributes

=head3 dbi_connector

A L<DBIx::Connector::Retry> object.  Instead of L<DBI> statement handles, this is the
recommended non-DBIC way for OnlineDDL (and BatchChunker) to interface with the DBI, as
it handles retries on failures.  The connection mode used is whatever default is set
within the object.

Required, except for DBIC users, who should be setting L</rsrc> above.  It is also
assumed that the correct database is already active.

The object will be tweaked to ensure sane defaults, proper post-connection details, and
a custom C<retry_handler>.

=cut

has dbi_connector => (
    is       => 'ro',
    isa      => InstanceOf['DBIx::Connector::Retry'],
    required => 0,
);

=head3 table_name

The table name to be copied and eventually replaced.  Required unless L</rsrc> is
specified.

=cut

has table_name => (
    is       => 'ro',
    isa      => Str,
    required => 1,
    lazy     => 1,
    default  => sub {
        my $rsrc = shift->rsrc // return;
        $rsrc->from;
    },
);

=head3 new_table_name

The new table name to be created, copied to, and eventually used as the final table.
Optional.

If not defined, a name will be created automatically.  This might be the better route,
since the default builder will search for an unused name in the DB right before OnlineDDL
needs it.

=cut

has new_table_name => (
    is       => 'ro',
    isa      => Str,
    required => 0,
    lazy     => 1,
    builder  => 1,
);

sub _build_new_table_name {
    my $self = shift;
    my $dbh  = $self->dbh;
    my $vars = $self->_vars;

    my $catalog         = $vars->{catalog};
    my $schema          = $vars->{schema};
    my $orig_table_name = $self->table_name;

    my $escape = $dbh->get_info( $GetInfoType{SQL_SEARCH_PATTERN_ESCAPE} ) // '\\';

    return $self->_find_new_identifier(
        "_${orig_table_name}_new" => set_subname('_new_table_name_finder', sub {
            $dbh = shift;
            my $like_expr = shift;
            $like_expr =~ s/([_%])/$escape$1/g;

            $dbh->table_info($catalog, $schema, $like_expr)->fetchrow_array;
        }),
        'SQL_MAXIMUM_TABLE_NAME_LENGTH',
    );
}

=head2 Progress Bar Attributes

=head3 progress_bar

The progress bar used for most of the process.  A different one is used for the actual
table copy with L<DBIx::BatchChunker>, since that step takes longer.

Optional.  If the progress bar isn't specified, a default one will be created.  If the
terminal isn't interactive, the default L<Term::ProgressBar> will be set to C<silent> to
naturally skip the output.

=cut

has progress_bar => (
    is       => 'rw',
    isa      => InstanceOf['Term::ProgressBar'],
);

sub _progress_bar_setup {
    my $self = shift;
    my $vars = $self->_vars;

    my $steps = 6 + scalar keys %{ $self->coderef_hooks };

    my $progress = $self->progress_bar || Term::ProgressBar->new({
        name   => $self->progress_name,
        count  => $steps,
        ETA    => 'linear',
        silent => !(-t *STDERR && -t *STDIN),  # STDERR is what {fh} is set to use
    });

    $vars->{progress_bar} = $progress;
}

=head3 progress_name

A string used to assist in creating a progress bar.  Ignored if L</progress_bar> is
already specified.

This is the preferred way of customizing the progress bar without having to create one
from scratch.

=cut

has progress_name => (
    is       => 'rw',
    isa      => Str,
    required => 0,
    lazy     => 1,
    default  => sub {
        my $table_name = shift->table_name;
        'Altering'.($table_name ? " $table_name" : '');
    },
);

=head2 Other Attributes

=head3 coderef_hooks

An hashref of coderefs.  Each of these are used in different steps in the process.  All
of these are optional, but it is B<highly recommended> that C<before_triggers> is
specified.  Otherwise, you're not actually running any DDL and the table copy is
essentially a no-op.

All of these triggers pass the C<DBIx::OnlineDDL> object as the only argument.  The
L</new_table_name> can be acquired from that and used in SQL statements.  The L</dbh_runner>
and L</dbh_runner_do> methods should be used to protect against disconnections or locks.

There is room to add more hooks here, but only if there's a good reason to do so.
(Running the wrong kind of SQL at the wrong time could be dangerous.)  Create an GitHub
issue if you can think of one.

=head4 before_triggers

This is called before the table triggers are applied.  Your DDL should take place here,
for a few reasons:

    1. The table is empty, so DDL should take no time at all now.

    2. After this hook, the table is reanalyzed to make sure it has an accurate picture
    of the new columns.  This is critical for the creation of the triggers.

=head4 before_swap

This is called after the new table has been analyzed, but before the big table swap.  This
hook might be used if a large DML operation needs to be done while the new table is still
available.  If you use this hook, it's highly recommended that you use something like
L<DBIx::BatchChunker> to make sure the changes are made in a safe and batched manner.

=cut

has coderef_hooks => (
    is       => 'ro',
    isa      => Dict[
        before_triggers => Optional[CodeRef],
        before_swap     => Optional[CodeRef],
    ],
    required => 0,
    default  => sub { +{} },
);

=head3 copy_opts

An hashref of different options to pass to L<DBIx::BatchChunker>, which is used in the
L</copy_rows> step.  Some of these are defined automatically.  It's recommended that you
specify at least these options:

    chunk_size  => 5000,     # or whatever is a reasonable size for that table
    id_name     => 'pk_id',  # especially if there isn't an obvious integer PK

Specifying L<DBIx::BatchChunker/coderef> is not recommended, since Active DBI Processing
mode will be used.

=cut

has copy_opts => (
    is       => 'ro',
    isa      => HashRef,
    required => 0,
    lazy     => 1,
    default  => sub { {} },
);

# This is filled in during copy_rows, since the _column_list call needs to happen after
# the DDL has ran.
sub _fill_copy_opts {
    my $self = shift;
    my $rsrc = $self->rsrc;
    my $dbh  = $self->dbh;
    my $vars = $self->_vars;

    my $copy_opts = $self->copy_opts;

    my $catalog         = $vars->{catalog};
    my $schema          = $vars->{schema};
    my $orig_table_name = $self->table_name;
    my $new_table_name  = $self->new_table_name;

    my $orig_table_name_quote = $dbh->quote_identifier($orig_table_name);
    my $new_table_name_quote  = $dbh->quote_identifier($new_table_name);

    my $dbms_name = $self->_vars->{dbms_name};

    # Figure out what the id_name is going to be
    my $id_name = $copy_opts->{id_name} //= $self->dbh_runner(run => set_subname '_pk_finder', sub {
        $dbh = $_;
        my @ids = $dbh->primary_key($catalog, $schema, $orig_table_name);

        die  "No primary key found for $orig_table_name"                                 unless @ids;
        warn "Using the first column of a multi-column primary key for $orig_table_name" if @ids > 1;

        $ids[0];
    });

    my $id_name_quote = $dbh->quote_identifier($id_name);

    if ($rsrc) {
        $copy_opts->{dbic_storage} //= $rsrc->storage;
        $copy_opts->{rsc} //= $rsrc->resultset->get_column($id_name);

        $copy_opts->{dbic_retry_opts} //= {};
        $copy_opts->{dbic_retry_opts}{max_attempts}  //= 20;
        $copy_opts->{dbic_retry_opts}{retry_handler}   = sub { $self->_retry_handler(@_) };
    }
    else {
        $copy_opts->{dbi_connector} //= $self->dbi_connector;
        $copy_opts->{min_stmt} //= "SELECT MIN($id_name_quote) FROM $orig_table_name_quote";
        $copy_opts->{max_stmt} //= "SELECT MAX($id_name_quote) FROM $orig_table_name_quote";
    }

    my @column_list = $self->_column_list;
    my $column_list_str = join(', ', map { $dbh->quote_identifier($_) } @column_list );

    # The INSERT..SELECT is a bit different depending on the RDBMS used, mostly because
    # of the IGNORE part
    my $insert_select_stmt;
    if    ($dbms_name eq 'MySQL') {
        $insert_select_stmt = join("\n",
            "INSERT IGNORE INTO $new_table_name_quote",
            "    ($column_list_str)",
            "SELECT",
            "    $column_list_str",
            "FROM $orig_table_name_quote",
            "WHERE $id_name_quote BETWEEN ? AND ?",
            "LOCK IN SHARE MODE"
        );
    }
    elsif ($dbms_name eq 'SQLite') {
        $insert_select_stmt = join("\n",
            "INSERT OR IGNORE INTO $new_table_name_quote",
            "    ($column_list_str)",
            "SELECT",
            "    $column_list_str",
            "FROM $orig_table_name_quote",
            "WHERE $id_name_quote BETWEEN ? AND ?",
        );
    }
    else {
        # Fallback to a generic JOIN solution
        my $old_full_id_name_quote = $dbh->quote_identifier(undef, $orig_table_name, $id_name);
        my $new_full_id_name_quote = $dbh->quote_identifier(undef, $new_table_name,  $id_name);

        $insert_select_stmt = join("\n",
            "INSERT INTO $new_table_name_quote",
            "($column_list_str)",
            "SELECT",
            "    $column_list_str",
            "FROM",
            "    $orig_table_name_quote",
            "    LEFT JOIN $new_table_name_quote ON (".join(" = ", $old_full_id_name_quote, $new_full_id_name_quote).")",
            "WHERE",
            "    $old_full_id_name_quote BETWEEN ? AND ? AND",
            "    $new_full_id_name_quote IS NULL",
        );
    }

    $copy_opts->{count_stmt} //= "SELECT COUNT(*) FROM $orig_table_name_quote WHERE $id_name_quote BETWEEN ? AND ?";
    $copy_opts->{stmt}       //= $insert_select_stmt;

    $copy_opts->{progress_name} //= "Copying $orig_table_name" unless $copy_opts->{progress_bar};

    return $copy_opts;
}

=head3 reversible

A L<Eval::Reversible> object, used for rollbacks.  A default will be created, if not
specified.

=cut

has reversible => (
    is       => 'rw',
    isa      => InstanceOf['Eval::Reversible'],
    required => 1,
    lazy     => 1,
    default  => sub { Eval::Reversible->new },
);

has _vars => (
    is       => 'rw',
    isa      => HashRef,
    required => 0,
    init_arg => undef,
    lazy     => 1,
    default  => sub { {} },
);

around BUILDARGS => sub {
    my $next  = shift;
    my $class = shift;

    my %args = @_ == 1 ? %{ $_[0] } : @_;

    # Quick sanity checks
    die 'A DBIC ResultSource (rsrc) or DBIx::Connector::Retry object (dbi_connector) is required' unless (
        $args{rsrc} || $args{dbi_connector}
    );

    $class->$next( %args );
};

sub BUILD {
    my $self = shift;
    my $rsrc = $self->rsrc;

    my $dbh = $self->dbh;

    # Get the current catalog/schema
    my $dbms_name = $self->_vars->{dbms_name} = $dbh->get_info( $GetInfoType{SQL_DBMS_NAME} );

    my $catalog;
    my $schema;
    if    ($dbms_name eq 'MySQL') {
        $catalog = undef;
        ($schema) = $dbh->selectrow_array('SELECT DATABASE()');
    }
    elsif ($dbms_name eq 'SQLite') {
        $catalog = undef;
        my $databases = $dbh->selectall_hashref('PRAGMA database_list', 'seq');
        $schema = $databases->{0}{name};
    }
    else {
        # Try to guess from the DSN parameters
        my %dsn = map { /^(.+)=(.+)$/; lc($1) => $2; } (split /\;/, $dbh->{Name});
        $catalog = $dsn{catalog};
        $schema  = $dsn{database} // $dsn{schema};
    }

    $self->_vars->{catalog} = $catalog;
    $self->_vars->{schema}  = $schema;

    # Add in the post-connection details
    my @stmts = $self->_post_connection_stmts;

    if ($rsrc) {
        ### DBIC Storage

        my @post_connection_details = map { [ do_sql => $_ ] } @stmts;

        # XXX: Tapping into a private attribute here, but it's a lot better than parsing
        # $storage->connect_info.
        my $on_connect_call = $rsrc->storage->_dbic_connect_attributes->{on_connect_call};

        # Parse on_connect_call to make sure we can add to it
        my $ref = defined $on_connect_call && ref $on_connect_call;
        unless ($on_connect_call) {
            $on_connect_call = \@post_connection_details;
        }
        elsif  (!$ref) {
            $on_connect_call = [ [ do_sql => $on_connect_call ], @post_connection_details ];
        }
        elsif  ($ref eq 'ARRAY') {
            # Double-check that we're not repeating ourselves by inspecting the array for
            # our own statements.
            @$on_connect_call = grep {
                my $e = $_;
                !(  # exclude any of ours
                    $e && ref $e && ref $e eq 'ARRAY' && @$e == 2 &&
                    $e->[0] && !ref $e->[0] && $e->[0] eq 'do_sql' &&
                    $e->[1] && !ref $e->[1] && (any { $e->[1] eq $_ } @stmts)
                );
            } @$on_connect_call;

            my $first_occ = $on_connect_call->[0];
            if ($first_occ && ref $first_occ && ref $first_occ eq 'ARRAY') {
                $on_connect_call = [ @$on_connect_call, @post_connection_details ];
            }
            else {
                $on_connect_call = [ $on_connect_call, @post_connection_details ];
            }
        }
        elsif  ($ref eq 'CODE') {
            $on_connect_call = [ $on_connect_call, @post_connection_details ];
        }
        else {
            die "Illegal reftype $ref for on_connect_call connection attribute!";
        }

        $rsrc->storage->_dbic_connect_attributes->{on_connect_call} = $on_connect_call;
    }
    else {
        ### DBIx::Connector::Retry (via DBI Callbacks)

        my $conn      = $self->dbi_connector;
        my $dbi_attrs = $conn->connect_info->[3];

        # Playing with refs, so no need to re-set connect_info
        $conn->connect_info->[3] = $dbi_attrs = {} unless $dbi_attrs;

        # Make sure the basic settings are sane
        $dbi_attrs->{AutoCommit} = 1;
        $dbi_attrs->{RaiseError} = 1;

        # Add the DBI callback
        my $callbacks  = $dbi_attrs->{Callbacks} //= {};
        my $package_re = quotemeta(__PACKAGE__.'::_dbi_connected_callback');

        my $ref = defined $callbacks->{connected} && ref $callbacks->{connected};
        unless ($callbacks->{connected}) {
            $callbacks->{connected} = set_subname '_dbi_connected_callback' => sub {
                shift->do($_) for @stmts;
                return;
            };
        }
        elsif (!$ref || $ref ne 'CODE') {
            die "Illegal reftype $ref for connected DBI Callback!";
        }
        elsif (subname($callbacks->{connected}) =~ /^$package_re/) {  # allow for *_wrapped below
            # This is one of our callbacks; leave it alone!
        }
        else {
            # This is somebody else's callback; wrap around it
            my $old_coderef = $callbacks->{connected};
            $callbacks->{connected} = set_subname '_dbi_connected_callback_wrapped' => sub {
                my $h = shift;
                $old_coderef->($h);
                $h->do($_) for @stmts;
                return;
            };
        }

        # Add a proper retry_handler
        $conn->retry_handler(sub { $self->_retry_handler(@_) });

        # And max_attempts.  XXX: Maybe they actually wanted 10 and not just the default?
        $conn->max_attempts(20) if $conn->max_attempts == 10;
    }

    # Go ahead and run the post-connection statements for this session
    $dbh->{AutoCommit} = 1;
    $dbh->{RaiseError} = 1;
    $dbh->do($_) for @stmts;
}

sub _post_connection_stmts {
    my $self = shift;
    my @stmts;

    my $dbms_name = $self->_vars->{dbms_name};

    if    ($dbms_name eq 'MySQL') {
        @stmts = (
            # Use the right database, just in case it's not in the DSN.
            "USE ".$self->dbh->quote_identifier($self->_vars->{schema}),

            # Foreign key constraints should not interrupt the process.  Nor should they be
            # checked when trying to add or remove them.  This would cause a simple FK DDL
            # to turn into a long-running operation on pre-existing tables.
            'SET SESSION foreign_key_checks=0',

            # Wait timeout for activity.  This is the MySQL default.
            'SET SESSION wait_timeout=28800',

            # Wait 60 seconds for any locks, and retry with the retry_handler.  That'll give us
            # 20 minutes to wait for locks.
            'SET SESSION lock_wait_timeout=60',

            # Only wait 2 seconds for InnoDB row lock wait timeouts, so that OnlineDDL is more
            # likely to be the victim of lock contention.
            'SET SESSION innodb_lock_wait_timeout=2',
        );
    }
    elsif ($dbms_name eq 'SQLite') {
        @stmts = (
            # See FK comment in MySQL section.  FKs in SQLite are a per-connection enabled
            # feature, so this is always a "session" command.
            'PRAGMA foreign_keys = OFF',

            # Only wait 1 second for file contentions.  The downside is that the default is
            # actually 0, so other (non-OnlineDDL) connections should have a setting that is more
            # than that.
            'PRAGMA busy_timeout = 1000',
        );
    }

    return @stmts;
}

=head1 CONSTRUCTORS

See L</ATTRIBUTES> for information on what can be passed into these constructors.

=head2 new

    my $online_ddl = DBIx::OnlineDDL->new(...);

A standard object constructor. If you use this constructor, you will need to manually
call L</execute> to execute the DB changes.

You'll probably just want to use L</construct_and_execute>.

=head2 construct_and_execute

    my $online_ddl = DBIx::OnlineDDL->construct_and_execute(...);

Constructs a DBIx::OnlineDDL object and automatically calls each method step, including
hooks.  Anything passed to this method will be passed through to the constructor.

Returns the constructed object, post-execution.  This is typically only useful if you want
to inspect the attributes after the process has finished.  Otherwise, it's safe to just
ignore the return and throw away the object immediately.

=cut

sub construct_and_execute {
    my $class      = shift;
    my $online_ddl = $class->new(@_);

    $online_ddl->execute;

    return $online_ddl;
}

=head1 METHODS

=head2 Step Runners

=head3 execute

Runs all of the steps as documented in L</STEP METHODS>.  This also includes undo
protection, in case of exceptions.

=cut

sub execute {
    my $self       = shift;
    my $reversible = $self->reversible;

    $self->_progress_bar_setup;

    $reversible->run_reversibly(set_subname '_execute_part_one', sub {
        $self->create_new_table;
        $self->create_triggers;
        $self->copy_rows;
        $self->swap_tables;
    });
    $reversible->run_reversibly(set_subname '_execute_part_two', sub {
        $self->drop_old_table;
        $self->cleanup_foreign_keys;
    });
}

=head3 fire_hook

    $online_ddl->fire_hook('before_triggers');

Fires one of the coderef hooks, if it exists.  This also updates the progress bar.

See L</coderef_hooks> for more details.

=cut

sub fire_hook {
    my ($self, $hook_name) = @_;

    my $hooks = $self->coderef_hooks;
    my $vars  = $self->_vars;

    my $progress = $vars->{progress_bar};

    return unless $hooks && $hooks->{$hook_name};

    $progress->message("Firing hook for $hook_name");

    # Fire the hook
    $hooks->{$hook_name}->($self);

    $progress->update;
}

=head2 DBI Helpers

=head3 dbh

    $online_ddl->dbh;

Acquires a database handle, either from L</rsrc> or L</dbi_connector>.  Not recommended
for active work, as it doesn't offer retry protection.  Instead, use L</dbh_runner> or
L</dbh_runner_do>.

=cut

sub dbh {
    my $self = shift;
    return $self->rsrc ? $self->rsrc->storage->dbh : $self->dbi_connector->dbh;
}

=head3 dbh_runner

    my @items = $online_ddl->dbh_runner(run => sub {
        my $dbh = $_;  # or $_[0]
        $dbh->selectall_array(...);
    });

Runs the C<$coderef>, locally setting C<$_> to and passing in the database handle.  This
is essentially a shortcut interface into either L<dbi_connector> or DBIC's L<BlockRunner|DBIx::Class::Storage::BlockRunner>.

The first argument can either be C<run> or C<txn>, which controls whether to wrap the
code in a DB transaction or not.  The return is passed directly back, and return context
is honored.

=cut

sub _retry_handler {
    my ($self, $runner) = @_;
    my $vars = $self->_vars;

    # NOTE: There's a lot of abusing the fact that BlockRunner and DBIx::Connector::Retry
    # (a la $runner) share similar accessor interfaces.

    my $error     = $runner->last_exception;
    my $dbms_name = $vars->{dbms_name};

    my $is_retryable = 0;

    # Locks/timeouts/etc. problems should still force a retry, so check for these kind of
    # errors.  Disable /x flag to allow for whitespace within string, but turn it on for
    # newlines and comments.
    if    ($dbms_name eq 'MySQL') {
        $is_retryable = $error =~ m<
            # Locks
            (?-x:Deadlock found)|
            (?-x:WSREP detected deadlock\/conflict)|
            (?-x:Lock wait timeout exceeded)|

            # Connections
            (?-x:MySQL server has gone away)|
            (?-x:Lost connection to MySQL server)|

            # Queries
            (?-x:Query execution was interrupted)
        >x;
    }
    elsif ($dbms_name eq 'SQLite') {
        $is_retryable = $error =~ m<
            # Locks
            (?-x:database( table)? is locked)|

            # Connections
            (?-x:attempt to [\w\s]+ on inactive database handle)|

            # Queries
            (?-x:query aborted)|
            (?-x:interrupted)
        >xi;
    }
    else {
        warn "Not sure how to inspect DB errors for $dbms_name systems!";
        return 0;
    }

    if ($is_retryable) {
        my $progress = $vars->{progress_bar};

        # Warn about the last error
        $progress->message("Encountered a recoverable error: $error") if $progress;

        # Pause for a second first, to discourage any future locks
        sleep 1;

        $progress->message( sprintf(
            "Attempt %u of %u",
            $runner->failed_attempt_count,
            $runner->max_attempts,
        ) ) if $progress;
    }

    return $is_retryable;
}

sub dbh_runner {
    my ($self, $method, $coderef) = @_;
    my $wantarray = wantarray;

    die "Only 'txn' or 'run' are acceptable run methods" unless $method =~ /^(?:txn|run)$/;

    my @res;
    if (my $rsrc = $self->rsrc) {
        # No need to load BlockRunner, since DBIC loads it in before us if we're using
        # this method.
        my $block_runner = DBIx::Class::Storage::BlockRunner->new(
            # defaults
            max_attempts => 20,

            # never overrides the important ones below
            %{ $self->dbic_retry_opts },

            retry_handler => sub { $self->_retry_handler(@_) },
            storage  => $rsrc->storage,
            wrap_txn => ($method eq 'txn' ? 1 : 0),
        );

        # This wrapping nonsense is necessary because Try::Tiny within BlockRunner has its own
        # localization of $_.  Fortunately, we can pass arguments to avoid closures.
        my $wrapper = set_subname '_dbh_run_blockrunner_wrapper' => sub {
            my ($s, $c) = @_;
            my $dbh = $s->rsrc->storage->dbh;

            local $_ = $dbh;
            $c->($dbh);  # also pass it in, because that's what DBIx::Connector does
        };

        unless (defined $wantarray) {           $block_runner->run($wrapper, $self, $coderef) }
        elsif          ($wantarray) { @res    = $block_runner->run($wrapper, $self, $coderef) }
        else                        { $res[0] = $block_runner->run($wrapper, $self, $coderef) }
    }
    else {
        my $conn = $self->dbi_connector;
        unless (defined $wantarray) {           $conn->$method($coderef) }
        elsif          ($wantarray) { @res    = $conn->$method($coderef) }
        else                        { $res[0] = $conn->$method($coderef) }
    }

    return $wantarray ? @res : $res[0];
}

=head3 dbh_runner_do

    $online_ddl->dbh_runner_do(
        "ALTER TABLE $table_name ADD COLUMN foobar",
        ["ALTER TABLE ? DROP COLUMN ?", undef, $table_name, 'baz'],
    );

Runs a list of commands, encapulating each of them in a L</dbh_runner> coderef with calls
to L<DBI/do>.  This is handy when you want to run a list of DDL commands, which you don't
care about the output of, but don't want to bundle it into a single non-idempotant
repeatable coderef.  Or if you want to save typing on a single do-able SQL command.

The items can either be a SQL string or an arrayref of options to pass to L<DBI/do>.

The statement is assumed to be non-transactional.  If you want to run a DB transaction,
you should use L</dbh_runner> instead.

=cut

sub dbh_runner_do {
    my ($self, @commands) = @_;

    foreach my $command (@commands) {
        my $ref = ref $command;
        die "$ref references not valid in dbh_runner_do" if $ref && $ref ne 'ARRAY';

        $self->dbh_runner(run => set_subname '_dbh_runner_do', sub {
            $_->do( $ref ? @$command : $command );
        });
    }
}

=head1 STEP METHODS

You can call these methods individually, but using L</construct_and_execute> instead is
highly recommended.  If you do run these yourself, the exception will need to be caught
and the L</reversible> undo stack should be ran to get the DB back to normal.

=head2 create_new_table

Creates the new table, making sure to preserve as much of the original table properties
as possible.

=cut

sub _create_table_sql {
    my $self  = shift;
    my $table = shift;
    my $dbh   = $self->dbh;
    my $vars  = $self->_vars;

    my $qtable = $dbh->quote_identifier($table);

    my $dbms_name = $vars->{dbms_name};
    my $create_sql;
    $self->dbh_runner(run => set_subname '_get_create_table_sql', sub {
        $dbh = $_;
        if    ($dbms_name eq 'MySQL') {
            $create_sql = $dbh->selectrow_hashref("SHOW CREATE TABLE $qtable")->{'Create Table'};
        }
        elsif ($dbms_name eq 'SQLite') {
            ($create_sql) = $dbh->selectrow_array('SELECT sql FROM sqlite_master WHERE name = ?', undef, $table);
        }
        else {
            die "Not sure how to create a new table for $dbms_name systems!";
        }
    });

    return $create_sql;
}

sub create_new_table {
    my $self = shift;
    my $dbh  = $self->dbh;
    my $vars = $self->_vars;

    my $progress   = $vars->{progress_bar};
    my $reversible = $self->reversible;

    my $orig_table_name = $self->table_name;
    my $new_table_name  = $self->new_table_name;

    my $orig_table_name_quote = $dbh->quote_identifier($orig_table_name);
    my $new_table_name_quote  = $dbh->quote_identifier($new_table_name);

    my $dbms_name = $vars->{dbms_name};

    $progress->message("Creating new table $new_table_name");

    my $table_sql = $self->_create_table_sql($orig_table_name);
    die "Table $orig_table_name does not exist in the database!" unless $table_sql;

    if    ($dbms_name eq 'MySQL') {
        # Since MySQL uses a global namespace for foreign keys, these will have to be renamed
        my $iqre = $dbh->get_info( $GetInfoType{SQL_IDENTIFIER_QUOTE_CHAR} ) || '`';
        $iqre = quotemeta $iqre;

        my @fk_names;
        push @fk_names, $1 for ($table_sql =~ /CONSTRAINT ${iqre}([^$iqre\s]+)${iqre} FOREIGN KEY/i);

        foreach my $fk_name (@fk_names) {
            my $new_fk_name = $self->_find_new_identifier(
                "_${fk_name}" => set_subname '_fk_name_finder', sub {
                    $_[0]->selectrow_array(
                        'SELECT table_name FROM information_schema.key_column_usage WHERE constraint_schema = DATABASE() AND constraint_name = ?',
                        undef, $_[1]
                    );
                },
            );
            $vars->{foreign_keys}{orig_names}{"$orig_table_name.$new_fk_name"} = $fk_name;

            my $fk_name_re = quotemeta $fk_name;
            $table_sql =~ s/(?<=CONSTRAINT ${iqre})$fk_name_re(?=${iqre} FOREIGN KEY)/$new_fk_name/;
        }
    }

    # Change the old->new table name
    my $orig_table_name_quote_re = '('.quotemeta($orig_table_name_quote).'|'.quotemeta($orig_table_name).')';
    $table_sql =~ s/(?<=^CREATE TABLE )$orig_table_name_quote_re/$new_table_name_quote/;

    # Actually create the table
    $self->dbh_runner_do($table_sql);

    # Undo commands, including a failure warning update
    $reversible->failure_warning("\nDropping the new table and rolling back to start!\n\n");
    $reversible->add_undo(sub { $self->dbh_runner_do("DROP TABLE $new_table_name_quote") });

    $progress->update;
}

=head2 create_triggers

Creates triggers on the original table to make sure any new changes are captured into the
new table.

=cut

sub create_triggers {
    my $self = shift;
    my $rsrc = $self->rsrc;
    my $dbh  = $self->dbh;
    my $vars = $self->_vars;

    my $progress   = $vars->{progress_bar};
    my $reversible = $self->reversible;

    my $catalog         = $vars->{catalog};
    my $schema          = $vars->{schema};
    my $orig_table_name = $self->table_name;
    my $new_table_name  = $self->new_table_name;

    my $dbms_name = $vars->{dbms_name};

    # Fire the before_triggers hook, which would typically include the DDL
    $self->fire_hook('before_triggers');

    $progress->message("Creating triggers");

    # This shouldn't be cached until now, since the actual DDL may change the column list
    my @column_list = $self->_column_list;

    ### Look for a unique ID set

    # We need to find a proper PK or unique constraint for UPDATE/DELETE triggers.
    # Unlike BatchChunker, we can't just rely on part of a PK.
    my @unique_ids;
    $self->dbh_runner(run => set_subname '_unique_id_finder', sub {
        $dbh = $_;

        my %potential_unique_ids = (
            PRIMARY => [ $dbh->primary_key($catalog, $schema, $orig_table_name) ],
        );
        my $unique_stats = $dbh->can('statistics_info') ?
            $dbh->statistics_info( $catalog, $schema, $orig_table_name, 1, 1 )->fetchall_arrayref({}) :
            []
        ;

        foreach my $index_name (uniq map { $_->{INDEX_NAME} } @$unique_stats) {
            my @unique_cols =
                map  { $_->{COLUMN_NAME} }
                sort { $a->{ORDINAL_POSITION} <=> $b->{ORDINAL_POSITION} }
                grep { $_->{INDEX_NAME} eq $index_name && !$_->{NON_UNIQUE} }  # some DBDs might not honor the $unique_only param
                @$unique_stats
            ;
            $potential_unique_ids{$index_name} = \@unique_cols;
        }

        my %column_set = map { $_ => 1 } @column_list;
        foreach my $index_name ('PRIMARY',
            # sort by the number of columns (asc), though PRIMARY still has top priority
            sort { scalar(@{$potential_unique_ids{$a}}) <=> scalar(@{$potential_unique_ids{$b}}) }
            grep { $_ ne 'PRIMARY' }
            keys %potential_unique_ids
        ) {
            my @unique_cols = @{ $potential_unique_ids{$index_name} };
            next unless @unique_cols;

            # Only use this set if all of the columns exist in both tables
            next unless all { $column_set{$_} } @unique_cols;

            @unique_ids = @unique_cols;
        }
    });

    die "Cannot find an appropriate unique index for $orig_table_name!" unless @unique_ids;

    ### Check to make sure existing triggers aren't on the table

    my @has_triggers_on_table = $self->dbh_runner(run => set_subname '_has_triggers_check', sub {
        $dbh = $_;

        if    ($dbms_name eq 'MySQL') {
            return $dbh->selectrow_array(
                'SELECT trigger_name FROM information_schema.triggers WHERE event_object_schema = DATABASE() AND event_object_table = ?',
                undef, $orig_table_name
            );
        }
        elsif ($dbms_name eq 'SQLite') {
            return $dbh->selectrow_array(
                'SELECT name FROM sqlite_master WHERE type = ? AND tbl_name = ?',
                undef, 'trigger', $orig_table_name
            );
        }
        else {
            die "Not sure how to check for table triggers for $dbms_name systems!";
        }
    });

    die "Found triggers on $orig_table_name!  Please remove them first, so that our INSERT/UPDATE/DELETE triggers can be applied."
        if @has_triggers_on_table;

    ### Find a good set of trigger names

    my $trigger_finder_sub;
    if    ($dbms_name eq 'MySQL') {
        $trigger_finder_sub = set_subname '_trigger_finder_mysql', sub {
            $_[0]->selectrow_array(
                'SELECT trigger_name FROM information_schema.triggers WHERE trigger_schema = DATABASE() AND trigger_name = ?',
                undef, $_[1]
            );
        };
    }
    elsif ($dbms_name eq 'SQLite') {
        $trigger_finder_sub = set_subname '_trigger_finder_sqlite', sub {
            $_[0]->selectrow_array(
                'SELECT name FROM sqlite_master WHERE type = ? AND name = ?',
                undef, 'trigger', $_[1]
            );
        };
    }
    else {
        die "Not sure how to check for table triggers for $dbms_name systems!";
    }

    foreach my $trigger_type (qw< INSERT UPDATE DELETE >) {
        my $trigger_name = $self->_find_new_identifier(
            "${orig_table_name}_onlineddl_".lc($trigger_type) => $trigger_finder_sub
        );
        $vars->{trigger_names}       {$trigger_type} = $trigger_name;
        $vars->{trigger_names_quoted}{$trigger_type} = $dbh->quote_identifier($trigger_name);
    }

    ### Now create the triggers, with (mostly) ANSI SQL

    my $orig_table_name_quote = $dbh->quote_identifier($orig_table_name);
    my $new_table_name_quote  = $dbh->quote_identifier($new_table_name);

    my $column_list_str     = join(', ', map {        $dbh->quote_identifier($_) } @column_list );
    my $new_column_list_str = join(', ', map { "NEW.".$dbh->quote_identifier($_) } @column_list );

    # Using REPLACE just in case the row already exists from the copy
    my $replace_sql = join("\n",
        "REPLACE INTO $new_table_name_quote",
        "    ($column_list_str)",
        "VALUES",
        "    ($new_column_list_str)",
    );

    my $update_unique_where_str = join(' AND ',
        (map {
            join(
                # Use NULL-safe equals, since unique indexes could be nullable.  "IS" is
                # the ANSI equivalent.
                ($dbms_name eq 'MySQL' ? ' <=> ' : ' IS '),
                "OLD.".$dbh->quote_identifier($_),
                "NEW.".$dbh->quote_identifier($_),
            );
        } @unique_ids)
    );

    my $delete_unique_where_str = join(' AND ',
        (map {
            join(
                # Use NULL-safe equals, since unique indexes could be nullable.  "IS" is
                # the ANSI equivalent.
                ($dbms_name eq 'MySQL' ? ' <=> ' : ' IS '),
                "$new_table_name_quote.".$dbh->quote_identifier($_),
                "OLD.".$dbh->quote_identifier($_),
            );
        } @unique_ids)
    );

    # For the UPDATE trigger, DELETE the row, but only if the unique IDs have been
    # changed.  The "NOT ($update_unique_where_str)" part keeps from deleting rows where
    # the unique ID is untouched.
    my $delete_for_update_sql = join("\n",
        "DELETE FROM $new_table_name_quote WHERE",
        "    NOT ($update_unique_where_str) AND",
        "    $delete_unique_where_str"
    );

    my $delete_for_delete_sql = join("\n",
        "DELETE FROM $new_table_name_quote WHERE",
        "    $delete_unique_where_str"
    );

    if ($dbms_name eq 'MySQL') {
        $delete_for_update_sql =~ s/^DELETE/DELETE IGNORE/;
        $delete_for_delete_sql =~ s/^DELETE/DELETE IGNORE/;
    }

    foreach my $trigger_type (qw< INSERT UPDATE DELETE >) {
        my $trigger_header = join(' ',
            "CREATE TRIGGER ".$vars->{trigger_names_quoted}{$trigger_type},
            "AFTER $trigger_type ON $orig_table_name_quote FOR EACH ROW"
        );

        # Even though some of these are just a single SQL statement, not every RDBMS
        # (like SQLite) supports leaving out the BEGIN/END keywords.
        my $trigger_sql = join("\n",
            $trigger_header,
            "BEGIN",
            '',
        );

        if    ($trigger_type eq 'INSERT') {
            # INSERT trigger: Just a REPLACE command
            $trigger_sql .= $replace_sql.';';
        }
        elsif ($trigger_type eq 'UPDATE') {
            # UPDATE trigger: DELETE special unique ID changes, then another REPLACE command.
            $trigger_sql .= join("\n",
                $delete_for_update_sql.';',
                $replace_sql.';',
            );
        }
        elsif ($trigger_type eq 'DELETE') {
            # DELETE trigger: Just a DELETE command
            $trigger_sql .= $delete_for_delete_sql.';';
        }
        $trigger_sql .= "\nEND";

        # DOIT!
        $self->dbh_runner_do($trigger_sql);

        $reversible->add_undo(sub {
            $self->dbh_runner_do( "DROP TRIGGER IF EXISTS ".$self->_vars->{trigger_names_quoted}{$trigger_type} );
        });
    }

    $progress->update;
}

=head2 copy_rows

Fires up a L<DBIx::BatchChunker> process to copy all of the rows from the old table to
the new.

=cut

sub copy_rows {
    my $self = shift;
    my $dbh  = $self->dbh;
    my $vars = $self->_vars;

    my $progress  = $vars->{progress_bar};
    my $copy_opts = $self->_fill_copy_opts;

    $progress->message("Copying all rows to the new table");

    DBIx::BatchChunker->construct_and_execute( %$copy_opts );
    $vars->{new_table_copied} = 1;

    # Analyze the table, since we have a ton of new rows now
    $progress->message("Analyzing table");

    my $dbms_name = $vars->{dbms_name};

    my $new_table_name       = $self->new_table_name;
    my $new_table_name_quote = $dbh->quote_identifier($new_table_name);

    if ($dbms_name eq 'SQLite') {
        $self->dbh_runner_do("ANALYZE $new_table_name_quote");
    }
    else {
        $self->dbh_runner_do("ANALYZE TABLE $new_table_name_quote");
    }

    $progress->update;
}

=head2 swap_tables

With the new table completely modified and set up, this swaps the old/new tables.

=cut

sub swap_tables {
    my $self = shift;
    my $dbh  = $self->dbh;
    my $vars = $self->_vars;

    my $progress   = $vars->{progress_bar};
    my $reversible = $self->reversible;

    my $catalog         = $vars->{catalog};
    my $schema          = $vars->{schema};
    my $orig_table_name = $self->table_name;
    my $new_table_name  = $self->new_table_name;

    my $dbms_name = $vars->{dbms_name};

    my $orig_table_name_quote = $dbh->quote_identifier($orig_table_name);
    my $new_table_name_quote  = $dbh->quote_identifier($new_table_name);

    my $escape = $dbh->get_info( $GetInfoType{SQL_SEARCH_PATTERN_ESCAPE} ) // '\\';

    # Fire the before_swap hook
    $self->fire_hook('before_swap');

    # The existing parent/child FK list needs to be captured prior to the swap.  The FKs
    # have already been created, and possibly changed/deleted, from the new table, so we
    # use that as reference.  They have *not* been re-created on the child tables, so
    # the original table is used as reference.
    my $fk_hash = $vars->{foreign_keys}{definitions} //= {};
    $self->dbh_runner(run => set_subname '_fk_info_query', sub {
        $dbh = $_;
        $fk_hash->{parent} = $self->_fk_info_to_hash( $dbh->foreign_key_info(undef, undef, undef, $catalog, $schema, $new_table_name)  );
        $fk_hash->{child}  = $self->_fk_info_to_hash( $dbh->foreign_key_info($catalog, $schema, $orig_table_name, undef, undef, undef) );
    });

    # Find an "_old" table name first
    my $old_table_name = $vars->{old_table_name} = $self->_find_new_identifier(
        "_${orig_table_name}_old" => set_subname('_old_table_name_finder', sub {
            my ($d, $like_expr) = @_;
            $like_expr =~ s/([_%])/$escape$1/g;

            $d->table_info($catalog, $schema, $like_expr)->fetchrow_array;
        }),
        'SQL_MAXIMUM_TABLE_NAME_LENGTH',
    );
    my $old_table_name_quote = $dbh->quote_identifier($old_table_name);

    $progress->message("Swapping tables ($new_table_name --> $orig_table_name --> $old_table_name)");

    # Let's swap tables!
    if    ($dbms_name eq 'MySQL') {
        $self->dbh_runner_do(
            "RENAME TABLE $orig_table_name_quote TO $old_table_name_quote, $new_table_name_quote TO $orig_table_name_quote"
        );
    }
    elsif ($dbms_name eq 'SQLite') {
        # SQLite does not have a swap table as a single statement, but it does have
        # transactional DDL.
        $self->dbh_runner(txn => set_subname '_table_swap_sqlite', sub {
            $dbh = $_;
            $dbh->do("ALTER TABLE $orig_table_name_quote RENAME TO $old_table_name_quote");
            $dbh->do("ALTER TABLE $new_table_name_quote RENAME TO $orig_table_name_quote");
        });
    }
    else {
        die "Not sure how to swap tables for $dbms_name systems!";
    }

    # Kill the undo stack now, just in case something weird happens between now and the
    # end of the reversibly block.  We've reached a "mostly successful" state, so rolling
    # back here would be undesirable.
    $reversible->clear_undo;
    $vars->{new_table_swapped} = 1;

    $progress->update;
}

=head2 drop_old_table

Drops the old table.  This will also remove old foreign keys on child tables.  (Those FKs
are re-applied to the new table in the next step.)

=cut

sub drop_old_table {
    my $self = shift;
    my $dbh  = $self->dbh;
    my $vars = $self->_vars;

    my $progress   = $vars->{progress_bar};
    my $reversible = $self->reversible;

    my $old_table_name       = $vars->{old_table_name};
    my $old_table_name_quote = $dbh->quote_identifier($old_table_name);

    my $dbms_name = $vars->{dbms_name};

    my $fk_hash = $vars->{foreign_keys}{definitions};

    $reversible->failure_warning( join "\n",
        '',
        "The new table has been swapped, but since the process was interrupted, foreign keys will",
        "need to be cleaned up, and the old table dropped.",
        '',
    );

    # The RDBMS may complain about dangling non-functional FKs if the DROP happens first,
    # so let's remove those child FKs first, and reapply them later.  We turn off FK
    # checks, so these constraint drops are quick and low risk.
    #
    # SQLite doesn't actually support DROP CONSTRAINT, but it doesn't do any messy business with
    # FK renames, either.  So, SQLite can just skip this step.
    unless ($dbms_name eq 'SQLite') {
        $progress->message("Removing FKs from child tables");

        # NOTE: Most of this is ANSI SQL

        foreach my $tbl_fk_name (sort keys %{$fk_hash->{child}}) {
            my $fk = $fk_hash->{child}{$tbl_fk_name};

            $self->dbh_runner_do(join ' ',
                'ALTER TABLE',
                $dbh->quote_identifier( $fk->{fk_table_name} ),
                'DROP',
                # MySQL uses 'FOREIGN KEY' on DROPs, and everybody else uses 'CONSTRAINT' on both
                ($dbms_name eq 'MySQL' ? 'FOREIGN KEY' : 'CONSTRAINT'),
                $dbh->quote_identifier( $fk->{fk_name} ),
            );
        }
    }

    # Now, the actual DROP
    $progress->message("Dropping old table $old_table_name");

    $self->dbh_runner_do("DROP TABLE $old_table_name_quote");

    $progress->update;
}

=head2 cleanup_foreign_keys

Clean up foreign keys on both the new and child tables.

=cut

sub cleanup_foreign_keys {
    my $self = shift;
    my $dbh  = $self->dbh;
    my $vars = $self->_vars;

    my $dbms_name = $vars->{dbms_name};

    my $progress   = $vars->{progress_bar};
    my $reversible = $self->reversible;

    # SQLite doesn't need this step
    if ($dbms_name eq 'SQLite') {
        $progress->update;
        return;
    }

    my $catalog    = $vars->{catalog};
    my $schema     = $vars->{schema};
    my $table_name = $self->table_name;

    my $table_name_quote = $dbh->quote_identifier($table_name);

    my $fk_hash = $vars->{foreign_keys}{definitions};

    $reversible->failure_warning( join "\n",
        '',
        "The new table is live, but since the process was interrupted, foreign keys will need to be",
        "cleaned up.",
        '',
    );

    if ($dbms_name eq 'MySQL') {
        # MySQL has global namespaces for foreign keys, so we are renaming them back to
        # their original names.  The original table has already been dropped, so there's
        # no more risk of bumping into that namespace.

        $progress->message("Renaming parent FKs back to the original constraint names");

        foreach my $tbl_fk_name (sort keys %{$fk_hash->{parent}}) {
            my $fk = $fk_hash->{parent}{$tbl_fk_name};

            my $changed_fk_name = $fk->{fk_name};
            my $orig_fk_name    = $vars->{foreign_keys}{orig_names}{"$table_name.$changed_fk_name"};

            unless ($orig_fk_name) {
                $progress->message("WARNING: Did not find original FK name for $table_name.$changed_fk_name!");
                next;
            }

            # _fk_to_sql uses this directly, so just change it at the $fk hashref
            $fk->{fk_name} = $orig_fk_name;

            $self->dbh_runner_do(join "\n",
                "ALTER TABLE $table_name_quote",
                "    DROP FOREIGN KEY ".$dbh->quote_identifier( $changed_fk_name ).',',
                "    ADD CONSTRAINT ".$self->_fk_to_sql($fk)
            );
        }
    }

    $progress->message("Adding FKs back on child tables");

    # Since we captured the child FK names prior to the swap, they should have the
    # original FK names, even before MySQL's "helpful" changes on "${tbl_name}_ibfk_" FK
    # names.
    foreach my $tbl_fk_name (sort keys %{$fk_hash->{child}}) {
        my $fk = $fk_hash->{child}{$tbl_fk_name};

        $self->dbh_runner_do(join ' ',
            "ALTER TABLE",
            $dbh->quote_identifier( $fk->{fk_table_name} ),
            "ADD CONSTRAINT",
            $self->_fk_to_sql($fk),
        );
    }

    $progress->update;
}

### Private methods

sub _find_new_identifier {
    my ($self, $desired_identifier, $finder_sub, $length_info_str) = @_;
    $length_info_str ||= 'SQL_MAXIMUM_IDENTIFIER_LENGTH';

    state $hash_digits = ['a' .. 'z', '0' .. '9'];

    my $hash = join '', map { $hash_digits->[rand @$hash_digits] } 1 .. 10;

    # Test out some potential names
    my @potential_names = (
        $desired_identifier, "_${desired_identifier}",
        "${desired_identifier}_${hash}", "_${desired_identifier}_${hash}",
        $hash, "_${hash}"
    );

    my $max_len = $self->dbh->get_info( $GetInfoType{$length_info_str} ) || 256;

    my $new_name;
    foreach my $potential_name (@potential_names) {
        $potential_name = substr($potential_name, 0, $max_len);  # avoid the ID name character limit

        my @results = $self->dbh_runner(run => set_subname '_find_new_identifier_dbh_runner', sub {
            $finder_sub->($_, $potential_name);
        });

        # Skip if we found it
        next if @results;

        $new_name = $potential_name;
        last;
    }

    # This really shouldn't happen...
    die "Cannot find a proper identifier name for $desired_identifier!  All of them are taken!" unless defined $new_name;

    return $new_name;
}

sub _column_list {
    my $self = shift;
    my $dbh  = $self->dbh;
    my $vars = $self->_vars;

    my $catalog         = $vars->{catalog};
    my $schema          = $vars->{schema};
    my $orig_table_name = $self->table_name;
    my $new_table_name  = $self->new_table_name;

    my (@old_column_list, @new_column_list);
    $self->dbh_runner(run => set_subname '_column_list_runner', sub {
        $dbh = $_;
        @old_column_list =
            map { $_->{COLUMN_NAME} }
            @{ $dbh->column_info( $catalog, $schema, $orig_table_name, '%' )->fetchall_arrayref({ COLUMN_NAME => 1 }) }
        ;
        @new_column_list =
            map { $_->{COLUMN_NAME} }
            @{ $dbh->column_info( $catalog, $schema, $new_table_name, '%' )->fetchall_arrayref({ COLUMN_NAME => 1 }) }
        ;
    });

    # We only care about columns that exist in both tables.  If a column was added on the
    # new table, there's no data to copy.  If a column was deleted from the new table, we
    # don't care about keeping it.
    my %new_column_set = map { $_ => 1 } @new_column_list;
    return grep { $new_column_set{$_} } @old_column_list;
}

sub _fk_info_to_hash {
    my ($self, $fk_sth) = @_;
    my $vars = $self->_vars;
    my $dbh  = $self->dbh;

    # NOTE: Need to account for alternate ODBC names

    my @fk_rows = @{ $fk_sth->fetchall_arrayref({}) };
    @fk_rows = sort {
        # Sort by FK name, then by the column sequence number
        $a->{FK_NAME} cmp $b->{FK_NAME} ||
        ($a->{KEY_SEQ} // $a->{ORDINAL_POSITION}) <=> ($a->{KEY_SEQ} // $a->{ORDINAL_POSITION})
    } @fk_rows;

    my (%fks, %create_table_sql);
    foreach my $row (@fk_rows) {
        # Some of these rows aren't even FKs
        next unless $row->{PKTABLE_NAME} || $row->{UK_TABLE_CAT};
        next unless $row->{FKTABLE_NAME} || $row->{FK_TABLE_NAME};

        my $fk_name       = $row->{FK_NAME}      // $row->{FKCOLUMN_NAME};
        my $fk_table_name = $row->{FKTABLE_NAME} // $row->{FK_TABLE_NAME};

        my $key = join( '.',
            $row->{PKTABLE_NAME} // $row->{UK_TABLE_CAT},
            $fk_name,
        );

        # Since there may be multiple columns per FK, those associated columns are
        # arrayrefs.
        unless ($fks{$key}) {

            $fks{$key} = {
                fk_name       => $fk_name,

                # The table where the original PK exists
                pk_table_name => $row->{PKTABLE_NAME} // $row->{UK_TABLE_CAT},
                pk_columns    => [ $row->{PKCOLUMN_NAME} // $row->{UK_COLUMN_NAME} ],

                # The table where the FK constraint has been declared
                fk_table_name => $fk_table_name,
                fk_columns    => [ $row->{FKCOLUMN_NAME} // $row->{FK_COLUMN_NAME} ],
            };

            # Sadly, foreign_key_info doesn't always fill in all of the details for the FK, so the
            # CREATE TABLE SQL is actually the better record.  Fortunately, this is all ANSI SQL.
            my $create_table_sql = $create_table_sql{$fk_table_name} //= $self->_create_table_sql($fk_table_name);
            my $fk_name_quote_re = '(?:'.quotemeta( $dbh->quote_identifier($fk_name) ).'|'.quotemeta($fk_name).')';

            if ($create_table_sql =~ m<
                CONSTRAINT \s $fk_name_quote_re \s (      # start capture of full SQL
                    FOREIGN \s KEY \s \( [^\)]+ \) \s     # "FOREIGN KEY" plus column list (which we already have above)
                    REFERENCES \s [^\(]+ \s \( [^\)]+ \)  # "REFERENCES" plus table+column list (again, already captured above)
                    \s? ( [^\)\,]* )                      # ON DELETE/UPDATE, DEFER, MATCH, etc.
                )                                         # end capture of full SQL
            >isx) {
                my ($fk_sql, $extra_sql) = ($1, $2);
                $fk_sql =~ s/^\s+|\s+$//g;

                $fks{$key}{fk_sql}      = $fk_sql;
                $fks{$key}{delete_rule} = $1 if $extra_sql =~ /ON DELETE ((?:SET |NO )?\w+)/i;
                $fks{$key}{update_rule} = $1 if $extra_sql =~ /ON UPDATE ((?:SET |NO )?\w+)/i;
                $fks{$key}{defer}       = $1 if $extra_sql =~ /((?:NOT )?DEFERRABLE(?: INITIALLY \w+)?)/i;
                $fks{$key}{match}       = $1 if $extra_sql =~ /(MATCH \w+)/i;
            }
        }
        else {
            push @{ $fks{$key}{pk_columns} }, $row->{PKCOLUMN_NAME} // $row->{UK_COLUMN_NAME};
            push @{ $fks{$key}{fk_columns} }, $row->{FKCOLUMN_NAME} // $row->{FK_COLUMN_NAME};
        }
    }

    return \%fks;
}

sub _fk_to_sql {
    my ($self, $fk) = @_;
    my $dbh = $self->dbh;

    # Everything after the CONSTRAINT keyword (ANSI SQL)

    if ($fk->{fk_sql}) {
        # Already have most of the SQL
        return join(' ',
            $dbh->quote_identifier($fk->{fk_name}),
            $fk->{fk_sql},
        );
    }

    return join(' ',
        $dbh->quote_identifier($fk->{fk_name}),
        'FOREIGN KEY',
        '('.join(', ', map { $dbh->quote_identifier($_) } @{ $fk->{fk_columns} }).')',
        'REFERENCES',
        $dbh->quote_identifier($fk->{pk_table_name}),
        '('.join(', ', map { $dbh->quote_identifier($_) } @{ $fk->{pk_columns} }).')',
        ( $fk->{match}       ? $fk->{match}                    : () ),
        ( $fk->{delete_rule} ? 'ON DELETE '.$fk->{delete_rule} : () ),
        ( $fk->{update_rule} ? 'ON UPDATE '.$fk->{update_rule} : () ),
        ( $fk->{defer}       ? $fk->{defer}                    : () ),
    );
}

=head1 SEE ALSO

=over

=item *

L<Percona's pt-online-schema-change|https://www.percona.com/doc/percona-toolkit/LATEST/pt-online-schema-change.html>

=item *

L<GitHub's gh-ost|https://github.com/github/gh-ost>

=item *

L<Facebook's OSC|https://www.facebook.com/notes/mysql-at-facebook/online-schema-change-for-mysql/430801045932/>

=item *

L<MySQL's Online DDL|https://dev.mysql.com/doc/refman/5.6/en/innodb-online-ddl.html>

=back

=head1 WHY YET ANOTHER OSC?

The biggest reason is that none of the above fully support foreign key constraints.
Percona's C<pt-osc> comes close, but also includes this paragraph:

    Due to a limitation in MySQL, foreign keys will not have the same names after the ALTER
    that they did prior to it. The tool has to rename the foreign key when it redefines it,
    which adds a leading underscore to the name. In some cases, MySQL also automatically
    renames indexes required for the foreign key.

So, tables swapped with C<pt-osc> are not exactly what they used to be before the swap.
It also had a number of other quirks that just didn't work out for us, related to FKs and
the amount of switches required to make it (semi-)work.

Additionally, by making DBIx::OnlineDDL its own Perl module, it's a lot easier to run
Perl-based schema changes along side L<DBIx::BatchChunker> without having to switch
between Perl and CLI.  If other people want to subclass this module for their own
environment-specific quirks, they have the power to do so, too.

=head1 AUTHOR

Grant Street Group <developers@grantstreet.com>

=head1 LICENSE AND COPYRIGHT

Copyright 2018 Grant Street Group

This program is free software; you can redistribute it and/or modify it
under the terms of the the Artistic License (2.0). You may obtain a
copy of the full license at:

L<http://www.perlfoundation.org/artistic_license_2_0>

=cut

1;
