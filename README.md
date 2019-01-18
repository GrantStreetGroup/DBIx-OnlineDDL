# NAME

DBIx::OnlineDDL - Run DDL on online databases safely

# VERSION

version 0.90

# SYNOPSIS

```perl
use DBIx::OnlineDDL;
use DBIx::BatchChunker;

DBIx::OnlineDDL->construct_and_execute(
    rsrc          => $dbic_schema->source('Account'),
    ### OR ###
    dbi_connector => $dbix_connector_retry_object,
    table_name    => 'accounts',

    coderef_hooks => {
        # This is the phase where the DDL is actually run
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
```

# DESCRIPTION

This is a database utility class for running DDL operations (like `ALTER TABLE`) safely
on large tables.  It has a similar scope as [DBIx::BatchChunker](https://metacpan.org/pod/DBIx::BatchChunker), but is designed for
DDL, rather than DML.  It also has a similar function to other utilities like
[pt-online-schema-change](https://www.percona.com/doc/percona-toolkit/LATEST/pt-online-schema-change.html) or
[gh-ost](https://github.com/github/gh-ost), but actually works properly with foreign
keys, and is written as a Perl module to hook directly into a DBI handle.

Like most online schema change tools, this works by creating a new shell table that looks
just like the old table, running the DDL changes (through the ["before\_triggers"](#before_triggers) hook),
copying data to the new table, and swapping the tables.  Triggers are created to keep the
data in sync.  See ["STEP METHODS"](#step-methods) for more information.

The full operation is protected with an [undo stack](#reversible) via [Eval::Reversible](https://metacpan.org/pod/Eval::Reversible).
If any step in the process fails, the undo stack is run to return the DB back to normal.

This module uses as many of the DBI info methods as possible, along with ANSI SQL in most
places, to be compatible with multiple RDBMS.  So far, it will work with MySQL or SQLite,
but can be expanded to include more systems with a relatively small amount of code
changes.  (See [DBIx::OnlineDDL::Helper::Base](https://metacpan.org/pod/DBIx::OnlineDDL::Helper::Base) for details.)

**DISCLAIMER:** You should not rely on this class to magically fix any and all locking
problems the DB might experience just because it's being used.  Thorough testing and
best practices are still required.

## When you shouldn't use this module

### Online DDL is already available in the RDBMS

If you're running MySQL 5.6+ without clustering, just use `LOCK=NONE` for every DDL
statement.  It is seriously simple and guarantees that the table changes you make are not
going to lock the table, or it will fail right away to tell you it's an incompatible
change.

If you're running something like Galera clusters, this typically wouldn't be an option,
as it would lock up the clusters while the `ALTER TABLE` statement is running, despite
the `LOCK=NONE` statement.  (Galera clusters were the prime motivation for writing this
module.)

Other RDBMSs may have support for online DDL as well.  Check the documentation first.  If
they don't, patches for this tool are welcome!

### The operation is small

Does your DDL only take 2 seconds?  Just do it!  Don't bother with trying to swap tables
around, wasting time with full table copies, etc.  It's not worth the time spent or risk.

### When you actually want to run DML, not DDL

[DBIx::BatchChunker](https://metacpan.org/pod/DBIx::BatchChunker) is more appropriate for running DML operations (like `INSERT`,
`UPDATE`, `DELETE`).  If you need to do both, you can use the ["before\_triggers"](#before_triggers) hook
for DDL, and the ["before\_swap"](#before_swap) hook for DML.  Or just run DBIx::BatchChunker after the
OnlineDDL process is complete.

### Other online schema change tools fit your needs

Don't have foreign key constraints and `gh-ost` is already working for you?  Great!
Keep using it.

# ATTRIBUTES

## DBIC Attributes

### rsrc

A [DBIx::Class::ResultSource](https://metacpan.org/pod/DBIx::Class::ResultSource).  This will be the source used for all operations, DDL or
otherwise.  Optional, but recommended for DBIC users.

The DBIC storage handler's `connect_info` will be tweaked to ensure sane defaults and
proper post-connection details.

### dbic\_retry\_opts

A hashref of DBIC retry options.  These options control how retry protection works within
DBIC.  Right now, this is just limited to `max_attempts`, which controls the number of
times to retry.  The default `max_attempts` is 20.

## DBI Attributes

### dbi\_connector

A [DBIx::Connector::Retry](https://metacpan.org/pod/DBIx::Connector::Retry) object.  Instead of [DBI](https://metacpan.org/pod/DBI) statement handles, this is the
recommended non-DBIC way for OnlineDDL (and BatchChunker) to interface with the DBI, as
it handles retries on failures.  The connection mode used is whatever default is set
within the object.

Required, except for DBIC users, who should be setting ["rsrc"](#rsrc) above.  It is also
assumed that the correct database is already active.

The object will be tweaked to ensure sane defaults, proper post-connection details, a
custom `retry_handler`, and set a default `max_attempts` of 20, if not already set.

### table\_name

The table name to be copied and eventually replaced.  Required unless ["rsrc"](#rsrc) is
specified.

### new\_table\_name

The new table name to be created, copied to, and eventually used as the final table.
Optional.

If not defined, a name will be created automatically.  This might be the better route,
since the default builder will search for an unused name in the DB right before OnlineDDL
needs it.

## Progress Bar Attributes

### progress\_bar

The progress bar used for most of the process.  A different one is used for the actual
table copy with [DBIx::BatchChunker](https://metacpan.org/pod/DBIx::BatchChunker), since that step takes longer.

Optional.  If the progress bar isn't specified, a default one will be created.  If the
terminal isn't interactive, the default [Term::ProgressBar](https://metacpan.org/pod/Term::ProgressBar) will be set to `silent` to
naturally skip the output.

### progress\_name

A string used to assist in creating a progress bar.  Ignored if ["progress\_bar"](#progress_bar) is
already specified.

This is the preferred way of customizing the progress bar without having to create one
from scratch.

## Other Attributes

### coderef\_hooks

A hashref of coderefs.  Each of these are used in different steps in the process.  All
of these are optional, but it is **highly recommended** that `before_triggers` is
specified.  Otherwise, you're not actually running any DDL and the table copy is
essentially a no-op.

All of these triggers pass the `DBIx::OnlineDDL` object as the only argument.  The
["new\_table\_name"](#new_table_name) can be acquired from that and used in SQL statements.  The ["dbh\_runner"](#dbh_runner)
and ["dbh\_runner\_do"](#dbh_runner_do) methods should be used to protect against disconnections or locks.

There is room to add more hooks here, but only if there's a good reason to do so.
(Running the wrong kind of SQL at the wrong time could be dangerous.)  Create a GitHub
issue if you can think of one.

#### before\_triggers

This is called before the table triggers are applied.  Your DDL should take place here,
for a few reasons:

```
1. The table is empty, so DDL should take no time at all now.

2. After this hook, the table is reanalyzed to make sure it has an accurate picture
of the new columns.  This is critical for the creation of the triggers.
```

#### before\_swap

This is called after the new table has been analyzed, but before the big table swap.  This
hook might be used if a large DML operation needs to be done while the new table is still
available.  If you use this hook, it's highly recommended that you use something like
[DBIx::BatchChunker](https://metacpan.org/pod/DBIx::BatchChunker) to make sure the changes are made in a safe and batched manner.

### copy\_opts

A hashref of different options to pass to [DBIx::BatchChunker](https://metacpan.org/pod/DBIx::BatchChunker), which is used in the
["copy\_rows"](#copy_rows) step.  Some of these are defined automatically.  It's recommended that you
specify at least these options:

```perl
chunk_size  => 5000,     # or whatever is a reasonable size for that table
id_name     => 'pk_id',  # especially if there isn't an obvious integer PK
```

Specifying ["coderef" in DBIx::BatchChunker](https://metacpan.org/pod/DBIx::BatchChunker#coderef) is not recommended, since Active DBI Processing
mode will be used.

These options will be included into the hashref, unless specifically overridden by key
name:

```perl
id_name      => $first_pk_column,  # will warn if the PK is multi-column
target_time  => 1,
sleep        => 0.5,

# If using DBIC
dbic_storage => $rsrc->storage,
rsc          => $id_rsc,
dbic_retry_opts => {
    max_attempts  => 20,
    # best not to change this, unless you know what you're doing
    retry_handler => $onlineddl_retry_handler,
},

# If using DBI
dbi_connector => $oddl->dbi_connector,
min_stmt      => $min_sql,
max_stmt      => $max_sql,

# For both
count_stmt    => $count_sql,
stmt          => $insert_select_sql,
progress_name => $copying_msg,
```

### db\_timeouts

A hashref of timeouts used for various DB operations, and usually set at the beginning of
each connection.  Some of these settings may be RDBMS-specific.

#### lock\_file

Amount of time (in seconds) to wait when attempting to acquire filesystem locks (on
filesystems which support locking).  Float or fractional values are allowed.  This
currently only applies to SQLite.

Default value is 1 second.  The downside is that the SQLite default is actually 0, so
other (non-OnlineDDL) connections should have a setting that is more than that to prevent
lock contention.

#### lock\_db

Amount of time (in whole seconds) to wait when attempting to acquire table and/or database
level locks before falling back to retry.

Default value is 60 seconds.

#### lock\_row

Amount of time (in whole seconds) to wait when attempting to acquire row-level locks,
which apply to much lower-level operations than ["lock\_db"](#lock_db).  At this scope, the lesser
of either of these two settings will take precedence.

Default value is 2 seconds.  Lower values are preferred for row lock wait timeouts, so
that OnlineDDL is more likely to be the victim of lock contention.  OnlineDDL can simply
retry the connection at that point.

#### session

Amount of time (in whole seconds) for inactive session timeouts on the database side.

Default value is 28,800 seconds (8 hours), which is MySQL's default.

### reversible

A [Eval::Reversible](https://metacpan.org/pod/Eval::Reversible) object, used for rollbacks.  A default will be created, if not
specified.

# CONSTRUCTORS

See ["ATTRIBUTES"](#attributes) for information on what can be passed into these constructors.

## new

```perl
my $online_ddl = DBIx::OnlineDDL->new(...);
```

A standard object constructor. If you use this constructor, you will need to manually
call ["execute"](#execute) to execute the DB changes.

You'll probably just want to use ["construct\_and\_execute"](#construct_and_execute).

## construct\_and\_execute

```perl
my $online_ddl = DBIx::OnlineDDL->construct_and_execute(...);
```

Constructs a DBIx::OnlineDDL object and automatically calls each method step, including
hooks.  Anything passed to this method will be passed through to the constructor.

Returns the constructed object, post-execution.  This is typically only useful if you want
to inspect the attributes after the process has finished.  Otherwise, it's safe to just
ignore the return and throw away the object immediately.

# METHODS

## Step Runners

### execute

Runs all of the steps as documented in ["STEP METHODS"](#step-methods).  This also includes undo
protection, in case of exceptions.

### fire\_hook

```
$online_ddl->fire_hook('before_triggers');
```

Fires one of the coderef hooks, if it exists.  This also updates the progress bar.

See ["coderef\_hooks"](#coderef_hooks) for more details.

## DBI Helpers

### dbh

```
$online_ddl->dbh;
```

Acquires a database handle, either from ["rsrc"](#rsrc) or ["dbi\_connector"](#dbi_connector).  Not recommended
for active work, as it doesn't offer retry protection.  Instead, use ["dbh\_runner"](#dbh_runner) or
["dbh\_runner\_do"](#dbh_runner_do).

### dbh\_runner

```perl
my @items = $online_ddl->dbh_runner(run => sub {
    my $dbh = $_;  # or $_[0]
    $dbh->selectall_array(...);
});
```

Runs the `$coderef`, locally setting `$_` to and passing in the database handle.  This
is essentially a shortcut interface into either [dbi\_connector](https://metacpan.org/pod/dbi_connector) or DBIC's [BlockRunner](https://metacpan.org/pod/DBIx::Class::Storage::BlockRunner).

The first argument can either be `run` or `txn`, which controls whether to wrap the
code in a DB transaction or not.  The return is passed directly back, and return context
is honored.

### dbh\_runner\_do

```
$online_ddl->dbh_runner_do(
    "ALTER TABLE $table_name ADD COLUMN foobar",
    ["ALTER TABLE ? DROP COLUMN ?", undef, $table_name, 'baz'],
);
```

Runs a list of commands, encapsulating each of them in a ["dbh\_runner"](#dbh_runner) coderef with calls
to ["do" in DBI](https://metacpan.org/pod/DBI#do).  This is handy when you want to run a list of DDL commands, which you don't
care about the output of, but don't want to bundle them into a single non-idempotant
repeatable coderef.  Or if you want to save typing on a single do-able SQL command.

The items can either be a SQL string or an arrayref of options to pass to ["do" in DBI](https://metacpan.org/pod/DBI#do).

The statement is assumed to be non-transactional.  If you want to run a DB transaction,
you should use ["dbh\_runner"](#dbh_runner) instead.

# STEP METHODS

You can call these methods individually, but using ["construct\_and\_execute"](#construct_and_execute) instead is
highly recommended.  If you do run these yourself, the exception will need to be caught
and the ["reversible"](#reversible) undo stack should be run to get the DB back to normal.

## create\_new\_table

Creates the new table, making sure to preserve as much of the original table properties
as possible.

## create\_triggers

Creates triggers on the original table to make sure any new changes are captured into the
new table.

## copy\_rows

Fires up a [DBIx::BatchChunker](https://metacpan.org/pod/DBIx::BatchChunker) process to copy all of the rows from the old table to
the new.

## swap\_tables

With the new table completely modified and set up, this swaps the old/new tables.

## drop\_old\_table

Drops the old table.  This will also remove old foreign keys on child tables.  (Those FKs
are re-applied to the new table in the next step.)

## cleanup\_foreign\_keys

Clean up foreign keys on both the new and child tables.

# SEE ALSO

- [Percona's pt-online-schema-change](https://www.percona.com/doc/percona-toolkit/LATEST/pt-online-schema-change.html)
- [GitHub's gh-ost](https://github.com/github/gh-ost)
- [Facebook's OSC](https://www.facebook.com/notes/mysql-at-facebook/online-schema-change-for-mysql/430801045932/)
- [MySQL's Online DDL](https://dev.mysql.com/doc/refman/5.6/en/innodb-online-ddl.html)

# WHY YET ANOTHER OSC?

The biggest reason is that none of the above fully support foreign key constraints.
Percona's `pt-osc` comes close, but also includes this paragraph:

```
Due to a limitation in MySQL, foreign keys will not have the same names after the ALTER
that they did prior to it. The tool has to rename the foreign key when it redefines it,
which adds a leading underscore to the name. In some cases, MySQL also automatically
renames indexes required for the foreign key.
```

So, tables swapped with `pt-osc` are not exactly what they used to be before the swap.
It also had a number of other quirks that just didn't work out for us, related to FKs and
the amount of switches required to make it (semi-)work.

Additionally, by making DBIx::OnlineDDL its own Perl module, it's a lot easier to run
Perl-based schema changes along side [DBIx::BatchChunker](https://metacpan.org/pod/DBIx::BatchChunker) without having to switch
between Perl and CLI.  If other people want to subclass this module for their own
environment-specific quirks, they have the power to do so, too.

# AUTHOR

Grant Street Group &lt;developers@grantstreet.com>

# LICENSE AND COPYRIGHT

Copyright 2018 Grant Street Group

This program is free software; you can redistribute it and/or modify it
under the terms of the the Artistic License (2.0). You may obtain a
copy of the full license at:

[http://www.perlfoundation.org/artistic\_license\_2\_0](http://www.perlfoundation.org/artistic_license_2_0)
