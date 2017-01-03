#!/usr/bin/perl
#-------------------------------------------------------------------------------
# dbDeploy.pl - Database Deploy 
#
# Deploy database table changes from one database connection to another by 
# querying the source and target datbases, and then either updating or inserting
# records in the target database.  Store the update and insert lists to a file
# that can be used to rollback to the previous settings when required.  This
# script will update or insert records to the target database, but it will only
# delete target records with the --restore option when records were previously
# inserted.  So if the specified select_sql statement returns more rows in the
# target than the source, those additional rows will remain unchanged in the
# target database.
#
# Only those target records matching the key columns of the source database will
# change when the update columns have values that differ from the source.  The
# script will count the updates and inserts, and present that count to the
# administrator with an option to refuse the deployment.
#
# WARNING:  The --restore option of this script is only intended to restore the
# database back to the state is was before the updates and inserts of a previous
# dbDeploy run were performed.  The resulting database state may be different
# from the original state if outside updates, inserts, or deletes have occurred
# since this script was last run.  You should back up the database manually 
# before each deployment if the changes are numerous, and this script SHOULD 
# NOT be used on frequently changing dynamic tables.
#
# Input
##
#  file        : Dumper config file that defines the following elements needed
#                for processing the deployment: 
#                  source_conn  : Source database connection name
#                  target_conn  : Target database connection name
#                  db_name      : Database name of the table to update
#                  table        : Name of the table to update
#                  key_cols     : Array of columns that make a unique key
#                  update_cols  : Array of columns to update. To accomodate 
#                                 inserts, key_cols plus update_cols names must
#                                 be all the non-automatic columns in the table.
#                  select_sql   : SQL statement for selecting source and target
#                                 records to update
#                  -- For Restore --
#                  updates      : Hash of updated per unique key with the
#                                 following format:
#                                 { <key> => {
#                                     old => {<column/value pairs>}
#                                     new => {<column/value pairs>}
#                                   },... }
#                  inserts      : Hash of inserted records per unique key with
#                                 the following format:
#                                 { <key> => {<column/value pairs>},...}
#
#  --source=s  : Override file defined Source database connection name
#  --target=s  : Override Target database connection name
#  --restore   : Restore the previous target database settings rather than 
#                deploy new ones (updates and inserts hash is required with 
#                this option)
#  --logDir=s  : Directory to store the transaction log file (default is ./). 
#                The log file can be used to restore the changes.
##
#
# SITO Mobile LTD., Boise, David Runyan, April 2016
#-------------------------------------------------------------------------------
#
use strict;
use lib '/shared/SITO/perl_lib';
use SITO::CmdUtil::DoUsage;
use SITO::DB;
use Getopt::Long;
use File::Basename;
use Data::Dumper;
use Date::Manip;

our $DoDebug = 0;
$| = 1;

#-------------------------------------------------------------------------------
# Main
#  Processing Steps
# - Read and check the input file contents, and load processing variables.
# - Connect to both the source and target databases.
# If Deploy mode, do these steps:
# - Run the database select SQL on the source database to get the records to 
#   deploy.
# - Run the select SQL on the target database to get records to update.
# - Compare source and target data to find the updates and inserts.
# If Restore mode, do these steps:
# - Get updates and inserts from the file data.
#
# - Report the count of updates, inserts (or deletes) to the admin, and ask for 
#   confirmation before continuing. Exit if told no.
# - Start a database transaction.
# - Apply the updates to the target database.
# If mode is Deploy,
# - Insert the new records to the target database.
# - Store any updates or inserts to a deployment log file.
#
# If mode is Restore,
# - Delete the previous inserts from the target database.
#
# - Commit the database transaction, and report the results.
#-------------------------------------------------------------------------------
my $doUsage = 0;
my( $err, $srcConn, $tgtConn, $srcDb, $tgtDb, $dbh, $nRecs, $msg );
my( $runType, $uKey, $lDir );
my $updRef = {};
my $insRef = {};
my $data = {};
my $delKeys = [];
my $doRestore = 0;

# Get and check input arguments.
#
unless ( GetOptions(
                 'help'    => \$doUsage,
                 'restore' => \$doRestore,
                 'source=s' => \$srcConn,
                 'target=s' => \$tgtConn,
                 'logDir=s' => \$lDir,
                 ) )
{
    $doUsage = 1;
}

my $inFile = $ARGV[0];
unless ( $doUsage || -s $inFile )
{
    $err = "Input file ($inFile) is missing or is empty.";
    $doUsage = 1  
}

my $plName = basename( $0 );

my $uObj = SITO::CmdUtil::DoUsage->new( appDir => dirname( $0 ),
                               appName => $plName );
if ( $doUsage || $err )
{
    print "\n$err\n"  if ( $err );
    print $uObj->getUsage();
    exit( 1 );
}

# - Read and check the input file contents, and load processing variables.
($data, $err) = readInputFile( $inFile, $srcConn, $tgtConn );

# - Connect to both the source and target databases.
unless ( $err )
{
    $srcDb = SITO::DB->new( connection => $data->{source_conn} );
    $err = $srcDb->{error};
    unless ( $err )
    {
        $tgtDb = SITO::DB->new( connection => $data->{target_conn} );
        $err = $tgtDb->{error};
    }
}

# If Deploy mode, do these steps:
# - Run the database select SQL on the source database to get the records to
#   deploy.
# - Run the select SQL on the target database to get records to update.
# - Compare source and target data to find the updates and inserts.
unless ( $err || $doRestore )
{
    ($updRef, $insRef, $err) = getDbChanges( $data, $srcDb, $tgtDb );
}

# If Restore mode, do these steps:
# - Get updates and deletes from the file data.
if ( ! $err && $doRestore )
{
    if ( 'HASH' eq ref($data->{updates}) )
    {
        foreach $uKey ( keys(%{$data->{updates}}) )
        {
            if ( defined($data->{updates}{$uKey}{old}) )
            {
                $updRef->{$uKey} = $data->{updates}{$uKey};
            }
            else
            {
                printf( "On Restore: No old data for key = %s\n", $uKey );
            }
        }
    }
    else
    {
        print "On Restore: No updates were found in the input file.\n";
    }

    if ( 'HASH' eq ref($data->{inserts}) )
    {
        map {
            push( @{$delKeys}, $_ );
        } keys( %{$data->{inserts}} );
    }
    else
    {
        print "On Restore: No inserts were found in the input file.\n";
    }
}

# - Report the count of updates, inserts (or deletes) to the admin, and ask for 
#   confirmation before continuing. Exit if told no.
unless ( $err )
{
    $nRecs = keys( %{$updRef} );
    $msg = sprintf( "\n%d record%s found to update, ", $nRecs,
                    ($nRecs == 1) ? ' was' : 's were' );
    if ( $doRestore )
    {
        $nRecs = scalar( @{$delKeys} );
        $runType = 'delete';
    }
    else
    {
        $nRecs = keys( %{$insRef} );
        $runType = 'insert';
    }
    $msg .= sprintf( "%d record%s found to %s\nDo want to continue? ", 
                $nRecs, ($nRecs == 1) ? ' was' : 's were', $runType );

    unless ( askToContinue($msg) )
    {
        printf( "Ok. Deployment to %s was aborted.\n", $data->{target_conn} );
        exit( 1 );
    }
}

# - Start a database transaction.
unless ( $err )
{
    $dbh = $tgtDb->getDBH();
    $dbh->{AutoCommit} = 0;
}

# - Apply the updates to the target database.
if ( ! $err && (keys(%{$updRef}) > 0) )
{
    $err = doDbUpdates( $data, $tgtDb, $updRef, $doRestore );
}

# If mode is Deploy,
# - Insert the new records to the target database.
unless ( $err || $doRestore )
{
    if ( keys(%{$insRef}) > 0 )
    {
        $err = doDbInserts( $data, $tgtDb, $insRef );
    }

    # - Store any updates or inserts to a deployment log file.
    unless ( $err )
    {
        $err = storeDeployLog( $data, $inFile, $lDir, $updRef, $insRef );
    }
}

# If mode is Restore,
# - Delete the previous inserts from the target database.
if ( ! $err && $doRestore && $#{$delKeys} >= 0 )
{
    $err = doDbDeletes( $data, $tgtDb, $delKeys );
}

# - Commit the database transaction, and report the results.
if ( $err )
{
    printf( "\n%s database update Failed (transaction rolled back): %s\n", 
            $plName, $err );
}
else
{
    $dbh->commit();
    print "\nDone.\n";
}

#-------------------------------------------------------------------------------
# Subroutines
#-------------------------------------------------------------------------------
# readInputFile
#
# Read the Dumper format input file and store the data in a return hash.  If
# the source or target connection override is specified, substitute those values
# for the ones in the file.
#
# Input
#    fileNm   : File name to read
#    sConn    : Alternate source database connection name
#    tConn    : Alternate target database connection
#
# Return
#    pkt      : Data packet defined in the file
#    err      : Error message if any
#
sub readInputFile
{
    my( $fileNm, $sConn, $tConn ) = @_;
    my( $err, $fp, $VAR1, $ele );
    my @inLns;
    my @misses;
    my $pkt = {};

    if ( open($fp, "<$fileNm") )
    {
        @inLns = <$fp>;
        close( $fp );
        eval( join('', @inLns) );
        if ( 'HASH' eq ref($VAR1) )
        {
            $pkt = $VAR1;
            $pkt->{source_conn} = $sConn  if ( defined($sConn) );
            $pkt->{target_conn} = $tConn  if ( defined($tConn) );
        }

        # Validate the required components.
        foreach $ele ( qw(source_conn target_conn db_name table select_sql
                          key_cols update_cols) )
        {
            unless( defined($pkt->{$ele}) )
            {
                push( @misses, $ele );
            }
        }
        if ( $#misses >= 0 )
        {
            $err = sprintf( "Data file is missing required fields: %s",
                          join(',', @misses) );
        }
        else
        {
            unless ( 'ARRAY' eq ref($pkt->{key_cols}) && 
                     'ARRAY' eq ref($pkt->{update_cols}) )
            {
                $err = "Data file field key_cols or update_cols has the wrong ".
                    "format";
            }
        }
    }
    else
    {
        $err = "File read error: $!";
    }

    return( $pkt, $err );
}

#-------------------------------------------------------------------------------
# getDbChanges
#
# Get the database changes found between the source and target database, and
# return a hash of updates and/or an array of inserts depending on how the 
# target database compares with the source.  When the select_sql statement is
# run against both databases, the results are compared to find the inserts and
# updates.
#
# Input
#   pkt      : Data packet from the input configuration file
#   sdbOb    : Source DB object
#   tdbOb    : Target DB object
#
# Return
#   uRef     : Hash of old and new update records per record unique key
#   iRef     : Hash of inserts per record key
#   err      : Error message if any
#
sub getDbChanges
{
    my( $pkt, $sdbOb, $tdbOb ) = @_;
    my( $err, $query, $rowPs, $srcRef, $srcKeys, $tgtRef, $tgtKeys );
    my( $col, $ky );
    my $uRef = {};
    my $iRef = {};

    my $updCols = ('ARRAY' eq ref($pkt->{update_cols})) 
        ? $pkt->{update_cols} : [];

    ($srcRef, $srcKeys, $err) = getDbData( $sdbOb, $pkt );
    ($tgtRef, $tgtKeys, $err) = getDbData( $tdbOb, $pkt )  unless ( $err );

    unless ( $err )
    {
        foreach $ky ( @{$srcKeys} )
        {
            if ( defined($tgtRef->{$ky}) )
            {
                foreach $col ( @{$updCols} )
                {
                    if ( $srcRef->{$ky}{$col} ne $tgtRef->{$ky}{$col} )
                    {
                        $uRef->{$ky}{new} = $srcRef->{$ky};
                        $uRef->{$ky}{old} = $tgtRef->{$ky};
                        last;
                    }
                }
            }
            else
            {
                $iRef->{$ky} = $srcRef->{$ky};
            }
        }
    }

    return( $uRef, $iRef, $err ); 
}

# getDbData - Get database data rows per key_cols key.
sub getDbData
{
    my( $dbObj, $pkt ) = @_;
    my( $err, $query, $rowPs, $rowP, $keyCols );
    my $dbRef = {};
    my $dbKeys = [];

    $query = $pkt->{select_sql};
    $keyCols = $pkt->{key_cols};

    eval {
        $rowPs = $dbObj->fetchall_AoH( query => $query );
    };
    if ( $@ )
    {
        $err = "Database selection failed: $@";
    }
    else
    {
        foreach $rowP ( @{$rowPs} )
        {
            my $ky;
            map {
                $ky .= ':' . $rowP->{$_};
            } @{$keyCols};

            $ky =~ s/^://;

            # If the key is not unique, keep only the first one.
            next  if ( defined($dbRef->{$ky}) );

            $dbRef->{$ky} = $rowP;
            push( @{$dbKeys}, $ky );
        }
    }

    return( $dbRef, $dbKeys, $err ); 
}

#-------------------------------------------------------------------------------
# askToContinue
#
# Provide an opportunity to skip the database updates by asking to continue.
#
# Input
#   msg  : question to ask
#
# Return
#   1 = do continue
#
sub askToContinue
{
    my( $msg ) = @_;
    my $ret = 0;
    my $ans;
    open( STDIN, "-" );
    print $msg . "y|n ";

    while( <STDIN> )
    {
        chomp;
        $ans = $_;
        if ( $ans =~ /^y/i )
        {
            $ret = 1;
            last;
        }
        elsif ( $ans =~ /^n/i )
        {
            last;
        }
        else
        {
            print "\nWhat?\n";
            print $msg . "y|n ";
        }
    }
    close( STDIN );

    return( $ret );
}

#-------------------------------------------------------------------------------
# storeDeployLog
#
# Store a deployment log file with the same format as the dbDeploy input file
# so the file can be later used to restore the database back to the original 
# state.  The output file will be named same as the input file only with a
# date stamp to mark the version.
#
# Input
#   pkt       : Deploy data settings packet
#   file      : Input file name
#   oDir      : Output directory name to contain the new deploy file
#   uRef      : Hash reference of updated values
#   iRef      : Hash reference of the inserted records
#
# Return
#   err       : Error message if any
#
sub storeDeployLog
{
    my( $pkt, $file, $oDir, $uRef, $iRef ) = @_;
    my( $err, $outFile, $fp );
    my $dtStamp = UnixDate( ParseDate('today'), "%q" );
    my $fileRef = {};

    if ( -d $oDir )
    {
        $oDir =~ s/\/$//;
        $outFile = sprintf( "%s/%s_%s", $oDir, basename($file), $dtStamp );
    }
    else
    {
        $outFile = $file . "_$dtStamp";
    }

    # Format the file hash reference to contain all the elements of the input
    # file plus the update and insert lists.
    #
    unless ( $err )
    {
        map {
            $fileRef->{$_} = $pkt->{$_};
        } keys( %{$pkt} );

        $fileRef->{updates} = $uRef;
        $fileRef->{inserts} = $iRef;
    }

    # Create and write to the restore file.
    unless ( $err ) 
    {
        if ( open($fp, ">$outFile") )
        {
            printf( "Writing restore file: %s\n", $outFile );
            printf( $fp "# dbDeploy Restore File (original = %s)\n",
                    $file );

            $Data::Dumper::Indent = 1;
            printf( $fp "%s\n", Dumper($fileRef) );
            close( $fp );
        }
        else
        {
            $err = sprintf( "Cannot open journal file %s for write.", 
                            $outFile );
        }
    }

    return( $err );
}

#-------------------------------------------------------------------------------
# doDbUpdates
#
# Do the database updates with either the new or old values given and found. In
# Restore mode, the old values are used to update back to the previous state. 
# Otherwise, we use the new values in the updates.  
#
# I wanted to use SITO::DB::doUpdates here, but I couldn't figure out how to
# get the flexible update columns and flexible key colums updates to work with
# that SITO package.  I opted instead to build my own prepare/execute sequence.
#
# Input
#  pkt    : Data packet containing the database and table names to update
#  dbObj  : DB object of the target database to change
#  uRef   : Update hash reference with both the old and new values to update
#  useOld : Flag indicating this run is a Restore so update with the old values
#
# Return
#  err    : Error message if any
#
sub doDbUpdates
{
    my( $pkt, $dbObj, $uRef, $useOld ) = @_;
    my( $err, $ret, $uType, $updSql, $sth, $ky, $colNm, $nUpds );
    my @updCols;
    my @keyCols;
    my $dbh = $dbObj->getDBH();

    $uType = ($useOld) ? 'old' : 'new';

    # Build the update SQL.
    $updSql = sprintf( "UPDATE %s.%s ", $pkt->{db_name}, $pkt->{table} );
    
    if ( 'ARRAY' eq ref($pkt->{update_cols}) &&
         'ARRAY' eq ref($pkt->{key_cols}) )
    {
        @updCols = @{$pkt->{update_cols}};
        @keyCols = @{$pkt->{key_cols}};
        $updSql .= sprintf( "SET %s = ? ", join(' = ?, ', @updCols) );
        $updSql .= sprintf( "WHERE %s = ?", join(' = ? AND ', @keyCols) );

        eval {
            $sth = $dbh->prepare( $updSql );
        };
        if ( $@ )
        {
            $err = "Database setup Failed: $@";
        }
    }
    else
    {
        $err = "Missing update_cols or key_cols definition in data packet.";
    }

    # Execute an update using either the old or new values for each key in 
    # the update hash.
    #
    unless ( $err )
    {
        foreach $ky ( keys(%{$uRef}) )
        {
            my @vals;
            foreach $colNm ( @updCols )
            {
                if ( defined($uRef->{$ky}{$uType}{$colNm}) )
                {
                    push( @vals, $uRef->{$ky}{$uType}{$colNm} );
                }
            }

            # Add the Where clause key values.
            push( @vals, (split(/:/, $ky)) );

            # Execute the update.
            eval {
                $ret = $sth->execute( @vals );
            };
            if ( $@ )
            {
                $err = "Database update Failed: $@";
                last;
            }
            else
            {
                unless ( $sth->rows() > 0 )
                {
                    $err = "No database update occurred. Aborting...";
                    last;
                }
            }
        }
        unless ( $err )
        {
            $nUpds = keys( %{$uRef} );
            printf("Updated %d record%s.\n", $nUpds, ($nUpds == 1) ? '' : 's');
        }
    }

    return( $err );
}

#-------------------------------------------------------------------------------
# doDbInserts
#
# Do the database inserts for new records found during the deployment
# processing.  The inserts are detected and handled separately from the updates
# so we can keep track of them to delete later if we need to rollback.
#
# Input
#  pkt    : Data packet provided with the input file
#  dbObj  : Target DB object
#  iRef   : Hash reference of insert records per record key
#
# Return
#  err    : Error message if any
#
sub doDbInserts
{
    my( $pkt, $dbObj, $iRef ) = @_;
    my( $err, $sth, $ky, $cols, $dbName, $table, $nIns );
    my $allVals = [];

    $dbName = $pkt->{db_name};
    $table = $pkt->{table};
    $cols = [@{$pkt->{key_cols}}, @{$pkt->{update_cols}}];

    foreach $ky ( keys(%{$iRef}) )
    {
        my $vals = [];
        map { 
            push( @{$vals}, $iRef->{$ky}{$_} );
        }  @{$cols};

        push( @{$allVals}, $vals ); 
    }

    eval {
        $sth = $dbObj->doInsert({ dbname => $dbName, table => $table,
                                columns => $cols, vals => $allVals });
    };
    if ( $@ )
    {
        $err = "Database insert Failed: $@";
    }
    else
    {
        $nIns = scalar( @{$allVals} );
        printf( "Inserted %d record%s.\n", $nIns, ($nIns == 1) ? '' : 's' )
            if ( $sth->rows() > 0 );
    }

    return( $err );
}

#-------------------------------------------------------------------------------
# doDbDeletes
#
# Delete all the records that were previously inserted to roll the database
# table back to the state is was before the last deployment.  This routine will
# check to make sure the first record key in the delete hash is a unique record
# before performing the deletes.  The assumption here is if the first key is 
# unique, all the others will be unique as well.  Care should be taken when
# defining the key_columns of the input file to they define a unique record or
# unwanted deletes may occur with this routine.
#
# Input
#  pkt    : Data packet of the input file specified with --restore option
#  dbObj  : Target DB object
#  dKeys  : Array reference of keys of records to delete
#
# Return
#  err    : Error message if any
#
sub doDbDeletes
{
    my( $pkt, $dbObj, $dKeys ) = @_;
    my( $err, $sth, $ky, $dbName, $table, $nDels );
    my @keyVals;
    my $allVals = [];
    my $keyCols = $pkt->{key_cols};
    my $where = sprintf( "%s = ?", join(' = ? AND ', @{$keyCols}) );
    $dbName = $pkt->{db_name};
    $table = $pkt->{table};

    foreach $ky ( @{$dKeys} )
    {
        my $vals = [];
        map {
            push( @{$vals}, $pkt->{inserts}{$ky}{$_} );
        } @{$keyCols}; 
        push( @{$allVals}, $vals ); 
    }

    eval {
        $sth = $dbObj->doDelete({ dbname => $dbName, table => $table,
                                where => $where, vals => $allVals });
    };
    if ( $@ )
    {
        $err = "Database delete Failed: $@";
    }
    else
    {
        $nDels = scalar( @{$allVals} );
        printf( "Deleted %d record%s.\n", $nDels, ($nDels == 1) ? '' : 's' )
            if ( $sth->rows() > 0 );
    }

    return( $err );
}
