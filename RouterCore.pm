#-------------------------------------------------------------------------------
# SITO::Queue::RouterCore - Single Touch Queue Router Core Perl Package
#
# This module provides the core structure and methods for RabbitMQ queue router
# processing used mainly in message delivery processing.  It supports router
# classes that enable specialized processing for Single Touch client messaging
# to allow for current and future messaging requirements.  Each router class is
# named after the function to perform or message type to process, and has its
# own Perl module that is loaded by RouterCore to define how to publish to and
# consume from the associated queue.  Future expansion to new messaging
# opportunities can be accomplished by designing and building new class
# libraries that support new queues for processing.
#
# Single Touch Systems Inc., Boise, David Runyan, April 2014
#-------------------------------------------------------------------------------
package SITO::Queue::RouterCore;

use strict;
use SITO::AMQP::Amqp;
use SITO::Config;
use SITO::UniqueID;
use SITO::MQDelay;
use SITO::MQDb;
use SITO::Macro;
use SITO::DB;
use Data::Dumper;
use JSON;

# Constants
use constant RC_ABORT_STATUS => 'ABORTED';
use constant RC_DEFAULT_ABORT_RT => ['RequestResults'];
use constant RC_DEFAULT_PMLIB => 'SITO::Queue::Lib';
use constant RC_QUEUE_MAX => 100;
use constant RC_QUEUE_ID_NAME => 'queue_router'; # see sito_global.id_store
use constant RC_ARGS_ROUTEKEY => 'routing_key';
use constant RC_ARGS_EXCHANGE => 'exchange_name';
use constant RC_ARGS_CARGO => 'cargo';
use constant RC_SETTINGS_KEY => 'settings';
use constant RC_MSG_CONTEXT => 'queue_router';
use constant RC_STATUS_KEY => 'request_status';
use constant RC_STATUS_DET_KEY => 'request_status_detail';
use constant RC_RETRY_SECS => 300; # default if no Config delay_seconds
use constant RC_RQST_DB => 'sito_messaging';
use constant RC_RQST_TABLE => 'request';
use constant RC_RQST_BATCH_TABLE => 'request_batch';
use constant RC_RQST_TAGS => 'request_tags';
use constant RC_RQST_NEW_STATE => 'PROCESSING';
use constant RC_RQST_RETRY_STATE => 'RETRY';
use constant RC_BATCH_ERR_STATE => 'FAILED_BATCH';

# Tag names to use when saving process state to request_tags
use constant RC_SAVE_TAGS => {
        settings => '_sito_settings',
        history => '_sito_history',
        RC_STATUS_DET_KEY() => '_sito_status_detail',
        cargo => '_sito_cargo',
    };

# Globals

=pod

=head1 NAME - SITO::Queue::RouterCore

B<RouterCore> - SITO Queue Router Core

=head1 ABSTRACT

SITO::Queue::RouterCore defines methods to support the queue processing of 
client or function specific data flow to direct message delivery.

=head1 SYNOPSIS

    use SITO::Queue::RouterCore
    my $rcOb = SITO::Queue::RouterCore->new( class_data => $classRef );
    die "$rcOb->{error}\n"  if ( $rcOb->{error} );

    # Publish the specified class.
    $ret = $rcOb->publishStart( cargo => $data );
    $err = $ret->Get_error();
    die "publishStart failed: $err\n"  if ( $err );

    # Consume records of queues belonging to all current classes.
    $ret = $rcOb->consume();

    # Consume records of just the specified class.
    $ret = $rcOb->consume( class_name => $classNm );
    $err = $ret->Get_error();
    die "Consume failed: $err\n"  if ( $err );

=head1 DESCRIPTION

The B<RouterCore> module provides the core structure and methods for RabbitMQ
queue router processing used mainly in message delivery processing.  It supports
router classes that enable specialized processing for Single Touch client
messaging to allow for current and future messaging requirements.  Each router
class is named after the function to perform or message type to process, and has
its own Perl module that is loaded by RouterCore to define how to publish to and
consume from the associated queue.  Future expansion to new messaging
opportunities can be accomplished by designing and building new class libraries
that support new queues for processing.

=head1 PUBLIC METHODS

B<_____________________________________________________________________________>

=head2 new($ARGS) - Create a new instance of RouterCore

Create and initialize a new B<RouterCore> object to hold a data cargo and
settings necesary to process any supported class of message to deliver.  The
settings are initialized once message class and cargo have been specified with
the B<publishStart> method.  Both are combined to form a message packet called
B<msgData> that directs processing and follows the message through to delivery.
The settings are used by the consumers of the Start Queue and subsequent queues
to direct processing to the next queue down the process route. 

Any new class can be added to B<RouterCore> by creating a Perl module in the
Queue/lib directory named exactly the same as that class minus '.pm'.  For
example, the Walmart SOAP class is class is defined by the Queue/lib/Wmt_soap.pm
Perl module.  All class modules must export the following callback routine:

    consumeCallback   : Callback routine to pass to the Consumer_poll method.

Once the class library file has been added, attributes for that class need to be
added to the sito_global.config and config_tags database tables to allow its use
with B<RouterCore>.  The database entries for a class must contain all the
elements necessary to defined the processing of that class.  See the B<start>
class for an example of what attribute tags to define.

=head3 Optional Arguments

=over 4

=item

$ARGS->{B<dbObj>}  : Database connection to use when database access is
necessary.  If not supplied, a connection will be opened using the default
connection parameters derived from the current server environment.

=item

$ARGS->{B<logFp>}  : Pointer to an open, writable log file. If not supplied,
STDERR is used anytime a print statement is issued in the module.

=item

$ARGS->{B<amqp_connection>}  : Optional connection string for AMQP (RabbitMQ) 
connection. Look in /shared/SITO/conf AMQP.conf on the mo_processor host for 
available connection strings.

=item

$ARGS->{B<debug>}  : Set this flag to 1 if you want debug statements to print
to the out to the B<logFp> (default is STDERR).

=item

$ARGS->{B<max_consume>}  : Maximum number of records to consume before halting
queue consumption of any given class.  This should only be used when running in
Debug mode because the Consumer_poll process will be taken abrubtly off line
when the consume count has been reached, and one must exit the calling program
to clean up orphaned channels left behind.

=back

=head3 Return Value

B<SITO::Queue::RouterCore> object pointer

=cut

sub new
{
    my $class = shift;
    my $ARGS = ('HASH' eq ref $_[0]) ? shift : { @_ };

    my $self = {};
    bless( $self, $class );
    my( $ret, $err, $conf, $stgNm, $ecRef, $clsNm, $eName, $qName );
    my( $vals, $clsType, $exchList );
    my @eSettings = qw( exchange_class work_class cargo_key settings_key 
                        mo_collector_ip ssh_resolver);
    $self->{msgData} = {};
    $self->{config} = {};

    # Load optional arguments
    $self->{dbObj} = (exists($ARGS->{dbObj})) ? $ARGS->{dbObj} : undef();
    $self->{logFp} = (exists($ARGS->{logFp})) ? $ARGS->{logFp} : *STDERR;
    $self->{max_consume} = (exists($ARGS->{max_consume})) 
        ? $ARGS->{max_consume} : undef();
    $self->{consume_count} = 0;
    $self->{debug} = (exists($ARGS->{debug})) ? $ARGS->{debug} : 0;

    # Connect to the database if not already connected.
    $self->{dbObj} = SITO::DB->new( {RAISE_ERROR => 1} )  unless ( $self->{dbObj} );
    # Retrieve the configuration parameters.
    my $cObj = SITO::Config->new( dbObj => $self->{dbObj} );
    $err = $cObj->{error};
    unless ( $err )
    {
        $ret = $cObj->getConf( config_key => 'QueueRouter' );
        $err = $ret->Get_error();

        # Check for essential configuration settings.
        $vals = $ret->Get_vals();
        if ( defined($vals->{data}) )
        {
            $conf = $vals->{data};
            foreach $stgNm ( @eSettings )
            { 
                unless ( defined($conf->{$stgNm}) ) 
                {
                    $err = "Missing essential \"$stgNm\" setting in config ".
                           "database.";
                    last;
                }
            }

            $self->{config} = $conf  unless ( $err );
        }
        else
        {
            $err = "QueueRouter in config database is missing or incomplete.";
        }
    }
    # Initialize a connection to the MQ exchange with publish_ack turned off
    # to improve performance.
    #
    unless ( $err )
    {
        my $qpArgs = {publish_ack => 0};
        $qpArgs->{connection} = $ARGS->{amqp_connection}
            if ( defined($ARGS->{amqp_connection}) );

        $ret = SITO::AMQP::Amqp->new($qpArgs);
        $err = $ret->Get_error();
        if ( $err )
        {
            $err = 'AMQP::Amqp::new Error: ' . $err;
        }
        else
        {
            $self->{amqp} = $ret->Get_vals();
        }
    }

    # Initialize all exchanges and queues in the configuration.
    unless ( $err )
    {
        foreach $clsType ( qw(exchange_class notify_class) )
        {
            $ecRef = $self->{config}{$clsType};
            foreach $clsNm ( keys(%{$ecRef}) )
            {
                $eName = (defined($ecRef->{$clsNm}{exchange})) 
                    ? $ecRef->{$clsNm}{exchange} : undef();
                $qName = (defined($ecRef->{$clsNm}{queue})) 
                    ? $ecRef->{$clsNm}{queue} : undef();

                if ( $eName )
                {
                    push( @{$exchList->{$eName}}, {name => $qName} );
                }
                else
                {
                    $err = "Config settings Error: Exchange Class - $clsNm ".
                        "is missing definition for its exchange.";
                    last;
                }
            }
        }
        $err = $self->_initExchanges( $exchList )  unless ( $err );
    }

    $self->{error} = $err  if ( $err );

    return( $self );
}

=pod

B<_____________________________________________________________________________>

=head2 publishStart($ARGS) - Publish Start of Queue Router flow

Publish the named class and cargo data to the Router Start queue to begin
processing the message packet.  This starts the Queue Router processing flow by
loading class specific configuration settings that the router uses to direct the
message processing.  The router uses the settings to keep track of the
processing history and the process route to know where to publish with the next
call to B<publishNext>.  This method must be called to begin queue processing.

Only work class names can be specified with this method.  The configuration
settings for this method are stored in the RouterCore object when B<new> is
called, and an error will result if no settings are found for the named work
class. 

=head3 Mandatory Arguments

=over 4

=item

$ARGS->{B<class_name>}  : The class name of the data to publish.  This name must
exist in the class data and in the class configuration tags found in the 
SITO::Config database to avoid error.  This name tells the router what process
route to load along with any other settings that it needs to determine where
to publish next.

=item

$ARGS->{B<cargo>}  : The message data to process with this run.  This must be a
single string of data that may be encoded in any format that the consumers of
the queues in the process route will understand.  When the record gets published
to the Start queue, the settings will be combined with the cargo to form the
object's B<msgData> which is JSON encoded before being added to the queue.  This
creates the message packet of data and settings that will guide the message
processing to delivery.  Cargo can be any formated string that the downstream
consumers require including itself a JSON string.

=back

=head3 Optional Arguments

=over 4

=item

$ARGS->{B<settings>}  : Hash of key value pairs to use as initial settings for
the start of publishing.  Use this feature to add settings to the start that
other ways of starting a queue processing flow may publish to the first queue.
For example, Node authentication may load the first queue directly rather than
calling the B<publishStart> method.  This option is mostly intended to aid 
testing.

=back

=head3 Return Value

B<SITO::Return> object with the following methods defined:

    Get_error() : Fatal error message if any

=cut

sub publishStart
{
    my $self = shift;
    my $ARGS = ('HASH' eq ref $_[0]) ? shift : { @_ };
    my( $err, $ret, $clsNm, $cargo, $clsConf, $inStgs );
    my $methNm = 'publishStart';
    my $clsType = 'work_class';
    my $config = $self->{config};
    my $cKey = (defined($config->{cargo_key})) 
        ? $config->{cargo_key} : RC_ARGS_CARGO();
    my $sKey = (defined($config->{settings_key})) 
        ? $config->{settings_key} : RC_SETTINGS_KEY();

    # The route_args hash will be filled with publish arguments name-value 
    # pairs per class name as processing proceeds from queue to queue. This
    # will allow run time customization of publishing to class queues.
    #
    my $mData = {$sKey => {history => [], route_args => {}, retry_ready => 0}};
    my $outRet = SITO::Return->new();

    # Verify input arguments - required: class_name, cargo.
    $clsNm = (defined($ARGS->{class_name})) ? $ARGS->{class_name} : undef();
    $cargo = (defined($ARGS->{cargo})) ? $ARGS->{cargo } : undef();
    if ( $clsNm && $cargo )
    {
        # Check the class type - work_class is requried for this method.
        if ( defined($config->{$clsType}) &&
             defined($config->{$clsType}{$clsNm}) )
        {
            $clsConf = $config->{$clsType}{$clsNm};
            unless ( defined($clsConf->{process_route}) )
            {
                $err = "The $clsNm class has no process_route defined in ".
                       "the config database.";
            }
        }
        else
        {
            $err = "Settings of type $clsType are missing for $clsNm in ". 
                   "the config database.";
        }
    }
    else
    {
        $err = "Missing required class_name and/or cargo input.";
    }

    # Add optional input settings to the message data.
    #
    $inStgs = ('HASH' eq ref($ARGS->{settings})) ? $ARGS->{settings} : {};
    map {
        $mData->{$sKey}{$_} = $inStgs->{$_};
    } keys( %{$inStgs} );

    # Initialize the msgData attribute with settings (process_route, and a
    # blank history), and cargo, and call publishNext to add the message packet
    # to the Start queue.
    #
    unless ( $err )
    {
        map { $mData->{$sKey}{$_} = $clsConf->{$_} }  keys( %{$clsConf} );
        $mData->{$cKey} = $cargo;
        $self->{msgData} = $mData;

        $ret = $self->publishNext();
        $err = $ret->Get_error();
    }

    if ( $err )
    {
        if ( 'SITO::Return' eq ref($ret) )
        {
            $outRet = $ret;
        }
        else
        {
            $outRet->Set_error( "$methNm Error: $err" )  if ( $err );
        }
    }

    return( $outRet );
}

=pod

B<_____________________________________________________________________________>

=head2 setMessageData($ARGS) - Set Message Data 

Set the message data packet owned by the current object after validating the 
minimum required fields - cargo and settings are included in the packet.  This 
new data packet will then be used by subsequent method calls of this object
including the B<publishNext> method to convert to JSON format and store the
packet to the appropriate exchange queue.  Using this method to change
message data rather than directly assigning the packet to the object will 
allow centralized preparation of the stored packet should it become necessary.

=head3 Mandatory Arguments

=over 4

=item

B<packet>  : Packet of message data to assign the current object.  Use this 
method to alter the message packet from a consumer callback routine or any
other code that needs to modify the data during Queue Router processing.

=back

=head3 Return Value

B<SITO::Return> object with the following methods defined:

    Get_error() : Fatal error message if any

=cut

sub setMessageData
{
    my $self = shift;
    my $ARGS = ('HASH' eq ref $_[0]) ? shift : { @_ };
    my( $err, $ret, $pkt );
    my $methNm = 'setMessageData';
    my $outRet = SITO::Return->new();

    $pkt = (exists($ARGS->{packet})) ? $ARGS->{packet} : undef();
    if ( $pkt )
    {
        $self->{msgData} = {%{$pkt}};  # copy so object can stand alone
    }
    else
    {
        $err = "Required \"packet\" argument is missing.";
    }

    if ( $err )
    {
        if ( 'SITO::Return' eq ref($ret) )
        {
            $outRet = $ret;
        }
        else
        {
            $outRet->Set_error( "$methNm Error: $err" )  if ( $err );
        }
    }

    return( $outRet );
}

=pod

B<_____________________________________________________________________________>

=head2 publishNext($ARGS) - Publish Next Class in Queue Router flow

Publish to the next exchange class in the Queue Router process flow by comparing
the config settings B<process_route> of the current RouterCore object with the
history of classes published so far.  The route history is initiated by the
B<publishStart> method, and if there is no history, this method will return an
out-of-sequence error.  If the history is empty, the first class on the process
route will be use as the publish target.  A consumer may alter the predefined
process route list by calling B<setNextClass> to set the next class in the route
before calling this method.

=head3 Optional Arguments

=over 4

=item

$ARGS->{B<delay>}  : Number of seconds to delay publishing to the next Class 
queue.  Specifying this option will cause a SITO::MQDelay publish method call
that will delay publishing to the next queue in the process route list until 
the given number of seconds have expired.  Currently, only digits are allowed
for the B<delay> value, but this method can be modified to accept dates if a
need arises that warrants additional development.

=back

=head3 Return Value

B<SITO::Return> object with the following methods defined:

    Get_error() : Fatal error message if any

=cut

sub publishNext
{
    my $self = shift;
    my $ARGS = ('HASH' eq ref $_[0]) ? shift : { @_ };
    my( $err, $ret, $pArgs, $mData, $pRoute, $pubCls, $pubEx, $pubQu );
    my( $vals, $cKey, $settings );
    my $logFp = $self->{logFp};
    my $methNm = 'publishNext';
    my $config = $self->{config};
    my $sKey = (defined($config->{settings_key})) 
        ? $config->{settings_key} : RC_SETTINGS_KEY();
    my $outRet = SITO::Return->new();

    # Check input and object attributes for required definitions.
    my $delay = (defined($ARGS->{delay})) ? $ARGS->{delay} : undef();

    if ( defined($self->{msgData}{$sKey}) )
    {
        $mData = $self->{msgData};
        $settings = $mData->{$sKey};
        $pRoute = $mData->{$sKey}{process_route};
        if ( $#{$pRoute} >= 0 )
        {
            # Get the next exchange and queue to publish.
            $ret = $self->getNextClass();
            $err = $ret->Get_error();
            unless ( $err )
            {
                $vals = $ret->Get_vals();
                $pArgs = $self->_macroToString( $vals->{publish_args} );
                $vals->{publish_args} = $pArgs;
                $pubCls = $vals->{class};
                $cKey = $vals->{cargo_key};
            }
        }
        else
        {
            $err = "Found unexpected empty process_route in the object.";
        }
    }
    else
    {
        $err = "Out of sequence method call.  Must call publishStart to ".
            "properly initilize a RouterCore object.";
    }

    # Either publish to the Delay queue or the next queue in the process route.
    unless ( $err )
    {
        if ( $pubCls )
        {
            if ( $delay )
            {
                push( @{$settings->{history}}, $pubCls );
                $ret = $self->_publishDelay( $vals, $delay );
                $err = $ret->Get_error();
            }
            else
            {
                $ret = $self->_publishExchange( $pubCls, $pArgs );
                $err = $ret->Get_error();
            }
        }
        else
        {
            print $logFp "Normal end of processing.";
        }
    }

    if ( $err )
    {
        if ( 'SITO::Return' eq ref($ret) )
        {
            $outRet = $ret;
        }
        else
        {
            $outRet->Set_error( "$methNm Error: $err" )  if ( $err );
        }
    }

    return( $outRet );
}


=pod

B<_____________________________________________________________________________>

=head2 publishNotify($ARGS) - Publish to Queue Router Class queue

Publish the current message packet to the specified Notify Class with no regards
to the current defined process route to initiate notifications to exchanges
outside of the Queue Router controlled exchanges.  The route key supplied with
this method determines which queue of the Notify Class defined exchange receives
the message.  Route keys can be generated at run time as needed to support 
different notifications through the same exchange as determined by the
consumer needing to send the notification.

The initialization of the exchange, queue, and route key does NOT happen in this
method so that process can be handled outside of the Queue Router system.  The
main objective of this feature is to provide more flexibility than what is
available with the Exchange Class methods.  This method also does not verify the
existance or availability of the specified exchange.  An error will be passed
back from the AMPQ processing if the exchange does not exist or if the route key
is not bound to the exchange.

=head3 Mandatory Arguments

=over 4

=item

B<class_name>  : Name of the Notify Class to publish.  The specified name must
be defined as a B<notify_class> type in the Config database or this method
will return an error.

=item

$ARGS->{B<route_key>}  : Name of the Route Key to use with the publish rather
than the default queue name.  

=back

=head3 Return Value

B<SITO::Return> object with the following methods defined:

    Get_error() : Fatal error message if any

=cut

sub publishNotify
{
    my $self = shift;
    my $ARGS = ('HASH' eq ref $_[0]) ? shift : { @_ };
    my( $err, $ret, $clsRef, $mData, $settings, $exchNm );
    my $logFp = $self->{logFp};
    my $methNm = 'publishNotify';
    my $config = $self->{config};
    my $sKey = (defined($config->{settings_key})) 
        ? $config->{settings_key} : RC_SETTINGS_KEY();
    my $cKey = (defined($config->{cargo_key})) 
        ? $config->{cargo_key} : RC_ARGS_CARGO();
    my $outRet = SITO::Return->new();

    # Check input and object attributes for required definitions.
    my $classNm = (defined($ARGS->{class_name})) 
        ? $ARGS->{class_name} : undef();
    my $routeKey = (defined($ARGS->{route_key})) 
        ? $ARGS->{route_key} : undef();

    if ( $classNm && $routeKey )
    {
        if ( defined($config->{notify_class}{$classNm}) )
        {
            $clsRef = $config->{notify_class}{$classNm};
            if ( defined($clsRef->{exchange}) )
            {
                $exchNm = $clsRef->{exchange};
            }
            else
            {
                $err = "No exchange has been defined for the $classNm class.";
            }
        }
        else
        {
            $err = "Class $classNm is not a Notify class.";
        }
    }
    else
    {
        $err = "Missing required class_name and/or route_key input.";
    }

    # Get the current message packet and settings.  If not defined, this method
    # was called out of sequence.
    #
    unless ( $err )
    {
        $mData = (defined($self->{msgData})) ? $self->{msgData} : undef();
        if ( $mData )
        {
            $settings = (defined($mData->{$sKey})) ? $mData->{$sKey} : undef();
            unless ( $mData && $settings )
            {
                $err = "Expected message data and/or settings are missing.";
            }
        }
    }

    # Publish to the exchange and queue.
    unless ( $err )
    {
        # Add classNm to process route so publishNext will pick up the
        # next class on the list.
        #
        $err = $self->_addClassToRoute( $classNm );
        unless ( $err )
        {
            my $eKey = RC_ARGS_EXCHANGE();
            my $rKey = RC_ARGS_ROUTEKEY();
            $ret = $self->_publishExchange( $classNm, {$eKey => $exchNm,
                                $rKey => $routeKey} );
            $err = $ret->Get_error();
        }
    }

    if ( $err )
    {
        if ( 'SITO::Return' eq ref($ret) )
        {
            $outRet = $ret;
        }
        else
        {
            $outRet->Set_error( "$methNm Error: $err" )  if ( $err );
        }
    }

    return( $outRet );
}

=pod

B<_____________________________________________________________________________>

=head2 publishAbort($ARGS) - Publish to the Abort Route

Abort the Queue Router processing and schedule a retry if the given class is
configured with a positive B<retry_max> value and the retry count has not
exceeded the maximum.  If no retry is configured or if the count is beyond the
retry maximum, modify the process route to proceed down the defined abort route.
Use a default abort route if none is defined for the class.

The Abort Route consists of zero or more exchange classes that finish the
processing after an error has occurred with the normal process flow.  These
classes can be used to gracefully end processing, report status back to the
caller, and  update the current message Request database record with the error
status.

Abort Route exchange class modules use the same mechanism to continue processing
as the normal Process Route classes - they call B<publishNext> to continue to
the next queue.  The abort route ends when there are no more abort classes to
publish.  If no Abort Route is defined in the current work class Config
settings, B<publishAbort> will abruptly end the Queue Router processing with no
request record status update.  One must define an B<abort_route> exchange class
array in the work class configuration to avoid this ending.

=head3 Mandatory Arguments

=over 4

=item

$ARGS->{B<class_name>}  : Name of the exchange class to abort.  This name is
used with its Config definitions and the message data settings to determine when
to schedule a retry.  The class Config definitions also contain the
B<abort_route> setting that is used to replace the processing flow when a retry
is not possible.

=back

=head3 Optional Arguments

=over 4

=item

$ARGS->{B<sito_return>}  : The SITO::Return object resulting from the error.  
This object will be added to the Message Settings, B<sito_return> field with
the following three fields:  B<error>, B<error_descr> and B<vals>, with the
B<vals> field being JSON encoded if it is anything other than a string.

=item

B<- OR -> (instead of B<sito_return>)

=item

$ARGS->{B<message_name>}  : Name of the message prompt to use as text to send
back to the customer (see sito_global.message).  The settings must contain a
B<system_id> and B<message_context> setting or the defaults of system_id=0 and
message_context=queue_router will be used.  If there is no message in the
sito_global that matches the name, context, and system ID, no message will be
sent to the customer, and the Abort Route classes may not report an error 
unless they allow for blank message text.

=item

B<- OR -> (instead of B<sito_return> or B<mssage_name>)

=item

$ARGS->{B<message_text>}  : Text of the message to send back to the customer to 
explain the processing error.  Macros will be expanded using the current
settings values as replacements if defined in specified text.

=item

$ARGS->{B<request_id>}  : Use this for the B<id> column of the request table
rather than the one stored in the message settings B<record_id> value.  This
allows a special abort from the StartRouter consumer even if the message packet
has yet to be created as long as the unique record was generated.

=item

$ARGS->{B<request_status>}  : Request status string to use when updating the
status of the current message request record.  If not specified, no status 
update will be made by B<publishAbort>.

=item

$ARGS->{B<request_cols>}  : Request record column values to use instead of any
like value named in the input arguments.  Use this argument to define column
values such as B<system_id>, B<partner_id>, B<request_mode> or any other
optional sito_messaging.request columns.

=back

=head3 Return Value

B<SITO::Return> object with the following methods defined:

    Get_error() : Fatal error message if any

=cut

sub publishAbort
{
    my $self = shift;
    my $ARGS = ('HASH' eq ref $_[0]) ? shift : { @_ };
    my( $err, $ret, $pArgs, $pRoute, $settings, $msg, $vals, $recId, $clsRef );
    my( $retryMax, $retryCnt, $doRetry, $inDescr );
    my $methNm = 'publishAbort';
    my $config = $self->{config};
    my $sKey = (defined($config->{settings_key})) 
        ? $config->{settings_key} : RC_SETTINGS_KEY();
    my $outRet = SITO::Return->new();
    my $mData = $self->{msgData};
    my $respKey = RC_STATUS_DET_KEY();
    my $statKey = RC_STATUS_KEY();
    my $dfltAbRoute = RC_DEFAULT_ABORT_RT();

    # Check input arguments, and message data for required settings.  Require
    # record_id if request_status is specified.
    #
    my $classNm = (defined($ARGS->{class_name})) 
        ? $ARGS->{class_name} : undef();
    my $inRet = ('SITO::Return' eq ref($ARGS->{sito_return})) 
        ? $ARGS->{sito_return} : undef();
    my $msgText = (! $inRet && defined($ARGS->{message_text})) 
        ? $ARGS->{message_text} : undef();
    my $msgName = (! $inRet && defined($ARGS->{message_name})) 
        ? $ARGS->{message_name} : undef();
    my $status = (defined($ARGS->{request_status})) 
        ? $ARGS->{request_status} : undef();
    my $rqstCols = ('HASH' eq ref($ARGS->{request_cols})) 
        ? $ARGS->{request_cols} : {};

    if ( defined($classNm) )
    {
        $clsRef = $config->{exchange_class}{$classNm};
        unless ( defined($clsRef) )
        {
            $err = "The specified class $classNm is not an exchange class.";
        }
    }
    else
    {
        $err = "Missing required class_name input.";
    }

    # Determine whether to retry or not.
    unless ( $err )
    {
        if ( defined($mData->{$sKey}) )
        {
            $settings = $mData->{$sKey};
            $settings->{record_id} = $ARGS->{request_id}
                if ( defined($ARGS->{request_id}) );

            # Run Retry logic only if retry_max is defined in the Config 
            # settings, retry_count is less than retry_max, and retry_ready
            # is true.
            #
            if ( defined($clsRef->{retry_max}) ) 
            {
                $retryMax = $clsRef->{retry_max};
                $retryCnt = (defined($settings->{$classNm}{retry_count})) 
                    ? $settings->{$classNm}{retry_count} : 0;
            }
            else
            {
                # Reset the retry max and count to zero if no retry_max is 
                # defined in this exchange class.
                #
                $retryMax = 0;
                $retryCnt = 0;
            }

            # Skip the retry logic if processing hasn't even made it pass what
            # the consumer considers input or config settings checks because
            # those errors will never auto-resolve no matter how many retries
            # are run.
            #
            $doRetry = (($retryMax > 0 && $retryCnt <= $retryMax && 
                         $settings->{retry_ready})) ? 1 : 0;

            if ( $status && ! $settings->{record_id} )
            {
                $err = "No record_id value exists in the message settings yet ".
                    "so Request status cannot be set. (status=$status)";
            }
        }
    }

    # Add sito_return values to message settings if specified.
    #
    if ( ! $err && $inRet )
    {
        $settings->{sito_return}{error} = $inRet->Get_error();
        $inDescr = $inRet->Get_descr();
        $settings->{sito_return}{descr} = (ref($inDescr)) 
            ? to_json( $inDescr ) : $inDescr;
    }

    # Look up and process message text on message_name or _text input.
    #
    unless ( $err )
    {
        if ( $msgName )
        {
            my $mCntx = (defined($settings->{message_context}))
                ? $settings->{message_context} : RC_MSG_CONTEXT();
            $ret = SITO::Message->new();
            $err = $ret->Get_error();
            unless ( $err )
            {
                my $mObj = $ret->Get_vals();
                $ret = $mObj->Get_prompt( name => $msgName, context => $mCntx );
                $err = $ret->Get_error();
                unless ( $err )
                {
                    $vals = $ret->Get_vals();
                    $msgText = $vals->{text};
                }
            }
        }

        # Expand the message text macros if necessary.
        if ( $msgText )
        {
            if ( $msgText =~ /\%\%/ )
            {
                $ret = SITO::Macro->new();
                $err = $ret->Get_error();
                unless ( $err )
                {
                    my $macro = $ret->Get_vals();
                    $ret = $macro->doReplacements( source => $msgText,
                                macros => $settings );
                    $err = $ret->Get_error();
                    unless ( $err )
                    {
                        $msg = $ret->Get_vals();
                    }
                }
            }
            else
            {
                $msg = $msgText;
            }
        }

        $settings->{$respKey} = $msg  if ( $msg );
    }

    $settings->{$statKey} = ($status) ? $status 
        : ((exists($settings->{abort_status})) 
            ? $settings->{abort_status} : RC_ABORT_STATUS());

    # Get the next class, and replace that to end of process_route with
    # abort_route classes.
    #
    
    unless ( $err )
    {
        # Add Abort route to both history and process_route so the next 
        # class to publish is the first class in abort_route.  If no
        # abort_route, processing continues to the default results route.
        #
        my @aRoute = (defined($settings->{abort_route})) 
            ? @{$settings->{abort_route}} : @{$dfltAbRoute};

        $ret = $self->getNextClass();
        my $gnErr = $ret->Get_error();
        if ( $gnErr )
        {
            $settings->{process_route} = \@aRoute;
        }
        else
        {
            $vals = $ret->Get_vals();
            my $pos = $vals->{route_pos};
            my @pRoute = @{$settings->{process_route}};

            # $pos is -1 at end of process_route.
            my @routeA = ($pos < 0) ? @pRoute : @pRoute[0..$pos-1];

            # Remove Retry from process route to avoid duplication when the
            # route is appended back to start the failed class over.
            # If it's the end of the process route, just repeat the last 
            # class over.
            #
            my @routeB = ($pos < 0) 
                ? $pRoute[$#pRoute]
                : grep( !/^Retry$/, @pRoute[$pos-1..$#pRoute] );

            # For Retry, insert Abort and the current class to the route end.
            # Otherwise, replace the rest of the process route with the abort
            # route.
            #
            $settings->{process_route} = ($doRetry)
                ? [@routeA, 'Retry', @routeB]
                : [@routeA, 'Abort', @aRoute];
        }

        if ( $doRetry )
        {
            $status .= '_'  if (defined($status));
            $status .= RC_RQST_RETRY_STATE();
            push( @{$settings->{history}}, 'Retry' );
            $settings->{$classNm}{retry_count} += 1;
            $ret = $self->_storeRetryTag( $settings, $classNm );
            $err = $ret->Get_error();
        }
        else
        {
            push( @{$settings->{history}}, 'Abort' );
        }

        $ret = $self->setMessageData( packet => $mData );
    }

    # Update the Request record status (aka state).  Can be from input, 
    # special retry status, or the default Abort status. 
    #
    if ( ! $err && defined($settings->{record_id}) )
    {
        $status = (defined($settings->{abort_status})) 
                ? $settings->{abort_status} : RC_ABORT_STATUS()
            unless ( defined($status) );

        my $cols = [qw(id state)];
        my $vals = [$settings->{record_id}, $status];
        my $startTm = (defined($settings->{task_start}))
            ? $settings->{task_start} : time();

        # Apply any valid request column values supplied.
        map {
            if ( defined($rqstCols->{$_}) )
            {
                push( @{$cols}, $_ );
                push( @{$vals}, $rqstCols->{$_} );
            }
        } (qw(system_id user_id request_mode fallback_mode partner_id 
              delivery_time state sent_time expires));

        my $pArgs = {db_name => RC_RQST_DB(), table => RC_RQST_TABLE(),
                    columns => $cols, values => $vals, dbmode => 'insert'};

        $pArgs->{where} = sprintf( "id = %d", $settings->{record_id} );
        $pArgs->{task_start} = $startTm;
        $ret = $self->_publishDb( $pArgs );
        $err = $ret->Get_error();

# djr 5-19-16 - Move batch_id from sm.request to sm.request_batch.
#
        if ( ! $err && defined($rqstCols->{batch_id}) )
        {
            $cols = [qw(request_id batch_id)];
            $vals = [$settings->{record_id}, $rqstCols->{batch_id}];
            my $pArgs = {db_name => RC_RQST_DB(), dbmode => 'insert',
                        table => RC_RQST_BATCH_TABLE(),
                        columns => $cols, values => $vals};
            $pArgs->{where} = sprintf( "record_id = %d", 
                                       $settings->{record_id} );
            $pArgs->{task_start} = $startTm;
            $ret = $self->_publishDb( $pArgs );
            $err = $ret->Get_error();
        }
# djr 5-19-16 END
    }

    # Call publishNext to publish with the Abort route now in control.
    unless ( $err )
    {
        my $pArgs = ($doRetry && $clsRef->{retry_seconds}) 
            ? {delay => $clsRef->{retry_seconds}} : {};

        $ret = $self->publishNext( $pArgs );
        $err = $ret->Get_error();
    }
    
    if ( $err )
    {
        if ( 'SITO::Return' eq ref($ret) )
        {
            $outRet = $ret;
        }
        else
        {
            $outRet->Set_error( "$methNm Error: $err" )  if ( $err );
        }
    }

    return( $outRet );
}

=pod

B<_____________________________________________________________________________>

=head2 setBranchClass($ARGS) - Set a Branch Class

Insert a class before the next class in the current process route, and set the
inserted class up to run with the optionally supplied publish arguments.  The
next time B<publishNext> is called, message data will be published to this new
exchange queue, and processing will continue to follow the process route flow.
This method provides a way to alter the process flow dynamically during run
time so the down stream processing can be changed according to the desired
business flow.

For now, only Exchange Classes can be specified with B<setBranchClass>.  Work
Class branches may be implemented at some future date if a business case is
discovered that requires multiple Exchange Classes be combined to form a
reusuable generic Work Class.

=head3 Mandatory Arguments

=over 4

=item

$ARGS->{B<class_name>}  : The name of the exchange type class of the desired
branch.  This name must exist in the SITO::Config database as a QueueRouter
connection element for the current application environment.

=back

=head3 Optional Arguments

=over 4

=item

$ARGS->{B<publish_args>}  : The custom publishing arguments to use with named
class.  The value of this option is a hash of name/value pairs that are to be
applied to the named class during the current message flow.  These publish
arguments remain associated with the class until the altered with another call
to B<setBranchClass> or until the message processing is finished.  Default
publish arguments are taken from the message settings and cargo when this
option is not specified.

=back

=head3 Return Value

B<SITO::Return> object with the following methods defined:

    Get_error() : Fatal error message if any

=cut

sub setBranchClass
{
    my $self = shift;
    my $ARGS = ('HASH' eq ref $_[0]) ? shift : { @_ };
    my( $err, $ret, $clsNm, $clsRef, $clsType, $vals, $indx );
    my $methNm = 'setBranchClass';
    my @pRoute;
    my $config = $self->{config};
    my $mData = $self->{msgData};
    my $outRet = SITO::Return->new();
    my $sKey = (defined($config->{settings_key})) 
        ? $config->{settings_key} : RC_SETTINGS_KEY();

    # Verify input arguments - required: class_name (of type Exchange).
    $clsNm = (defined($ARGS->{class_name})) ? $ARGS->{class_name} : undef();
    if ( $clsNm )
    {
        $clsRef = (defined($config->{exchange_class}{$clsNm}))
            ? $config->{exchange_class}{$clsNm} : {};
        if ( defined($clsRef->{exchange}) )
        {
            # Assign the class specific publish arguments if specified.
            if ( exists($ARGS->{publish_args}) )
            {
                $mData->{$sKey}{publish_args}{$clsNm} = $ARGS->{publish_args};
            }

            # Get the route list position of the next class.
            $ret = $self->getNextClass();
            $err = $ret->Get_error(); 
        }
        else
        {
            $err = "Class \"$clsNm\" is not an Exchange Class so it cannot ".
                   "be used to branch.";
        }
    }
    else
    {
        $err = "Missing required class_name input argument.";
    }

    # Insert the given class in front of the next class.
    unless ( $err )
    {
        $vals = $ret->Get_vals();
        my $nextPos = $vals->{route_pos};
        my $rtRef = $mData->{$sKey}{process_route};
        @pRoute = @{$rtRef};  # copy array intended
        my $newRoute = [];
        if ( $nextPos == 0 )
        {
            unshift( @{$rtRef}, $clsNm );
        }
        elsif ( $nextPos < 0 )
        {
            # End of route position is -1.
            push( @{$rtRef}, $clsNm );
        }
        else
        {
            for ( $indx=0; $indx<=$nextPos; $indx++ )
            {
                if ( $indx == $nextPos )
                {
                    push( @{$newRoute}, $clsNm );
                    push( @{$newRoute}, @pRoute );
                    last;
                }
                else
                {
                    push( @{$newRoute}, shift(@pRoute) );
                }
            }
            @{$rtRef} = @{$newRoute};   # copy new to mData process route
        }
    }

    if ( $err )
    {
        if ( 'SITO::Return' eq ref($ret) )
        {
            $outRet = $ret;
        }
        else
        {
            $outRet->Set_error( "$methNm Error: $err" )  if ( $err );
        }
    }

    return( $outRet );
}

=pod

B<_____________________________________________________________________________>

=head2 listExchangeClasses($ARGS) - List Queue and Exchange Classes with 
settings

Return a hash of all the exchange classes in the current Queue Router
configuration database.  Each class will have at least the following attribute 
values defined: exchange, queue, and consume_pm.  Other attributes may be added
later as necessary for Queue Router development.

=head3 Mandatory Arguments

=over 4

=item

B<None>

=back

=head3 Return Value

B<SITO::Return> object with the following methods defined:

    Get_error() : Fatal error message if any
    Get_vals()  : Hash reference of the following attributes per class name:
           { exchange => $exchangeName,
             queue => $queueName,
             consume_pm => $usePath,
           }

=cut

sub listExchangeClasses
{
    my $self = shift;
    my $ARGS = ('HASH' eq ref $_[0]) ? shift : { @_ };
    my $methNm = 'listExchangeClasses';
    my $config = $self->{config};
    my $outRet = SITO::Return->new();

    my $exRef = (exists($self->{config}{exchange_class}))
        ? $self->{config}{exchange_class} : undef();

    if ( $exRef )
    {
        $outRet->Set_vals( $exRef );
    }
    else
    {
        $outRet->Set_error( "$methNm: No records of type exchange_class were ".
                        "found in the Queue Router configuraion database." );
    }

    return( $outRet );
}

=pod

B<_____________________________________________________________________________>

=head2 getQueue($ARGS) - Get Queue and Exchange names

Return the queue and exchange names associated with the specified exchange
class.  The exchange and queue names are defined in the SITO::Config database of
the QueueRouter connection elements that have a tag type of B<exchange_class>,
and this method will recognize only exchange type classes.  All others types or
classes not found in the database will return error.

=head3 Mandatory Arguments

=over 4

=item

$ARGS->{B<class_name>}  : The name of the exchange type class of the desired
queue and exchange.  This name must exist in the SITO::Config database as a
QueueRouter connection element for the current application environment.

=back

=head3 Return Value

B<SITO::Return> object with the following methods defined:

    Get_error() : Fatal error message if any
    Get_vals()  : Hash reference with the following fields defined:
           { class => $className,
             queue => $queueName,
             exchange => $exchangeName 
             consume_pm => $pmFile 
             consume_lib => $lib 
             publish_args => $pArgs
             cargo_key => $cKey
           }

=cut

sub getQueue
{
    my $self = shift;
    my $ARGS = ('HASH' eq ref $_[0]) ? shift : { @_ };
    my( $err, $ret, $clsNm, $clsRef, $clsType, $vals, $pArgs );
    my $methNm = 'getQueue';
    my $config = $self->{config};
    my $outRet = SITO::Return->new();
    my $msgData = $self->{msgData};
    my $rArgs = (exists($self->{msgData}{route_args})) 
        ? $self->{msgData}{route_args} : {};

    # Verify input arguments - required: class_name.
    $clsNm = (defined($ARGS->{class_name})) ? $ARGS->{class_name} : undef();
    if ( $clsNm )
    {
        $clsRef = (defined($config->{exchange_class}{$clsNm}))
            ? $config->{exchange_class}{$clsNm} : {};
        if ( defined($clsRef->{exchange}) )
        {
            map { $vals->{$_} = $clsRef->{$_} } keys( %{$clsRef} );
            $vals->{class} = $clsNm;

            # If publish arguments are defined for the class, use them. 
            # Otherwise, use the defaults.
            #
            if ( defined($rArgs->{$clsNm}) )
            {
                $vals->{publish_args} = $rArgs->{$clsNm};
                $vals->{cargo_key} = (defined($rArgs->{$clsNm}{cargo_key}))
                    ? $rArgs->{$clsNm}{cargo_key} : RC_ARGS_CARGO();
            }
            else
            {
                my $eArgNm = RC_ARGS_EXCHANGE();
                my $qArgNm = RC_ARGS_ROUTEKEY();
                $vals->{publish_args}{$eArgNm} = $vals->{exchange};
                $vals->{publish_args}{$qArgNm} = $vals->{queue};
                $vals->{cargo_key} = RC_ARGS_CARGO();
            }

            $outRet->Set_vals( $vals );
        }
        else
        {
            $err = "Class \"$clsNm\" is not an Exchange Class so it has no ".
                   "exchange or queue name.";
        }
    }
    else
    {
        $err = "Missing required class_name input argument.";
    }

    if ( $err )
    {
        if ( 'SITO::Return' eq ref($ret) )
        {
            $outRet = $ret;
        }
        else
        {
            $outRet->Set_error( "$methNm Error: $err" )  if ( $err );
        }
    }

    return( $outRet );
}

=pod

B<_____________________________________________________________________________>

=head2 getNextClass($ARGS) - Get the Next Class in the Process List

Get the next class to process in the current process route with regards to the
current history.  The object message data history will be evaluated to see where
we are in the process route, and return the next class name in the route.  If we
are at the end of the process route, this method will return an empty hash with
no error.  This method will be mostly called from any publish method that needs
to know where to publish next, but there may be other uses identified at a later
time.

=head3 Optional Arguments 

=over 4

=item

B<None> 

=item

=back

=head3 Return Value

B<SITO::Return> object with the following methods defined:

    Get_error() : Error message if any
    Get_vals()  : Hash with the following fields defined:
        class => $class    : Next class of the process route to process.
        exchange => $exch  : Name of the exchange belonging to the class.
        queue => $queue    : Name of the queue belonging to the class.
        publish_args => $pArgs : Publish arguments specific to the class.
        route_pos => $int  : Array position of this next class in the process
                             route list (-1 = no next class found).

=cut

sub getNextClass
{
    my $self = shift;
    my $ARGS = ('HASH' eq ref $_[0]) ? shift : { @_ };
    my( $err, $ret, $nextCls, $clsRef, $pRoute, $hist, $vals );
    my( $pArgs, $settings );
    my $routePos = -1;
    my $logFp = $self->{logFp};
    my $methNm = 'getNextClass';
    my $outRet = SITO::Return->new();
    my $mData = $self->{msgData};
    my $config = $self->{config};
    my $sKey = (defined($config->{settings_key})) 
        ? $config->{settings_key} : RC_SETTINGS_KEY();

    # Validate message data exists.  If not, this method was called out of 
    # sequence.
    #
    if ( defined($mData->{$sKey}) )
    {
        $settings = $mData->{$sKey};
        $pRoute = $settings->{process_route};
        $hist = $settings->{history};
        $pArgs = (exists($settings->{publish_args}))  
            ? $settings->{publish_args} : {};
        unless ( $#{$pRoute} >= 0 ) {
            $err = "No process_route found in the message data settings.";
        }
    }
    else
    {
        $err = "Message data has no settings.  This method was probably ".
            "called before publishStart could initialize the object.";
    }

    # Allow for classes to repeat in the process route.  Retrieve the last
    # class in history to see if it repeats in the history.  If so, compare 
    # the number of times that class repeats in history and process route.  
    # Return error if the number of history repeats is greater that process
    # route.  If less than or equal, return the next class in the process route.
    #
    unless ( $err )
    {
        if ( $#{$hist} >= 0 )
        {
            my $lastClass = $hist->[$#{$hist}];
            my $nHist = grep( /$lastClass/, @{$hist} );
            if ( $nHist == 1 )
            {
                my $indx;
                for ( $indx=0; $indx<=$#{$pRoute}; $indx++ )
                {
                    if ( $lastClass eq $pRoute->[$indx] )
                    {
                        if ( defined($pRoute->[$indx+1]) )
                        {
                            $routePos = $indx+1;
                            $nextCls = $pRoute->[$routePos];
                        }
                        last;
                    }
                }
                unless ( defined($nextCls) )
                {
                    if ( $lastClass eq $pRoute->[$#{$pRoute}] )
                    {
                        printf( $logFp 
                                "Config process route ended after ".
                                "%d classes.\n", scalar(@{$pRoute}) );
                    }
                    else
                    {
                        $err = "Process route is missing the last class in ".
                            "the router history - $lastClass.";
                    }
                }
            }
            else
            {
                # For multiple instance case, check to see if the repeats are
                # accounted for in the process route.
                #
                my $nRoute = grep( /$lastClass/, @{$pRoute} );
                if ( $nRoute >= $nHist )
                {
                    # Next class is position of last history + 1;
                    my $nextPos = $#{$hist} + 1;
                    if ( defined($pRoute->[$nextPos]) )
                    {
                        $nextCls = $pRoute->[$nextPos];
                        $routePos = $nextPos;
                    }
                }
                else
                {
                    $err = "Message data history went beyond the ".
                        "process_route.";
                }
            }
        }
        else
        {
            # No history yet - return first process route class.
            $nextCls = $pRoute->[0];
            $routePos = 0;
        }
    }

    # Get the exchange and queue information.
    if ( ! $err && $nextCls )
    {
        $ret = $self->getQueue( class_name => $nextCls );
        $err = $ret->Get_error();
    }

    if ( $err )
    {
        if ( 'SITO::Return' eq ref($ret) )
        {
            $outRet = $ret;
        }
        else
        {
            $outRet->Set_error( "$methNm Error: $err" )  if ( $err );
        }
    }
    else
    {
        if ( $nextCls )
        {
            $vals = $ret->Get_vals();
            $vals->{route_pos} = $routePos;
        }
        else
        {
            # At end of processing nextCls will not be defined.
            $vals->{route_pos} = -1;
        }

        $vals->{publish_args} = $pArgs->{$nextCls}  
            if ( exists($pArgs->{$nextCls}) );

        $outRet->Set_vals( $vals );
    }

    return( $outRet );
}

=pod

B<_____________________________________________________________________________>

=head2 consumeClasses($ARGS) - Consume Records from Class Queues

Consume all records from one or more exchange class queues and wait for more.
This method calls the AMQP::Consumer_poll method to consume records of the
specified queues, and it will continue to consume until canceled with a Kill
signal or until an error occurs with the Consumer_poll call.  Errors don't
generally happen with Consume_poll so calling this method results in a
continuous daemon like process.

To set up the consumption, this method normally loads the Perl module file named
after the class found in the SITO/Queue/Lib directory, but any file can be named
in the B<exchange_class> definition in the config database.  The PM file
inherits from RouterCore, and it overwrites the B<consumeCallback> method to
provide a callback routine to Consumer_poll.  It is in this callback that each
queue record is processed either to completion, or to publish to yet another
queue in the process route.

=head3 Mandatory Arguments 

=over 4

=item

$ARGS->{B<class_names>}  : The array of exchange class names of the queues to
consume.  The named classes must have a like named Perl module file in the
Queue/Lib directory, or some other name and location as defined by the
B<consume_pm> attribute of the class definition found in the config database to
avoid error.

=item

=back

=head3 Optional Arguments 

=over 4

=item

$ARGS->{B<max_consume>}  : Maximum number of records to consume before halting
queue consumption of any given class.  This should only be used when running in
Debug mode because the Consumer_poll process will be taken abrubtly off line
when the consume count has been reached, and one must exit the calling program
to clean up orphaned channels left behind.

=back

=head3 Return Value

B<SITO::Return> object with the following methods defined:

    Get_error() : Error message if any

=cut

sub consumeClasses
{
    my $self = shift;
    my $ARGS = ('HASH' eq ref $_[0]) ? shift : { @_ };
    my( $err, $ret, $vals, $clsNm );
    my $qSets = [];
    my $optArgs = [qw(max_consume debug)];
    my $methNm = 'consumeClasses';
    my $outRet = SITO::Return->new();

    # Validate input arguments.
    my $clsNames = (defined($ARGS->{class_names})) ? $ARGS->{class_names} : [];
    $self->{max_consume} = $ARGS->{max_consume} 
        if ( exists($ARGS->{max_consume}) );

    # Build the queue sets to be passed to Consumer_poll.
    if( 'ARRAY' eq ref($clsNames) )
    {
        foreach $clsNm (@{$clsNames} )
        {
            $ret = $self->_loadClassCb( $clsNm, $optArgs );
            $err = $ret->Get_error();
            last if ( $err );
            $vals = $ret->Get_vals();
            push( @{$qSets}, $vals )
        }
    }
    else
    {
        $err = "Required class_names argument is not an array of names.";
    }

    # Run the consumer.
    unless ( $err )
    {
        $ret = $self->{amqp}->Consumer_poll( $qSets );
        $err = $ret->Get_error();
    }

    if ( $err )
    {
        if ( 'SITO::Return' eq ref($ret) )
        {
            $outRet = $ret;
        }
        else
        {
            $outRet->Set_error( "$methNm Error: $err" )  if ( $err );
        }
    }
    else
    {
        $outRet = $ret;
    }

    return( $outRet );
}

=pod

B<_____________________________________________________________________________>

=head2 getMessageData($ARGS) - Get Message Data 

Get the message data packet owned by the current B<RouterCore> object.  The data
packet contains cargo and settings with key names that can be defined in the
Config B<cargo_key> and B<settings_key> settings specific to the application
environment.  Default values for these keys are B<cargo> and B<settings> if not
defined in the Config database.  The packet returned is a COPY of the current
object message packet.

This helper method was implemented to aid testing and debug efforts. It can be
used in conjunction with B<setMessageData> for testing to change the message
packet as needed, but one should not try to circumvent the normal process
determined settings in production.  

=head3 Optional Arguments

=over 4

=item

B<None> 

=back

=head3 Return Value

B<SITO::Return> object with the following methods defined:

    Get_error() : Fatal error message if any
    Get_vals()  : Hash reference of the message data packet

=cut

sub getMessageData
{
    my $self = shift;
    my $ARGS = ('HASH' eq ref $_[0]) ? shift : { @_ };
    my( $err, $ret, $jStr, $pkt );
    my $methNm = 'getMessageData';
    my $outRet = SITO::Return->new();

    if ( 'HASH' eq ref($self->{msgData}) )
    {
        # Copy the message data packet to avoid direct changes to self.
        # Use JSON to convert to a string, and the decode to the new reference.
        # 
        eval {
            $jStr = to_json( $self->{msgData} );
        };
        unless ( $@ )
        {
            eval {
                $pkt = from_json( $jStr );
            };
        }

        if ( $@ )
        {
            $err = "JSON error occurred during packet copy: $@";
        }
        else
        {
            $outRet->Set_vals( $pkt );
        }
    }
    else
    {
        $err = "Message data is missing or is not of the expected structure.";
        $outRet->Set_error( $err );
    }
    
    return( $outRet );
}

=pod

B<_____________________________________________________________________________>

=head2 publishDBwRequest($ARGS) - Publish DB Updates with Request Transaction

Publish one or more database updates, along with the request and request_tags
updates, all wrapped in a transaction to provision all-or-nothing updates for
the set of database records provided.  If any of the publish attempts to the
db_updates queue fail, roll back all updates.  If publishing succeeds for all
records, do the actual database updates from within a Mysql transaction.

This method not only employs the SITO::MQDb transaction scheme to skip
publishing on error, but it also specifies the B<start_task> argument of 
MQDb::Publish_db so all records in the specified transaction are guaranteed to
be published to the same queue regardless of the number of queues that MQDb 
controls.  This feature will ensure that the transaction database updates
will happen in FIFO order.

=head3 Mandatory Arguments 

=over 4

=item

$ARGS->{B<packet>}  : The message packet to use when retrieving Settings to
store in the Request tables.  This argument is required so one can call it from
places (eg., from a consumer)  that may not have the internal message packet
defined yet. 

=item

=back

=head3 Optional Arguments 

=over 4

=item

$ARGS->{B<records>}  : Array reference of database records to add to the
transaction.  Each record must be a hash with the following fields defined:
B<db_name>, B<dbmode>, B<table>, B<columns>, B<values>, B<where> (optional), and
B<macro> (optional).  The MQDb::Publish_db B<task_start> argument will be added
to each of these arrays with the current time() value so as to ensure all
updates of the transaction are sent to the same Database Updates queue.

=item

$ARGS->{B<expires>}  : Number of days to set as the expiration time for the
heavy weight request_tags table entries.  Heavy weight tags include the
following: _sito_settings, _sito_cargo, _sito_history, and _sito_status_detail.
All of these tags will be removed from the database once they expire, but the
rest of the Request tags will remain in the database.

=item

$ARGS->{B<caller>}  : Name of the caller routine to append to the record ID
when the transaction ID is created.  The transaction ID is used when defining
the MQDb update transaction, and none is specified, a special suffix of 
B<unknown_caller> will be combined with the Request record ID to form a unique
ID for the transaction.

=item

$ARGS->{B<skip_request>}  : Skip the automatic updates of the B<request> and
B<request_tags> database tables.  Use this option when you have multiple
database updates to do, and you want those updates to occur within a
transaction, but yet no Request database updates are desired.

=item

$ARGS->{B<transaction_id>}  : Use this value to uniquely identify the database
transaction rather than the normal B<caller> value plus record_id.  Use this
option in conjunction with B<skip_request> when there in no record_id value in
the packet.  Because this ID is what designates the database transaction, 
uniqueness only has to be for the life of database queue entry.

=back

=head3 Return Value

B<SITO::Return> object with the following methods defined:

    Get_error() : Error message if any

=cut

sub publishDBwRequest
{
    my $self = shift;
    my $ARGS = ('HASH' eq ref $_[0]) ? shift : { @_ };
    my( $err, $ret, $settings, $rid, $sTag, $tNm, $tVal, $valRef );
    my( $where, $macro, $pArgs, $sid ); 
    my $cols = [];
    my $vals = [];
    my @dbUpds;
    my $debug = $self->{debug};
    my $logFp = $self->{logFp};
    my $startTm = time();
    my $methNm = 'publishRequestUpdate';
    my $outRet = SITO::Return->new();
    my $config = $self->{config};
    my $mdObj = SITO::MQDb->new( dbObj => $self->{dbObj} );
    my $rqstDb = RC_RQST_DB();
    my $rqstTbl = RC_RQST_TABLE();
    my $rqstTags = RC_RQST_TAGS();
    my $saveTags = RC_SAVE_TAGS();
    my $sKey = (defined($config->{settings_key}))
        ? $config->{settings_key} : RC_SETTINGS_KEY();
    my $cKey = (defined($config->{cargo_key}))
        ? $config->{cargo_key} : RC_ARGS_CARGO();
    my $statKey = (defined($config->{status_key}))
        ? $config->{status_key} : RC_STATUS_KEY();

    # Collect and verify required input arguments.  
    my $eDays = ($ARGS->{expires} =~ /^\d+$/) ? $ARGS->{expires} : 0;
    my $msgPkt = (defined($ARGS->{packet})) ? $ARGS->{packet} : undef();
    my $recsIn = ('ARRAY' eq ref($ARGS->{records})) ? $ARGS->{records} : [];
    my $skipRequest = (defined($ARGS->{skip_request})) 
        ? $ARGS->{skip_request} : 0;
    my $txId = (defined($ARGS->{transaction_id})) 
        ? $ARGS->{transaction_id} : undef();

    # Expires date has been changed to a flag because of record volume and 
    # using Mysql partitioning to truncate the request_tags table.  If eDays
    # is set here, set it's value to 1.
    #
    my $eFld = ($eDays) ? 1 : 0;

    if ( $msgPkt )
    {
        $settings = $msgPkt->{$sKey};
        $rid = $settings->{record_id};
        $startTm = $settings->{task_start} 
            if ( defined($settings->{task_start}) );

        if ( $rid )
        {
            unless ( $txId )
            {
                $txId = sprintf( "%d_%s", $rid, (defined($ARGS->{caller})) 
                    ? $ARGS->{caller} : 'unkown_caller' );
            }
        }
        else
        {
            # Record ID is not required when calling this method with the 
            # skip_request option, but we still need a transaction ID.  Use 
            # the specified transaction_id value in this case.
            # 
            if ( $skipRequest )
            {
                $err = "The transaction_id option is required when no ".
                        "record_id value is specified in the packet."
                    unless ( $txId );
            }
            else
            {
                $err = "Required record_id field is missing in message ".
                        "Settings."
            }
        }
    }
    else
    {
        $err = "Missing required \"packet\" input argument.";
    }

    unless ( $err )
    {
        push( @dbUpds, {dbmode => 'transaction', transaction_mode => 'start', 
                task_start => $startTm, transaction_id => $txId} );

        if ( $#{$recsIn} >= 0 )
        {
            push( @dbUpds, @{$recsIn} );
        }
    }
        
    unless ( $err || $skipRequest )
    {
					#_______________________________________
					# This hash defines the request table
					# fields that will be updated in this
					# next section. Each field will be
					# updated if the settings has an entry
					# for it.
					#_______________________________________
	my $Keys = {
		     $statKey        => 'state',
		     'sent_time'     => 'sent_time',
		     'fallback_mode' => 'fallback_mode',
		   };
        # Always add or update the request record to avoid trying to insert
        # request_tags without a parent request record.
        #
        push( @{$cols}, 'id' );
        push( @{$vals}, $rid );

	map {
	      if(defined($settings->{$_}))
	        {
                  push( @{$cols}, $Keys->{$_} );
                  push( @{$vals}, $settings->{$_} );
		}
	    } keys(%$Keys);

        $sid = ($settings->{system_id} =~ /^\d+$/ ) 
            ? $settings->{system_id} : 0;

        if ( $sid > 0 )
        {
            push( @{$cols}, 'system_id' );
            push( @{$vals}, $settings->{system_id} );
        }
        
        # Insert request record or update on duplicate key.
        #
        push( @dbUpds, {db_name => RC_RQST_DB(), dbmode => 'insert',
            table => RC_RQST_TABLE(), task_start => $startTm,
            columns => $cols, values => $vals, where => "id = $rid"} );

        # Add a tag for the history and a catch-all one for settings.
        #
        foreach $sTag ( keys(%{$saveTags}) )
        {
            # Cargo and Settings are at the top level of the message packet.
            $valRef = ($sTag =~ /settings/i) 
                ? $msgPkt->{$sKey} : (($sTag =~ /cargo/i)
                    ? $msgPkt->{$cKey} : $settings->{$sTag});

            unless ( defined($valRef) )
            {
                printf( $logFp "Skipping save tag %s update as its value is ".
                        "not defined.", $sTag )  if ( $debug );
                next;
            }

            # JSON ecode any save-tags value that is not a scalar.
            $tNm = $saveTags->{$sTag};
            if ( ref($valRef) )
            {
                eval {
                    $tVal = to_json( $valRef );
                };
                if ( $@ )
                {
                    $err = "JSON encoding failed for $tNm: $@";
                    last;
                }
            }
            else
            {
                $tVal = $valRef;
            }

            $cols = [qw(request_id tag_name tag_value expires)];
            $vals = [$rid, $tNm, $tVal, $eFld];
            if ( $sid > 0 )
            {
                push( @{$cols}, 'system_id' );
                push( @{$vals}, $sid );
            }

            push( @dbUpds, {db_name => RC_RQST_DB(), dbmode => 'insert',
                    table => RC_RQST_TAGS(), task_start => $startTm,
                    columns => $cols,
                    values => $vals,
                    where => sprintf("request_id = %d AND tag_name ='%s'",
                                $rid, $tNm),
                });
        }
    }
# djr 5-19-16  - The batch_id column was removed from sm.request because the
#                request table was too large on production to add and index
#                the new column.  It was moved to sm.request_batch instead.
#                Here's where we insert a record to the request_batch table
#                whenever a batch_id value is provided in the settings packet.
#
    unless ( $err )
    {
        # Add a record to the request_batch table.
        #
        if ( defined($settings->{batch_id}) )
        {
            $cols = [qw(request_id batch_id)];
            $vals = [$rid, $settings->{batch_id}];
            push( @dbUpds, {db_name => RC_RQST_DB(), dbmode => 'insert',
                table => RC_RQST_BATCH_TABLE(), task_start => $startTm,
                columns => $cols, values => $vals, 
                where => "request_id = $rid"} );
        }
    }
# djr 5-19-16 END

    # - Publish request and request_tags table updates.
    unless ( $err )
    {
        my $didPublish = 0;
        foreach $pArgs ( @dbUpds )
        {
            # Add task_start = $startTm if not defined so transactions will 
            # always go to the same queue with the multi-queue logic.
            #
            $pArgs->{task_start} = $startTm  
                unless ( defined($pArgs->{task_start}) );

            $ret = $self->{mqDb}->Publish_db( $pArgs );
	    printf("db publish1 : %s\n", Dumper($ret)) if ( $debug );
	    printf("db publish1 args: %s\n", Dumper($pArgs)) if ( $debug );
            $err = $ret->Get_error();
            last  if ( $err );

            $didPublish = 1;
        }
        
        # Rollback or commit the transaction depending on if we published and
        # had an error of not.
        #
        my $tMode = ($err) ? 'rollback' : 'commit';
        if ( $didPublish )
        {
            $ret = $self->{mqDb}->Publish_db({ task_start => $startTm,
                        dbmode => 'transaction', transaction_id => $txId,
                        transaction_mode => $tMode });
	    printf("db publish2 : %s\n", Dumper($ret)) if ( $debug );
            $err = $ret->Get_error();
        }
    }

    if ( $err )
    {
        if ( 'SITO::Return' eq ref($ret) )
        {
            $outRet = $ret;
        }

        $outRet->Set_error(sprintf("%s Error: %s", $methNm, $err)) if ( $err );
    }

    return( $outRet );
}

=pod

B<_____________________________________________________________________________>

=head2 consumeCallback($ARGS) - Consume Callback Stub

This is the callback stub method that needs to be overwritten by the exchange 
class specific child object Perl module.  The stub always prints a message to 
the log and returns error so that we can tell when b<consumeClass> is not
working correctly.

=head3 Optional Arguments 

=over 4

=item

B<None>

=item

=back

=head3 Return Value

B<SITO::Return> object with the following methods defined:

    Get_error() : Error message if any

=cut

sub consumeCallback
{
    my $self = shift;
    my $logFp = $self->{logFp};
    my $outRet = SITO::Return->new();
    my $err = "consumeCallback Stub Error: Forgot to overwrite ".
              "this callback method with an exchange class specific one.";

    print( $logFp $err )  if ( $logFp );

    $outRet->Set_error( $err );

    return( $outRet );
}

#-------------------------------------------------------------------------------
# Private Routines
#-------------------------------------------------------------------------------
# _initExchanges
#
# Initialize all the exchanges and queues of the given exchange list by creating
# the exchanges and queues if necessary and binding the queues to their 
# exchanges.
#
# Input
#   self  : RouterCore object for the AMQP connection
#   eList : Hash reference of an array of queue names per exchange name
#
# Return
#   err   : Error message if any
#
sub _initExchanges
{
    my( $self, $eList ) = @_;
    my( $err, $ret, $descr, $exName, $qName, $qArgs, $eArgs, $bArgs );
    my( $qRef, $rKey );
    my $amqp = $self->{amqp};
   
    foreach $exName ( keys(%{$eList}) )
    {
                        # Code from Gary (MQMo.pm)
                        #_______________________________
                        # This call uses the passive
                        # mode of the exchange creation
                        # command to see if the
                        # exchange is present. If some
                        # other error beside the one
                        # reporting the absence of the
                        # exchange is returned, then a
                        # larger issue is in play and
                        # so we return with an error.
                        #
                        # If the exchange does not
                        # exist, the passive mode is
                        # removed and the creation
                        # method is called once again.
                        #_______________________________
        $eArgs = {
                exchange_name => $exName,
                passive => 1,
                exchange_type => 'direct',
                durable => 0,
                internal => 0,
            };
        $ret = $amqp->Create_exchange( $eArgs );
        $err = $ret->Get_error();
        $descr = $ret->Get_descr();
        if ( $err && $descr =~ /no exchange/ )
        {
            delete( $eArgs->{passive} );
            $ret = $amqp->Create_exchange( $eArgs );
            $err = $ret->Get_error();
        }

        last if ( $err );

                        #_______________________________
                        # As was done above for the
                        # exchange, the same is done
                        # here for the queue. If the
                        # queue is created, the new
                        # queue is also bound to the
                        # appropriate exchange.
                        #_______________________________
        foreach $qRef ( @{$eList->{$exName}} )
        {
            $qName = $qRef->{name};

            # Notify Class type exchanges normally don't have queues to bind.
            next  unless ( defined($qName) );

            # When we are not broadcasting to multiple queues, routeKey is
            # always the same as queueNm.
            #
            $rKey = (defined($qRef->{route_key})) ? $qRef->{route_key} : $qName;
            $qArgs = { 
                queue => $qName,
                passive => 1,
                exclusive => 0,
                durable => 0,
                auto_delete => 0,
            };
            $ret = $amqp->Create_queue( $qArgs );
            $err = $ret->Get_error();
            $descr = $ret->Get_descr();
            if ( $err && $descr =~ /no queue/ )
            {
                $qArgs->{passive} = 0;
                $ret = $amqp->Create_queue( $qArgs );
                $err = $ret->Get_error();
                last if ( $err );
            }

            $bArgs = {
                bind_queue => $qName,
                bind_exchange => $exName,
                routing_key => $rKey,
            };
            $ret = $amqp->Bind_queue( $bArgs );
            $err = $ret->Get_error();
            last if ( $err );
        }
    }

    return( $err );
}
#-------------------------------------------------------------------------------
# _getJSON - Get JSON Perl elements
#
# Get one or all of the Perl elements defined in the given JSON string.  If 
# the input string causes JSON decode to puke, log the error and return 
# nothing.
#
# Input
#   self  : RouterCore object to use with logging
#   inStr : Input JSON formatted string
#   field : Optional field to return
#
# Return
#   retRef : Reference to Perl that is produced with the JSON decode and
#           optional field look up.
#
sub _getJSON
{
    my( $self, $inStr, $field ) = @_;
    my $retRef;
    my $logFp = $self->{logFp};

    eval {
        $retRef = from_json( $inStr );
    };
    if ( $@ )
    {
        printf( $logFp "_getJSON Failed: $@" )  if ( $logFp );
    }
    elsif ( defined($field) && 'HASH' eq ref($retRef) && 
            exists($retRef->{$field}) )
    {
        $retRef = $retRef->{$field};
    }

    return( $retRef );
}

#-------------------------------------------------------------------------------
# _loadClassCb
#
# Load all the necessary Consumer_poll argument for the given class name.  As
# part of the load, instantiate the class perl module and add the callback 
# referenc to the return arguments.
#
# Input
#  self    : RouterCore object for method access
#  clsName : Exchange class name to load
#  optArgs : Optional arguments allowed by consumeClasses
#
# Return
#  SITO::Return object with the following methods defined:
#   Get_error() : Error message if any
#   Get_vals()  : Hash of Consumer_poll arguments for the specifed class
#
sub _loadClassCb
{
    my( $self, $clsName, $optArgs ) = @_;
    my( $err, $ret, $vals, $cArgs, $pmLib, $cb, $libPath );
    my $outRet = SITO::Return->new();

    $ret = $self->getQueue( class_name => $clsName );
    $err = $ret->Get_error();

    unless ( $err )
    {
        $vals = $ret->Get_vals();
        $cArgs->{queue_name} = $vals->{queue}  if ( defined($vals->{queue}) );
        $pmLib = $vals->{consume_pm}  if ( defined($vals->{consume_pm}) );
        $libPath = $vals->{consume_lib}  if ( defined($vals->{consume_lib}) );
        

        if ( $cArgs->{queue_name} )
        {
            if ( $libPath )
            {
                eval( sprintf("use lib (qw(%s));", join(' ',@$libPath)) );
            }

            unless ( $@ )
            {
                $pmLib = sprintf( "%s::%s", RC_DEFAULT_PMLIB(), $clsName )
                    unless ( $pmLib );

                eval( 'use ' . $pmLib );
            }

            if ( $@ )
            {
                $err = "Library load Failed: $@";
            }
        }
        else
        {
            $err = "Class $clsName is missing a queue definition. ";
        }
    }

    unless ( $err )
    {
        my $cbArgs = {};
        map { $cbArgs->{$_} = $self->{$_} if(defined($self->{$_})) 
            } @{$optArgs};

        eval {
   
            $ret = $pmLib->new( $cbArgs );
            if ( 'SITO::Return' eq ref($ret) )
            {
                $cb = $ret->Get_vals();
                $err = $ret->Get_error();
            }
            else
            {
                $cb = $ret;
                $err = $ret->{error}  if ( defined($ret->{error}) );
            }
            $cb = ('SITO::Return' eq ref($ret)) ? $ret->Get_vals() : $ret;
        };
        if ( $@ || $err )
        {
            $err = sprintf( "consumeCallback reference Failed: %s", 
                            ($err) ? $err : $@ );
        }
        else
        {
            $cArgs->{callback} = sub { return($cb->consumeCallback(@_)); };

            $cArgs->{qmax} = (defined($self->{max_consume})) 
                        ? $self->{max_consume} : RC_QUEUE_MAX();
        }
    }

    if ( $err )
    {
        if ( 'SITO::Return' eq ref($ret) && $err eq $ret->Get_error() )
        {
            $outRet = $ret;
        }
        else
        {
            $outRet->Set_error( $err )  if ( $err );
        }
    }
    else
    {
        $outRet->Set_vals( $cArgs );
    }

    return( $outRet );
}
#-------------------------------------------------------------------------------
# _macroToString
#
# Given a hash reference of name-value pairs with possible macro values, expand
# those macros using self as the source for the macro expansion.  If the value
# in self is something other than a scalar, convert that value to JSON.  This
# routine will allow one to specify a macro such as %%msgData%%, and then have
# it expanded to the current value at publish time.  For now, this routine only
# expands macros in a hash, and only at the top level.
#
# Input
#  self    : RouterCore object for method access
#  srcRef  : Hash of name-value pairs to expand
#
# Return
#  tgtRef  : Hash with macros expanded.
#
sub _macroToString
{
    my( $self, $srcRef ) = @_;
    my( $fld, $name, $val, $tgtRef, $jStr );
    my $methNm = '_macroToString';
    my $logFp = $self->{logFp};

    if ( 'HASH' eq ref($srcRef) )
    {
        $tgtRef = {%{$srcRef}};   # copy hash intended 
        foreach $fld ( keys(%{$tgtRef}) )
        {
            if ( $tgtRef->{$fld} =~ /^%%(\w+)%%$/ )
            {
                $name = $1;
                if ( defined($self->{$name}) )
                {
                    $val = $self->{$name};
                    if ( ref($val) )
                    {
                        eval {
                            $jStr = to_json( $val );
                        };
                        if ( $@ )
                        {
                            printf( $logFp "%s JSON failure IGNORED: %s", 
                                    $methNm, $@ ); 
                        }
                        else
                        {
                            $tgtRef->{$fld} = $jStr;
                        }
                    }
                    else
                    {
                        $tgtRef->{$fld} = $val;
                    }
                }
                else
                {
                    printf( $logFp "%s failure IGNORED: \"%s\" is not defined ".
                            "in self.", $methNm, $name ); 
                }
            }
        }
    }
    else
    {
        $tgtRef = $srcRef;
    }

    return( $tgtRef );
}

#-------------------------------------------------------------------------------
# _publishDelay
#
# Publish to the Delay queue for the given class so the target queue processing
# will be delayed for the specified delay seconds.
#
# Input
#  self    : RouterCore object for method access
#  clsRef  : Hash of class reference name-value pairs
#  dSecs   : Number of seconds to delay
#
# Return
#  SITO::Return object with the following methods defined:
#   Get_error() : Error message if any
#
sub _publishDelay
{
    my( $self, $clsRef, $dSecs ) = @_;
    my( $err, $ret, $vals, $dArgs, $mqdObj, $jStr );
    my $methNm = '_publishDelay';
    my $outRet = SITO::Return->new();

    if ( $dSecs =~ /^\d+$/ )
    {
        $ret = SITO::MQDelay->new(); 
        $err = $ret->Get_error();
        $mqdObj = $ret->Get_vals()  unless( $err );
    }
    else
    {
        $err = "Unexpected value received for the Delay option - $dSecs";
    }

    # Gather the Delay arguments, but skip JSON conversion of payload as 
    # delayed_request is expecting a hash.
    #
    unless ( $err )
    {
        $dArgs = { expire_delta => $dSecs,
                   target_exchange => $clsRef->{exchange},
                   target_route => $clsRef->{queue},
                   payload => $self->{msgData} };

        # Publish the delay.
        $ret = $mqdObj->Publish_delayed_request( $dArgs );
        $err = $ret->Get_error();
    }

    if ( $err )
    {
        if ( 'SITO::Return' eq ref($ret) )
        {
            $outRet = $ret;
        }
        else
        {
            $outRet->Set_error( "$methNm: $err" )  if ( $err );
        }
    }

    return( $outRet );
}

#-------------------------------------------------------------------------------
# _stopOnMax
#
# Stop consumer when maximum consume count is reached.
#
# Input
#  self    : RouterCore object for method access
#  methNm  : Name of method calling this routine (for print to log)
#
sub _stopOnMax
{
    my( $self, $methNm ) = @_;
    my $maxCnt = $self->{max_consume};
    my $cntNow = $self->{consume_count};
    my $logFp = $self->{logFp};

    if ( defined($maxCnt) )
    {
        $self->{consume_count} += 1;
        my $cntNow = $self->{consume_count};
        my $maxCnt = $self->{max_consume};
        if ( $cntNow >= $maxCnt )
        {
            printf( $logFp "%s took the consumer off line after a max ".
                "consume count of %d was reached.", $methNm, $maxCnt );

            $self->{amqp}{consumer_online} = 0;
        }
    }
}

#-------------------------------------------------------------------------------
# _dbNow
#
# Get the database value for now() imediately rather than waiting to set it 
# when the record is processed from the db_updates queue.  Most times this will
# be close to using the now() macro, but just in case there is a delay with 
# consuming the db queue, one can call this routine to get the immediate now()
# date-time.
#
# Input
#  self    : RouterCore object for method access
#
# Return
#  dbNow   : Immediat now() time in the default Mysql format
#
sub _dbNow
{
    my $self = shift;
    my $dbObj = $self->{dbObj};
    my( $rowPs, $dbNow );

    eval {
        $rowPs = $dbObj->fetchall_AoH( query => "SELECT now() AS 'DbNow'" );
    };
    unless ( $@ )
    {
        $dbNow = $rowPs->[0]{DbNow};
    }

    return( $dbNow );
}

#-------------------------------------------------------------------------------
# _addRequestDbRec
#
# Add a record to the request table and zero or more records to the request_tags
# table with the message packet fields.  The message packet settings should
# contain a record_id value by now, and that value becomes the ID of the 
# request record.  Some of the request table columns like user_id and system_id
# may not be defined yet so the new request insert will take whatever defaults
# the database may have or NULL for those columns.
#
# Input
#  self  :  RouterCore object for database handle
#  cargo : Cargo hash already decoded
#
# Return
#  SITO::Return object with the following methods set:
#    Get_error()  : Retrieve error message if any
#
sub _addRequestDbRec
{
    my( $self, $cargo ) = @_;
    my( $err, $ret, $sid, $uid, $rCols, $rVals, $tagNm, $tagVal, $sName );
    my( $txId, $recId, $pArgs );
    my @dbUpds;
    my $methNm = '_addRequestDbRec';
    my $startTm = time();
    my $didPublish = 0;
    my $dbName = RC_RQST_DB();
    my $outRet = SITO::Return->new();
    my $mData = $self->{msgData};
    my $config = $self->{config};
    my $sKey = (defined($config->{settings_key})) 
        ? $config->{settings_key} : RC_SETTINGS_KEY();
    my $settings = (exists($mData->{$sKey})) ? $mData->{$sKey} : undef();

    # Collect request table column values and format the database insert record
    # within a transaction.
    #
    if ( $settings && $cargo )
    {
        $sid = $settings->{system_id};
        $rCols = ['id','state','received'];
        if ( defined($settings->{record_id}) )
        {
            $recId = $settings->{record_id};
            $rVals = [$recId, RC_RQST_NEW_STATE(), 1 ];
            map { if (defined($settings->{$_})) { push(@{$rCols}, $_); 
                    push(@{$rVals}, $settings->{$_}); }
            } qw(system_id request_mode);

            if ( defined($settings->{user_record}) )
            {
                push( @{$rCols}, 'user_id'); 
                push( @{$rVals}, $settings->{user_record}{user_id} );
            }

            # Retrieve the task_start time or assign it if not defined so all
            # updates within the transaction will go to the same db queue.
            #
            if ( defined($settings->{task_start}) )
            {
                $startTm = $settings->{task_start};
            }
            else
            {
                $settings->{task_start} = $startTm;
            }

            # Start with a transaction record so request and request_tags 
            # inserts are all or nothing.
            #
            $txId = $recId . '_trans';
            push( @dbUpds, {dbmode => 'transaction', 
                    transaction_mode => 'start', task_start => $startTm, 
                    transaction_id => $txId} );
            
            # Add the request record to the update list.
            push( @dbUpds, {db_name => $dbName, table => RC_RQST_TABLE(),
                    task_start => $startTm, dbmode => 'insert',
                    columns => $rCols, values => $rVals,
                    macro => {received => 'IF(?="1",now(),now())'} } );

# djr 5-19-16 - Move batch_id from sm.request to sm.request_batch.  Production
#               sm.request was too big to add the batch_id column with index so
#               we created the sm.request_batch table to link request with 
#               request_batch_tags.
#
            if ( defined($settings->{batch_id}) )
            {
                $rCols = [qw(request_id batch_id)];
                $rVals = [$recId, $settings->{batch_id}];
                push( @dbUpds, {db_name => $dbName, dbmode => 'insert',
                    table => RC_RQST_BATCH_TABLE(), task_start => $startTm, 
                    columns => $rCols, values => $rVals} );
            }
# djr 5-19-16 END
        }
        else
        {
            $err = "Message data is missing the required record_id setting.";
        }
    }
    else
    {
        $err = "Message data is missing a settings and/or cargo definition.";
    }

    # Add a tag record to the transaction for each cargo key and value.
    #
    unless ( $err )
    {
        foreach $tagNm ( keys(%{$cargo}) )
        {
            # If the value is not a scalar, convert it to JSON before adding
            # the tag to the update list.
            #
            ($pArgs, $err) = $self->_formatRequestTag( $recId, $sid, $tagNm, 
                                            $cargo->{$tagNm}, $startTm );
            last  if ( $err );
            push( @dbUpds, $pArgs );
        }
    }

    # Add tags for status detail and process history to updated later.
    #
    my $saveTags = RC_SAVE_TAGS();
    unless ( $err )
    {
        foreach $sName ( qw(request_status_detail history) )
        {
            $tagNm = $saveTags->{$sName}; 
            $tagVal = (defined($settings->{$sName})) ? $settings->{$sName} : '';
            ($pArgs, $err) = $self->_formatRequestTag( $recId, $sid, $tagNm,
                                                    $tagVal, $startTm, 1 );
            last if ( $err );
            push( @dbUpds, $pArgs );
        }
    }

    # Store the settings for down stream use.
    unless ( $err )
    {
        ($pArgs, $err) = $self->_formatRequestTag( $recId, $sid, 
                           $saveTags->{settings}, $settings, $startTm, 1 );
        push( @dbUpds, $pArgs )  unless ( $err );
    }

    # Publish the request and request_tags entries.
    unless ( $err )
    {
        foreach $pArgs ( @dbUpds )
        {
            $ret = $self->{mqDb}->Publish_db( $pArgs );
            $err = $ret->Get_error();
            last  if ( $err );

            $didPublish = 1;
        }
    }

    # Rollback or commit the transaction depending on if we published and
    # had an error of not.
    #
    my $tMode = ($err) ? 'rollback' : 'commit';
    if ( $didPublish )
    {
        $ret = $self->{mqDb}->Publish_db({ task_start => $startTm,
                        dbmode => 'transaction', transaction_id => $txId,
                        transaction_mode => $tMode });
        $err = $ret->Get_error();
    }

    if ( $err )
    {
        if ( 'SITO::Return' eq ref($ret) )
        {
            $outRet = $ret;
        }
        else
        {
            $outRet->Set_error( "$methNm: $err" );
        }
    }

    return( $outRet );
}

#-------------------------------------------------------------------------------
# _formatRequestTag
#
# Format the given tag name and value to create a hash of the request_tags 
# record to insert to the database for the given record ID.
#
# Input
#  self  : RouterCore object for database handle
#  recId : Record ID of the tag record
#  sid   : System ID of the tag record
#  name  : Tag name of the record
#  value : Tag value of the record to add
#  tTime : Task start time to assign to the record
#  eDays : Days until the record expires
#
# Return
#  tagRef : Tag record hash reference to add to the insert list
#  err    : Error message if any
#
sub _formatRequestTag
{
    my( $self, $recId, $sid, $name, $value, $tTime, $eDays ) = @_;
    my( $err, $tagRef, $tagVal, $cols, $vals );
    my $methNm = '_formatRequestTag';
    $sid = 0  unless ( defined($sid) );

    # The expires column has been converted to a flag value for data truncation.
    my $eFld = ($eDays) ? 1 : 0;
    
    if ( $recId && $name )
    {
        $value = ''  unless ( defined($value) );
        if ( ref($value) )
        {
            eval {
                $value = to_json( $value )  
            };
            if ( $@ )
            {
                $err = sprintf( "JSON conversion error on %s value: %s",
                                $name, $@ );
                last;
            }
        }
            
        $cols = [ qw(request_id system_id tag_name tag_value expires) ];
        $vals = [ $recId, $sid, $name, $value, $eFld ];

        $tagRef = {db_name => RC_RQST_DB(), 
                table => RC_RQST_TAGS(), dbmode => 'insert',
                columns => $cols, values => $vals};

        $tagRef->{task_start} = $tTime  if ( defined($tTime) );
    }
    else
    {
        $err = "Input to $methNm is missing required record_id and/or ".
               "tag_name input.";
    }

    return( $tagRef, $err );
}

#-------------------------------------------------------------------------------
# _storeRetryTag
#
# Store a record in the request_tags table for the retry so we can report on
# retries later.  The tag name will be the same for all the retries of a given
# message so the report queries that select by the indexed tag_name will be
# fast.  Tag value is a hash of error messages per retry number (key: retry_99),
# and the hash will be JSON encoded.  Actual writing of the database record is
# left up to the consumer of the database updates queue.
#
# Input
#  self    : RouterCore object for database handle
#  sData   : Settings portion of the message data
#  classNm : Name of the class to be retried
#
# Return
#  SITO::Return object with the following methods set:
#    Get_error()  : Retrieve error message if any
#
sub _storeRetryTag
{
    my( $self, $sData, $classNm ) = @_;
    my( $err, $jVal, $rHist, $cols, $vals );
    my $ret = SITO::Return->new();
    my $sdKey = RC_STATUS_DET_KEY();
    my $rKey = sprintf( "retry_%d", $sData->{$classNm}{retry_count} );
    my $recId = (defined($sData->{record_id})) ? $sData->{record_id} : undef();
    my $errMsg = (defined($sData->{sito_return}{descr}))
        ? $sData->{sito_return}{descr}
        : ((defined($sData->{$sdKey})) ? $sData->{$sdKey} : undef());
    

    if ( $recId && $errMsg )
    {
        # Append the retry number and message to the retry history if it 
        # exists.  Retry history is the JSON string stored to request_tags.
        #
        $rHist = from_json( $sData->{$classNm}{retry_history} )
            if ( defined($sData->{$classNm}{retry_history}) ) ;

        $rHist->{$rKey} = $errMsg;
        $jVal = to_json( $rHist );
        $sData->{$classNm}{retry_history} = $jVal;

        $cols = [qw(request_id tag_name tag_value expires)];
        $vals = [$recId, '_sito_retry', $jVal, 0];

        my $pArgs = {db_name => RC_RQST_DB(), table => RC_RQST_TAGS(),
                    columns => $cols, values => $vals, dbmode => 'insert'};

        $pArgs->{where} = sprintf( "request_id = %d", $sData->{record_id} );
        $pArgs->{task_start} = $sData->{task_start}  
            if ( defined($sData->{task_start}) );

        $ret = $self->_publishDb( $pArgs );
    }

    return( $ret );
}
            
#-------------------------------------------------------------------------------
# _publishDb
#
# Given a set of database update definitions, call MQDb to publish an update
# to the specified database.
#
# Input
#  self    : RouterCore object for database handle
#  sData   : Settings portion of the message data
#  classNm : Name of the class to be retried
#
# Return
#  SITO::Return object with the following methods set:
#    Get_error()  : Retrieve error message if any
#
sub _publishDb
{
    my( $self, $pArgs ) = @_;
    my( $ret, $err );
    my $outRet = SITO::Return->new();

    # Some MQDb settings may be defined in RouterCore object that affect
    # database queues.  We may decide to create multiple update queues 
    # later, and this allows us to specify something other than the default
    # db_updates queue.
    #
    my $mdArgs = {};
    map { $mdArgs->{$_} = $self->{$_}  if ( defined($self->{$_}) ); 
    } qw( dbObj queue_name );

    my $mdObj = SITO::MQDb->new( $mdArgs );
    if ( $mdObj->{error} )
    {
        $err = $mdObj->{error};
    }
    else
    {
        $ret = $mdObj->Publish_db( $pArgs );
        $err = $ret->Get_error();
    }

    if ( $err )
    {
        if ( 'SITO::Return' eq ref($ret) )
        {
            $outRet = $ret;
        }
        else
        {
            $outRet->Set_error( $err )  if ( $err );
        }
    }

    return( $outRet );
}

#-------------------------------------------------------------------------------
# _publishExchange
#
# Publish to the named class with the given publish arguments.
#
# Input
#  self    : RouterCore object for method access
#  clsNm   : Name of the class to publish (add this name to history)
#  pArgs   : Publish arguments set so far
#
# Return
#  SITO::Return object with the following methods defined:
#   Get_error() : Error message if any
#
sub _publishExchange
{
    my( $self, $clsNm, $pArgs ) = @_;
    my $jCargo;
    my $ret = SITO::Return->new();
    my $config = $self->{config};
    my $mData = $self->{msgData};
    my $sKey = (defined($config->{settings_key})) 
        ? $config->{settings_key} : RC_SETTINGS_KEY();
    my $cKey = RC_ARGS_CARGO();

    # Add the next class to the history, and format the message data
    # with JSON.
    #
    push( @{$mData->{$sKey}{history}}, $clsNm );
    eval {
        $jCargo = to_json( $mData );
    };
    if ( $@ )
    {
        $ret->Set_error( "JSON converion Failed - $@" );
    }
    else
    {
        $pArgs->{$cKey} = $jCargo;
        my $aOpts = $config->{AMQP_Publish_exchange}{optional}
            if ( defined($config->{AMQP_Publish_exchange}) );
    
        map { $pArgs->{$_} = $aOpts->{$_}; } keys( %{$aOpts} );

        $ret = $self->{amqp}->Publish_exchange( $pArgs );
    }

    return( $ret );
}

#-------------------------------------------------------------------------------
# _addClassToRoute
#
# Add the given class name to the process route so the next call to publishNext
# will not complain about the history being out of sync with the process route.
#
# Input
#  self    : RouterCore object for method access
#  clsNm   : Name of the class to publish (add this name to history)
# 
# Return
#  err     : Error message if any
#
sub _addClassToRoute
{
    my( $self, $clsNm ) = @_;
    my( $err, $vals, $pos, $ret );
    my $config = $self->{config};
    my $sKey = (defined($config->{settings_key})) 
        ? $config->{settings_key} : RC_SETTINGS_KEY();

    my $settings = $self->{msgData}{$sKey};
    
    if ( 'ARRAY' eq ref($settings->{process_route}) )
    {
        my @pRoute = @{$settings->{process_route}};

        if ( $#pRoute >= 0 )
        {
            $ret = $self->getNextClass();
            $err = $ret->Get_error();
            unless ( $err )
            {
                $vals = $ret->Get_vals();
                $pos = $vals->{route_pos};

                if ( $pos >= 0 )
                {
                    my @routeA = @pRoute[0..$pos-1];
                    my @routeB = @pRoute[$pos..$#pRoute];
                    $settings->{process_route} = [@routeA, $clsNm, @routeB];
                }
                else
                {
                    # Add the class to the end of the route if no next is found.
                    push( @{$settings->{process_route}}, $clsNm );
                }
            }
        }
    }
    else
    {
        $err = "Something clobbered the message settings process_route array.";
    }

    return( $err );
}

#-------------------------------------------------------------------------------

=pod

=head1 DEPENDENCIES

    SITO::Config
    SITO::AMPQ

=head1 AUTHOR

David Runyan, Single Touch Systems Inc.

=head1 VERSION

Version 1.0

=head1 SEE ALSO

=cut

# Return true
1;
