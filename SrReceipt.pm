#-------------------------------------------------------------------------------
# Queue::Lib::SrReceipt - Queue Library, SendRequest Receipt
#
# This perl module provides a support for the SrReceipt exchange class queue
# consumption by defining a callback routine for Queue Router processing of the
# SendRequest Receipt queue.  The purpose of the callback is to receive a
# message request by verifying access credentials, applying defaults settings,
# verifying the required inputs are provided in the request, and notifying the
# Node server of the request status. Upon successful verification, this module
# publishes to other queues for each record in the request list.
#
# SITO Mobile LTD., Boise, David Runyan, December 2014
#-------------------------------------------------------------------------------
package SITO::Queue::Lib::SrReceipt;

use strict;

use SITO::Queue::RouterCore;
use SITO::Queue::Lib::SrUtil;
use SITO::JSON;
use SITO::System;
use SITO::RedisPoll;
use SITO::MQDelay;
use SITO::Tools (qw(Copy_struct));
use Data::Dumper;
use Date::Manip (qw(Date_TimeZone));
our @ISA = (qw(SITO::Queue::RouterCore));

=head1 NAME - SITO::Queue::Lib::SrReceipt

B<SrReceipt> - SITO Queue SendRequest Receipt Processing

=head1 ABSTRACT

SITO::Queue::Lib::SrReceipt defines the callback method needed to consume the
SendRequest Receipt queue.

=head2 SrReceipt - Intended for Internal Use

This object is intended for specific use within the program or class structure
that spawned its creation, and as such, the author did not feel it necessary to
publish the method usage.  Comments inside SrReceipt.pm should help one
understand its purpose and functions.  If you find a need to use this object
beyond its current scope, please add the standard STI pod elements to make it
easier to understand for wider use.

=cut

#-------------------------------------------------------------------------------
# new
#
# Create a new instance of SrReceipt that will inherit from RouterCore all
# methods that callback needs to process records of the Walmart Error queue.
#
# Optional Input
#   dbObj   => $dbO     : Connected DB object
#   logFp   => $logFp   : Log file pointer 
#
# Return
#   SITO::Queue::Lib::SrReceipt object pointer
#
sub new
{
    my $class = shift;
    my $ARGS = ('HASH' eq ref $_[0]) ? shift : { @_ };

    # Get base definitions from parent.
    my $self = $class->SUPER::new( $ARGS );

    bless( $self, $class );

    my $mdArgs = (defined($self->{dbObj})) ? {dbObj => $self->{dbObj}} : {};
    $self->{mqDb} = SITO::MQDb->new( $mdArgs );
    $self->{error} = $self->{mqDb}{error}  if ( $self->{mqDb}{error} );

    return( $self );     
}

#-------------------------------------------------------------------------------
# consumeCallback
#
# Process the SendRequest Receipt queue records by performing the following 
# tasks:
# - Decode the JSON cargo packet of the request.
# - Apply defaults to any settings missing in the packet.
# - Verify the required settings are defined in the packet.
# - Notify the Node server of the verification status.
# - Assign a batch ID to the batch request
# - Publish each record in the request list to the appropriate queue.
#   Phone records to the phone_p1 queue, segment records to the
#   segment queue.
#
# Input
# Arguments are passed to this method by AMQP::Consumer_poll, and the following
# arguments will be available from the calling routine:
# 
#  data        : Queue record (in JSON string format) to be processed
#  channel_set : (not used) Collection of consumer specifications of the 
#                connection
#  my_channel  : (not used) Channel number that is assigned to the consumer
#                process running this callback
#
# Return
#  SITO::Return object with the following methods defined:
#    Get_vals()   : Hash with the following keys defined:
#                   ack => 1    : To acknowlege the consumption
#
sub consumeCallback
{
    my $self = shift;
    my $ARGS = ('HASH' eq ref $_[0]) ? shift : { @_ };
    my( $err, $ret, $vals, $msgPkt, $cargo, $recId, $reqList, $cData );
    my( $sysNm, $sid, $batchId, $segReqsts, $phReqsts, $sArgs, $sCode );
    my $donePhones = {};
    my $doneSegs = {};
    my $common = {};
    my $settings = {};
    my $classNm = 'SrReceipt';
    my $outRet = SITO::Return->new();
    my $logFp = $self->{logFp};
    my $config = $self->{config};
    my $data = $ARGS->{data};
    my $startTm = time();
    my $sKey = (defined($config->{settings_key}))
        ? $config->{settings_key} : $self->RC_SETTINGS_KEY();
    my $cKey = (defined($config->{cargo_key}))
        ? $config->{cargo_key} : $self->RC_ARGS_CARGO();
    my $srConf = (defined($config->{send_request})) 
        ? $config->{send_request} : {};
    my $wConf = $config->{work_class}{Send_request};  
    my $phClass = $self->SR_DEFAULT_PHONE_CLASS();
    my $exConf = $config->{exchange_class}{SrReceipt};
    my $srEx = $exConf->{exchange};
    my $srRecptQ = $exConf->{queue};
    my $minDelay = (defined($exConf->{min_delay})) 
        ? $exConf->{min_delay} : 3600;
    my $srStates = $self->SR_BATCH_STATES();
    my $svrTz = (defined($config->{server_timezone}))
        ? $config->{server_timezone} : Date_TimeZone();

    # Proper case time zone.
    $svrTz =~ s/([a-z]+)/\u\L$1/ig;

    printf( $logFp "------In %s------\n", $classNm );

    # - Decode the JSON cargo packet of the request.
    if ( defined($data) )
    {
    printf($logFp "in: %s\n", $data) if($self->{'debug'});
        $msgPkt = $self->_getJSON( $data );
        $err = "JSON decode of queue data failed."  unless ( $msgPkt );
        unless ( $err )
        {
            if ( defined($msgPkt->{$cKey}) )
            {
                $cargo = ('HASH' eq ref($msgPkt->{$cKey}))
                    ? $msgPkt->{$cKey} : $self->_getJSON( $msgPkt->{$cKey} );
                $sysNm = $cargo->{systemName} 
                    if ( defined($cargo->{systemName}) ); 
                $sid = $cargo->{system_id}
                    if ( defined($cargo->{system_id}) ); 

                # Client data expected in the 'json' string as fed by Node.
                if ( defined($cargo->{json}) )
                {
                    $cData = $self->_getJSON( $cargo->{json} );
                    $common = $cData->{common} if ( defined($cData->{common}) );
                    $reqList = $cData->{requests} 
                        if ( 'ARRAY' eq ref($cData->{requests}) );
                }
            }
            $settings = $msgPkt->{$sKey}  if ( defined($msgPkt->{$sKey}) );
        }
    }
    else
    {
        $err = "No data set handed to callback.";
    }

    # Move send time from cargo to settings for ready access.
    if ( ! $err && defined($cData->{sendTime}) )
    {
        $settings->{sendTime} = $cData->{sendTime};
        delete( $cData->{sendTime} );
    }

    # Copy any Work Class settings that are not already defined in the message
    # packet.  This is needed to load things like user_cid_field.
    #
    unless ( $err )
    {
        map {
            $settings->{$_} = $wConf->{$_} unless ( defined($settings->{$_}) );
        } keys( %{$wConf} );
    }

    # System name or ID is required in the SrReceipt cargo.  Get the ID from 
    # system name if no ID is found.  Return error if no ID is found matching
    # the name.
    #
    unless ( $err )
    {
        $sArgs = (defined($sid)) ? {system_id => $sid} 
            : ((defined($sysNm)) ? {system_name => $sysNm} : undef());
        if ( defined($sArgs) )
        {
            $ret = SITO::System->new();
            $err = $ret->Get_error();
            unless ( $err )
            {
                my $sysOb = $ret->Get_vals();
                $ret = $sysOb->Get_system( $sArgs );
                $err = $ret->Get_error();
                unless ( $err )
                {
                    $vals = $ret->Get_vals();
                    $sid = $vals->{system_id};
                    $sysNm = $vals->{system_name};
                    $sCode = $vals->{system_csc};
                }
            }
        }
        else
        {
            $err = "Message cargo is missing the required systemName or ".
                    "system_id value.";
        }

        unless ( $err || (defined($sysNm) && defined($sid)) )
        {
            $err = "System name or system ID is missing from database ".
                   "(system_name=$sysNm, system_id=$sid).";
        }
    }

    # - Apply defaults to any settings missing in the packet.
    #   Defaults per system ID are possible in the send_request type Config 
    #   settings.
    #
    unless ( $err )
    {
        # Assign the packet system ID to that value found above.
        $settings->{system_id} = $sid;
        $settings->{system_csc} = $sCode;
        $settings->{systemName} = $sysNm;
        $settings->{task_start} = $startTm;

        # Pass along the Node received time for later SrDelivery processing.
        $settings->{received} = $cargo->{received}  
            if ( defined($cargo->{received}) );

        # System specific defaults can be defined in Config with this format:
        # tag_type = 'send_request'
        # tag_name = sidToTag
        # tag_value = {"sid1":{"defaults":{}...}}
        # 
        #
        my $sDefNm = ('HASH' eq ref($srConf->{sidToTag}{$sid}))
            ? $srConf->{sidToTag}{$sid}{defaults} : undef();
        my $sDeflts = (defined($sDefNm) && 'HASH' eq ref($srConf->{$sDefNm}))
            ? $srConf->{$sDefNm} : {};

        # Add system default values for any missing in the cargo common.
        map {
            $common->{$_} = $sDeflts->{$_} unless ( defined($common->{$_}) );
        } keys( %{$sDeflts} );

        my $cDeflts = ('HASH' eq ref($srConf->{common_defaults}))
            ? $srConf->{common_defaults} : {};

        # Add common default values for any common tags yet missing.
        map {
            $common->{$_} = $cDeflts->{$_} unless ( defined($common->{$_}) );
        } keys( %{$cDeflts} );
    }

    # - Verify the required settings are defined in the packet.
    my $segNms = [];
    unless ( $err )
    {
        # Extract all segment names from the request list to initalize in the
        # Redis batch and batch tags.
        #
        if ( $#{$reqList} >= 0 )
        {
            map {
                push( @{$segNms}, $_->{segment} ) if ( defined($_->{segment}) );
            } @{$reqList};
        }
        else
        {
            $err = "No Requests were provided to process.";
        }
    }

    # Assign any SrReceipt default_common values to the common block if they
    # are not already defined.
    #
    if ( ! $err && 'HASH' eq ref($exConf->{default_common}) )
    {
        my $dfltCom = $exConf->{default_common};
        map {
            $common->{$_} = $dfltCom->{$_}  unless ( defined($common->{$_}) );
        } keys( %{$dfltCom} );
    }

    # Convert ttl string to seconds if supplied.
    if ( ! $err && defined($common->{ttl}) )
    {
        $ret = $self->_convertTTL( $common->{ttl} );
        $err = $ret->Get_error();
        $common->{ttl} = $ret->Get_vals() unless ( $err );
    }

    # Allow the batch ID to be predefined for those processes outside the 
    # automated request generation to create an batch ID that can be used to 
    # refrence the new batch later.
    #
    my $newBatch = 0;
    if ( ! $err && $settings->{batch_state} =~ /$srStates->{predef}/i )
    {
        $settings->{batch_state} = '';
        $newBatch = 1;
    }

    # If the request has a batch ID, check to see if the deliver condition
    # is GO launch.  If not, abort the request.
    my $goAgain = 0;
    $batchId = $settings->{batch_id}  if ( defined($settings->{batch_id}) );
    if ( ! $err && ! $newBatch && $batchId )
    {
        if ( $self->isSrBatch($batchId, SR_DELIVER_GO()) )
        {
            $goAgain = 1;
        }
        else
        {
            $err = sprintf( "Batch %s ABORTED - re-entrant batch is NOT ".
                    "%s for launch.", $batchId, SR_DELIVER_GO() );
        }
    }

    # If sendTime is specified, get the delay Epoch that is adjusted for server
    # time zone, and determine if we still have time to deliver the batch.  If
    # send time is greater than one hour from now, delay the batch by 
    # resubmitting to the SrReceipt queue with the delay.
    #
    # At this point, sendTime is expected to be in UTC time zone.
    #
    my $delayTm = 0;
    my $isDelayed = 0;
    my $sendTm;
    my $dbObj = $self->{dbObj};
    my $sendUTC = (defined($settings->{sendTime}))
        ? $settings->{sendTime} : undef();
    unless ( $err || $goAgain )
    {
        # If send time is empty, fill it in with now() so the Send History
        # will show a date and time.  Using UTC time zone to match the expected
        # time zone of sendTime.
        # 
        unless ( defined($sendUTC) )
        {
            ($sendUTC, $err) = timeToTZ( $dbObj, 'now()', $svrTz, 'UTC' );
            $settings->{sendTime} = $sendUTC;
        }

        # Convert to server time before getting the send epoch because that is
        # the time zone that works with the MQDelay module.
        # 
        unless ( $err )
        {
            ($sendTm, $err) = timeToTZ( $dbObj, $sendUTC, 'UTC', $svrTz );
        }
        
        unless ( $err )
        {
            $ret = $self->_getDelayEpoch( date => $sendTm, zone => $svrTz );
            $err = $ret->Get_error();
            unless ( $err )
            {
                $delayTm = $ret->Get_vals();
                if ( $delayTm > ($startTm + $minDelay) )
                {
                    $isDelayed = 1;
                }
                else
                {
                    $delayTm = 0;
                }
            }
        }

        # - Assign a batch ID to the batch request.
        #   Create a record in Redis to store the batch data, and publish a 
        #   record to the sr_batch_dlr queue that will be used to update 
        #   request_batch_tags when either the request is done or it has timed 
        #   out.
        #
        unless ( $err )
        {
            unless ( defined($batchId) )
            {
                $batchId = createBatchId();
                $settings->{batch_id} = $batchId;
            }
            $ret = $self->_writeRedisBatch( $batchId, $sid, $common, $segNms,
                                {send_utc => $sendUTC, send_server => $sendTm, 
                                 server_tz => $svrTz,
                                 start => $startTm, delay => $delayTm} );
            $err = $ret->Get_error();
        }

        # Publish a delay for the message to come back to the sr_receipt queue
        # when it is time to process.
        #
        if ( ! $err && $isDelayed )
        {
            $ret = SITO::MQDelay->new();
            $err = $ret->Get_error();
            unless ( $err )
            {
                my $mqd = $ret->Get_vals();
                $ret = $mqd->Publish_delayed_request( expire_epoch => $delayTm,
                                target_exchange => $srEx,
                                target_route => $srRecptQ,
                                payload => $msgPkt
                            );
                $err = $ret->Get_error();
            }
        }
    }

    # - Notify the Node server of the verification status.
    if ( $err )
    {
        printf( $logFp "%s Error: %s", $classNm, $err );
        $self->_notifyNode( 'error', $msgPkt, $batchId, $cargo, $err );
    }
    else
    {
        $self->_notifyNode( 'ok', $msgPkt, $batchId, $cargo );
    }

    # - Publish each record in the request list to the appropriate queue.
    #   Phone records go to the phone_p1 queue, segment records to the
    #   segment queue.
    #
    unless ( $err || $isDelayed )
    {
        $ret = $self->publishRequests( $msgPkt, $reqList, $phClass );
        $err = $ret->Get_error();
        unless ( $err )
        {
            $vals = $ret->Get_vals();
            $donePhones = $vals->{done_phones};
            $phReqsts = $vals->{phone_list};
            $doneSegs = $vals->{done_segments};
            $segReqsts = $vals->{segment_list};
        }
        
        # Add phone keys to Redis to store the number of phones delivered, and a
        # phone done list to check for duplations with any segmentation 
        # expansion. Also add an array of phone records to process in case we 
        # decide to optimize phone processing with future development (per 
        # Josh requirements).
        # 
        if ( ! $err && $#{$phReqsts} >= 0 )
        {
            $ret = $self->addRedisRequests( $batchId, $donePhones, $phReqsts, 
                                           $common, 'phone' );
            $err = $ret->Get_error();
        }

        # Add the segment list to Redis so we can have a place to store 
        # segment names with calculation IDs.
        #
        if ( ! $err && $#{$segReqsts} >= 0 )
        {
            $ret = $self->addRedisRequests( $batchId, $doneSegs, $segReqsts, 
                                           $common, 'segment' );
            $err = $ret->Get_error();
        }
    }


    if ( $err )
    {
        # If error occurred, set the error and description in the packet, and
        # continue processing.  The error should be detected in SrNewRequest to
        # abort the processing there after a request record has been generated
        # so we have a place to store the error status and description. 
        #
        my( $errPkt, $descr );
        $errPkt = Copy_struct( $msgPkt );

        if ( 'SITO::Return' eq ref($ret) &&
             $ret->Get_error() eq $err )
        {
            $errPkt->{$sKey}{sito_return}{error} = $ret->Get_error();
            $descr = $ret->Get_descr();
            $descr = to_json( $descr )  if ( ref($descr) );
        }
        else
        {
            $errPkt->{$sKey}{sito_return}{error} = 'MESSAGE REQUEST FAILURE';
            $descr = $err;
        }
        $errPkt->{$sKey}{sito_return}{descr} = $descr;

        $ret = $self->setMessageData( packet => $errPkt ); 
        my $err2 = $ret->Get_error();
        if ( $err2 )
        {
            printf( $logFp "Attempt to pass error from SrReciept to next ".
                    "queue failed: %s", $err2 );
        }

        # PublishNext regardless of error so any error will be stored in the
        # request that is generated next.
        #
        $ret = $self->publishNext();
        $err = $ret->Get_error();
        if ( $err )
        {
            printf( $logFp "SrReceipt publishNext FAILED: %s", $err );
        }
    }

    # Stop the consumer if max_consume is defined and consume_count is over
    # that maximum.
    #
    $self->_stopOnMax( $classNm . ' callback') 
        if ( defined($self->{max_consume}) );

    # Send acknowledge back to consumer to allways remove the current item
    # from the queue
    #
    $outRet->Set_vals( {ack => 1} );

    return( $outRet );
}

#-------------------------------------------------------------------------------
# _notifyNode
#
# Format the request status and publish a notification back to the Node server
# via the appropriate queue.
#
# Input
#   self    : RouterCore object with all the message settings
#   status  : Either 'error' or 'ok'
#   pkt     : Message packet
#   bid     : Batch ID
#   cargo   : Cargo hash
#   errMsg  : Error message when status = error
#
sub _notifyNode
{
    my( $self, $status, $pkt, $bid, $cargo, $errMsg ) = @_;
    my( $err, $ret, $frame, $pktOut, $payload, $mode,$pClass );
    my $logFp = $self->{logFp};
   
    if ( defined($cargo->{api_id}) && defined($cargo->{key}) )
    {
        $pktOut = Copy_struct( $pkt );  # copy to avoid changing caller packet.

        $pktOut->{'X-settings'}{api_id} = $cargo->{api_id};
        $pktOut->{'X-settings'}{api_route} = $cargo->{key};
        $pktOut->{'event'} = $pkt->{'X-settings'}->{'api_r_event'};

    $payload = {};
        if ( $status eq 'ok' )
        {
            $payload->{error} = undef();
            $payload->{descr} = undef();
            $payload->{values} = {status => 'ok', batch_id => $bid};
        }
        else
        {
            $payload->{error} = $errMsg;
            $payload->{descr} = "Request format validation failed: $errMsg";
            $payload->{values} = undef();
        }
    $mode = $pkt->{'X-settings'}->{'api_mode'};
    if($mode eq 'http')
      {
            $frame = {api_id => $cargo->{api_id}};
        $frame->{payload} = to_json($payload);
      }
    elsif($mode eq 'api3')
      {
        my $Pt = {};
            $frame = {};
            $frame->{'session'} = $cargo->{api_id};
            $frame->{'payload'} = $Pt;
        $Pt->{'authentication_token'} = $pkt->{'X-settings'}->{'api3_auth'};
        $Pt->{'event'} = $pkt->{'X-settings'}->{'api3_event'};
        $Pt->{'data'} = $payload;
      }
        $pktOut->{cargo} = $frame;
    printf($logFp "Out: %s\n",Dumper($pktOut)) if ($self->{'debug'});

        $self->setMessageData( packet => $pktOut );

        $pClass = $pkt->{'X-settings'}->{'api_r_class'};
        $ret = $self->publishNotify( class_name => $pClass,
                                     route_key => $cargo->{key} );
        $err = $ret->Get_error();
        printf( $logFp "_notifyNode publishNotify Failed: %s", $err )
            if ( $err );

        # Reset the object message packet to remove notify settings.
        $self->setMessageData( packet => $pkt );
    }
    else
    {
        printf( $logFp "_notifyNode: Skipping Node notification because " .
            "either api_id or key is missing in the message cargo." );
    }
}

#-------------------------------------------------------------------------------
# _writeRedisBatch
#
# Write the message packet to new batch record in Redis.  This is where we store
# the processing counts, common tags, and request list for later processing.
# Each batch will have the following data structures set in Redis:
#
#    Sr_<batch#> = {
#        batch_size = 999,
#        good_count = 999,
#        bad_count = 999,
#        common_tags = <JSON>,
#        deliver_condition = 'go|abort',
#        state = 'processing|delayed:<epoch>|aborted|done',
#        batch_start = <time>,
#    }
#
# Also publish a delayed record to the DLR queue that will facilitate insert
# of a request_batch_tags records for the batch, and will delete the Redis 
# records for the batch once the delivery is done or the record has expired.
#
# Input
#   self    : RouterCore object with all the message settings
#   bid     : Batch ID
#   sid     : System ID
#   common  : Common tags hash
#   segList : Array of segment names to initialize in the batch
#   timeRef : Hash with the following values:
#             { send_utc => $sendUTC   : Send time in UTC
#               send_server => $sendTm : Send time in server time zone
#               server_tz => $zone     : Server time zone 
#               start => $startTm      : Processing start time
#               delay => $delayTm      : Delay epoch time
#             }
#
# Return
#  SITO::Return object with the following methods set:
#    Get_error()  : Return error message if any
#
sub _writeRedisBatch
{
    my( $self, $bid, $sid, $common, $segList, $timeRef ) = @_;
    my( $err, $ret, $rpOb, $cTagsJ, $json, $bRef, $epoch, $mqd, $vals );
    my( $rqstRef, $rRefJ );
    my $dArgs = {};
    my $outRet = SITO::Return->new();
    my $config = $self->{config};
    my $logFp = $self->{logFp};
    my $dcNm = SR_DELIVER_COND_NM();
    my $go = SR_DELIVER_GO();
    my $bKey = sprintf( $self->SR_SUM_KEY_FMT(), $bid  );
    my $sKey = (defined($config->{settings_key}))
        ? $config->{settings_key} : $self->RC_SETTINGS_KEY();
    my $expDt = (defined($common->{expiration})) 
        ? $common->{expiration} : undef();
    my $dlvrDt = (defined($common->{deliverTime})) 
        ? $common->{deliverTime} : undef();
    my $dlvrTz = (defined($common->{deliverTimeZone})) 
        ? $common->{deliverTimeZone} : undef();
    my $ttl = (defined($common->{ttl})) ? $common->{ttl} 
        : ((defined($expDt)) ? undef() : $self->SR_REDIS_TTL());
    my $dlrQnm = (defined($config->{exchange_class}{SrDlr}{queue}))
        ? $config->{exchange_class}{SrDlr}{queue} : undef();
    my $dlrEx = (defined($config->{exchange_class}{SrDlr}{exchange}))
        ? $config->{exchange_class}{SrDlr}{exchange} : undef();
    my $srStates = $self->SR_BATCH_STATES();

    my $stateVal = ($timeRef->{delay} > 0) 
        ? sprintf( "%s:%d", $srStates->{delay}, $timeRef->{delay} ) 
        : $srStates->{process};

    # Need to store the requests list with any segment names so the 
    # GUI can show the segment names to the user for identification of
    # the message batch.
    #
    my $segRef = {};
    if ( 'ARRAY' eq ref($segList) && $#{$segList} >= 0 )
    {
        map {
            # Each segment name gets initialized values to be filled later.
            $segRef->{$_} = {n_users => 0, calc_id => 0};
        } @{$segList};
    }
    $rqstRef = {segment => $segRef};

    if ( 'HASH' eq ref($common) )
    {
        eval {
            $json = SITO::JSON->new();
            $cTagsJ = $json->encode( $common );
            $rRefJ = $json->encode( $rqstRef );
        };
        if ( $@ )
        {
            $err = "JSON encode of common Failed: $@";
        }
    }
    else
    {
        $err = "Missing expected common block in the message cargo.";
    }

    # Add batch record to Redis with zero lengths, and common_tags.
    unless ( $err )
    {
        $bRef = { common_tags => $cTagsJ, batch_size => 0, good_count => 0,
                  bad_count => 0, system_id => $sid, 
                  batch_start => $timeRef->{start},
                  $dcNm => $go, 'state' => $stateVal, requests => $rRefJ,
                  send_time => $timeRef->{send_utc} };


        # The delay_time field is used in goBatch to check if we have time to
        # reset the batch to GO status.
        #
        $bRef->{delay_time} = $timeRef->{delay}  if ( $timeRef->{delay} > 0 );

        $ret = SITO::RedisPoll->new();
        $err = $ret->Get_error();
        unless ( $err )
        {
            $rpOb = $ret->Get_vals();
            $ret = $rpOb->Set_hash( name => $bKey, hash => $bRef );
            $err = $ret->Get_error();
        }
    }

    # Store the initial request_batch_tags db records for the new batch.
    unless ( $err )
    {
        $ret = $self->syncBatchTagsDb( $bid, 'SrReceipt' );
        $err = $ret->Get_error();
    }

    # Determine the Epoch time for the batch expiration by using either ttl from
    # the send time or the expiration time (both on server time zone).
    # If both times are specified, check to make sure the expiration is greater     # than the send time or return error. The delay time is adjusted up to the 
    # nearest hour to reduce the number of delay queues to generate.
    #
    unless ( $err )
    {
        my $dlvrEpoch;
        $dArgs->{date} = $timeRef->{send_server};
        $dArgs->{zone} = $timeRef->{server_tz};
        $ret = $self->_getDelayEpoch( $dArgs );
        $err = $ret->Get_error();
        $dlvrEpoch = $ret->Get_vals() unless ( $err );

        if ( defined($expDt) && ! defined($ttl) )
        {
            $dArgs->{date} = $expDt;
        }
        else
        {
            $expDt = undef();
            $ttl = $self->SR_REDIS_TTL()  unless ( $ttl );
            $dArgs->{time} = (defined($dlvrEpoch)) ? $dlvrEpoch + $ttl
                : time() + $ttl;
        }

        unless ( $err )
        {
            $ret = $self->_getDelayEpoch( $dArgs );
            $err = $ret->Get_error();
            unless ( $err )
            {
                $epoch = $ret->Get_vals();
                if ( defined($expDt) && defined($dlvrEpoch) && 
                     $epoch < $dlvrEpoch )
                {
                    $err = sprintf( "Message expiration date cannot come " .
                            "before the send date (expiration=%s, " .
                            "send_time=%s)", $expDt, $timeRef->{send_server} );
                }
            }
        }
    }

    # Publish a delayed request to the sr_batch_dlr queue for DLR or time out
    # processing.
    #
    if ( ! $err && $epoch )
    {
        if ( $dlrQnm && $dlrEx )
        {
            $ret = SITO::MQDelay->new();
            $err = $ret->Get_error();
            unless ( $err )
            {
                $mqd = $ret->Get_vals();
                $ret = $mqd->Publish_delayed_request( expire_epoch => $epoch,
                                target_exchange => $dlrEx,
                                target_route => $dlrQnm,
                                payload => {batch_id => $bid, system_id => $sid}
                            );
                $err = $ret->Get_error();
            }
        }
        else
        {
            $err = "Missing SrDlr exchange class definition for the ".
                "DLR queue and exchange names."
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
            $outRet->Set_error( "Redis write Failed for batch $bid: $err" );
        }
    }

    return( $outRet );
}

#-------------------------------------------------------------------------------
# _convertTTL
#
# Convert time to live string with 'm' - minutes, 'h' - hours, or 'd' - days
# indicator to seconds.
#
# This may be moved to RouterCore later if we need it in some other module
# besides SrReceipt.
# 
# Input
#   self   : RouterCore object
#   in_ttl : Input TTL string with optional m, h, or d, mark
#
# Return
#   SITO::Return with the following methods set:
#     Get_error()  : Error message if any
#     Get_vals()   : TTL in seconds
#
sub _convertTTL
{
    my( $self, $in_ttl ) = @_;
    my( $err, $out_ttl, $mark, $num );
    my $mult = { s => 1, m => 60, h => 3600, d => 3600*24 };
    my $outRet = SITO::Return->new();
    
    if ( $in_ttl =~ /^(\d+)([a-z])$/i )
    {
        $num = $1;
        $mark = lc( $2 );  
        if ( defined($mult->{$mark}) )
        {
            $out_ttl = $num * $mult->{$mark};
        }
        else
        {
            $err = sprintf( "The input TTL '%s' mark is not supported.", 
                            $mark );
        }
    }
    elsif ( $in_ttl =~ /^\d+$/ )
    {
        $out_ttl = $in_ttl;
    }
    else
    {
        $err = "The input TTL value is not supported.";
    }

    if ( $err )
    {
        $outRet->Set_error( $err );
    }
    else
    {
        $outRet->Set_vals( $out_ttl );
    }

    return( $outRet );
}

#-------------------------------------------------------------------------------
# _getDelayEpoch
#
# Given a date or time, get the Epoch value to use with publishing to the Delay
# queue.  The Epoch value is adjusted per time zone when a date is supplied, and
# it is rounded up to the next hour (or minute if in Debug mode) so as to limit
# the number of Delay queues generated.
#
# Input (named args)
#   self   : RouterCore object
#   time   : Time value to use rather than date
#   date   : Date value to use rather than time
#   zone   : Optional time zone value to use when date is specified
#
# Return
#   SITO::Return with the following methods set:
#     Get_error()  : Error message if any
#     Get_vals()   : Epoch value
#
sub _getDelayEpoch
{
    my $self = shift;
    my $ARGS = ('HASH' eq ref $_[0]) ? shift : { @_ };
    my( $err, $ret, $tArgs, $granMrk, $tzob, $epoch, $vals );
    my $outRet = SITO::Return->new();
    my $dt = (defined($ARGS->{date})) ? $ARGS->{date} : undef();
    my $tm = (defined($ARGS->{time})) ? $ARGS->{time} : undef();
    my $zone = (defined($ARGS->{zone})) ? $ARGS->{zone} : undef();

    $ret = SITO::TZ->new();
    $err = $ret->Get_error();
    unless ( $err )
    {
        # Using one minute granularity for Delay queues per Dev discussion.
        #
        $tArgs = {granularity => '+minute'};
        $tzob = $ret->Get_vals();

        if ( $tm )
        {
            $tArgs->{epoch} = $tm;
        }
        elsif ( $dt )
        {
            $tArgs->{target} = $dt;

            # Validate the time zone if specified.
            #
            if ( defined($zone) )
            {
                $ret = $tzob->Validate_timezone_string( tz => $zone );
                $err = $ret->Get_error();
                $tArgs->{tz} = $zone;
            }
        }
    }

    unless ( $err )
    {
        $ret = $tzob->Map_time( $tArgs ); 
        $err = $ret->Get_error();
        unless ( $err )
        {
            $vals = $ret->Get_vals();
            $epoch = $vals->{epoch};
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
            $outRet->Set_error( $err );
        }
    }
    else
    {
        $outRet->Set_vals( $epoch );
    }

    return( $outRet );
}

# Return true
1;
