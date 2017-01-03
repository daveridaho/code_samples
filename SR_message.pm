################################################################################
# SR_message - SendRequest Message
#
# This package is sourced by MoHandler to define a method for providing a
# mechanism to send a message to a phone number or a user segment via the new
# SendRequest Queue processing that is currently hosted on the ...34.183
# RabbitMQ server to the /ew queues.
#
# Here are the rule parameters this handler expects.
#   'rule_parameters' => {
#        'system_id' => $sid,
#        'template' => $msgNm,
#        'segment' => $segNm,
#        'phone' => $phone,
#        'delay' => $dSecs,
#    }
#     Where,
#        $sid        : System ID of the message to deliver
#        $msgNm      : The name of the sito_global.message to send
#        $segNm      : The name of the user segment to receive the message
#        $phone      : One phone number to receive the message rather than a 
#                      segment.  If this option is specfied, the segment 
#                      argument is ignored.
#        $dSecs      : Number of seconds to delay the message. If not specfied,
#                      the normal sendMessage delay of 5 minutes will be
#                      employed.
#        
# SITO Mobile LTD., Boise, David Runyan, October 2016
################################################################################
package SR_message;

use Exporter;
our @ISA = qw(Exporter);
our @EXPORT = qw(SR_message);

use strict;
use SITO::SrBridge;
use SITO::System;
use SITO::Message;
use SITO::Macro;
use SITO::DB;
use Data::UUID;
use Data::Dumper;
$Data::Dumper::Indent = 1;

sub SR_message
{
    my($self, $packet) = @_;
    my( $err, $ret, $sid, $msgNm, $segNm, $phone, $dSecs, $outRef, $sbOb );
    my( $stngs, $vals, $uuOb, $bid );
    my $data = {};
    my $cntx = {};
    my $apiArgs = {};
    my $transp = 'sms';  # Move this to input if some other transport is needed.
    my $method = 'SR_message';

    # - Get and validate the input rule_parameters variables.
    #
    my $params = $packet->{rule_parameters};
    $sid = (defined($params->{system_id})) 
        ? $params->{system_id} : $packet->{system_id};
    $msgNm = $params->{template};
    $segNm = $params->{segment};
    $phone = $params->{phone};
    $dSecs = (defined($params->{delay})) ? $params->{delay} : 0;

    if ( defined($sid) && $msgNm && ($segNm || $phone) )
    {
        ($data->{common}{message}, $err) = getMsgText( $msgNm, $packet );
        $data->{requests} = ($phone) 
            ? [{transport => [$transp], phone => $phone}] 
            : [{transport => [$transp], segment => $segNm}];

        # Get the database time, and apply the SR Queue server time zone.
        unless ( $err )
        {
            ($data->{common}{deliverTime}, $err) = getDbTime( $dSecs );
            $data->{common}{timezone} = 'America/Boise';
        }

        # Get the system name to use in the common block and API context.
        unless ( $err )
        {
            $cntx->{system_id} = $sid;
            $ret = SITO::System->new();
            $err = $ret->Get_error();
            unless ( $err )
            {
                my $sysOb = $ret->Get_vals();
                $ret = $sysOb->Get_system( system_id => $sid );
                $err = $ret->Get_error();
                unless ( $err )
                {
                    $vals = $ret->Get_vals();
                    $data->{common}{systemName} = $vals->{system_name};
                    $cntx->{system_name} = $vals->{system_name};
                }
            }
        }
    }
    else
    {
        $err = sprintf( "Missing required parameters - " .
                "system_id=%d template=%s segment=%s or phone=%s",
                $sid, $msgNm, $segNm, $phone );
    }

    # Get the batch ID and format the request settings.
    unless ( $err )
    {
        $apiArgs = {data => $data, context => $cntx};

        eval {
            $uuOb = Data::UUID->new();
            $bid = $uuOb->create_str();
        };
        if ( $@ )
        {
            $err = $@;
        }
        else
        {
            $stngs->{batch_id} = $bid;
            $stngs->{batch_state} = 'PREDEFINED'; # copied from Queue::SrUtil

            # These have remained unchanged since Gary was with us.
            $stngs->{api_name} = 'SR';
            $stngs->{api_mode} = 'http';
            $stngs->{api_r_event} = 'SR_RESPONSE';
            $stngs->{api_r_class} = 'NodeApi';
            $stngs->{api_username} = 'gary';

            $apiArgs->{settings} = $stngs;

            # Skip the API response as we won't need a reply from it.
            $apiArgs->{skip_response} = 1;
        }
    }
     
    # Call SrBridge to publish to the SR Start queue.
    unless ( $err )
    {
        $ret = SITO::SrBridge->new( amqp_connection => 'sr_dlr' );
        $err = $ret->Get_error();
        unless ( $err )
        {
            $sbOb = $ret->Get_vals();
            $ret = $sbOb->Post_API_request( $apiArgs );
            $err = $ret->Get_error();
        }
    }

    # Use the long description if error.
    if ( $err && 'SITO::Return' eq ref($ret) )
    {
        my $descr = $ret->Get_descr();
        $err = $descr  if ( length($descr) > length($err) );
    }

    $outRef = ($err) ? {'error' => "$method Error: $err"} : 1;

    return( $outRef );

}

#-------------------------------------------------------------------------------
# getDbTime
#
# Get the database date-time value of now() plus the delay seconds.
#
sub getDbTime
{
    my( $secs ) = @_;
    my( $err, $retTm, $rowPs, $dbObj, $query, $nowStr );

    $dbObj = SITO::DB->new();
    $nowStr = ($secs) ? sprintf( "(now() - interval %d second)", $secs )
                      : 'now()';

    $query = sprintf("SELECT %s AS 'dbTime'", $nowStr);

    eval {
        $rowPs = $dbObj->fetchall_AoH( query => $query );
    };
    if ( $@ )
    {
        $err = "Database selection error - $@";
    }
    else
    {
        $retTm = $rowPs->[0]{dbTime};
    }

    return( $retTm, $err );
}

#-------------------------------------------------------------------------------
# getMsgText 
#
# Get the message text with macro replacements already applied if any match the
# packet values.  Ignore any missing macro errors.
#
sub getMsgText
{
    my( $name, $pkt ) = @_;
    my( $ret, $err, $msgOb, $macOb, $msgRec, $text, $expText );
    my $mArgs = { name => $name, context => 'send_request' };
    map { 
        $mArgs->{$_} = $pkt->{$_}  if ( defined($pkt->{$_}) );
    } (qw(system_id carrier language));

    # Retrieve the message text from the database.
    $ret = SITO::Message->new();
    $err = $ret->Get_error();
    unless ( $err )
    {
        $msgOb = $ret->Get_vals();
        $ret = $msgOb->Get_prompt( $mArgs );
        $msgRec = $ret->Get_vals()  unless ( $err );
    }

    # Apply any macros that exist in the packet.
    $expText = $text = $msgRec->{text};
    if ( ! $err && $text =~ /%/ )
    {
        $ret = SITO::Macro->new();
        $err = $ret->Get_error();
        unless ( $err )
        {
            $macOb = $ret->Get_vals();
            $ret = $macOb->doReplacements({ source => $text, macros => $pkt });
            $err = $ret->Get_error();

            # Ignore macro error, and just return the unexpanded text.
            $expText = ($err) ? $text : $ret->Get_vals();
            $err = undef();
        }
    }

    # Return the longer error description if it exists.
    if ( $err && 'SITO::Return' eq ref($ret) )
    {
        $err = $ret->Get_descr();
    }

    return( $expText, $err );
}

# Return true
1;
