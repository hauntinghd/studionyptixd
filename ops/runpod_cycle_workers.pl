#!/usr/bin/env perl
# runpod_cycle_workers.pl — force-terminate warm/running workers on a RunPod
# endpoint so the next invocation cold-boots on the current template image.
#
# Flow: set workersMax=0 + workersStandby=0 (terminates all), wait, then
# restore to the values captured at the top of the run.
#
# Usage:
#   export RUNPOD_API_KEY=...
#   perl ops/runpod_cycle_workers.pl <endpoint_id>
use strict;
use warnings;
use JSON::PP;

my ($endpoint_id) = @ARGV;
die "usage: runpod_cycle_workers.pl <endpoint_id>\n" unless defined $endpoint_id;

my $api_key = $ENV{RUNPOD_API_KEY}
    or die "RUNPOD_API_KEY not set\n";

my $gql_url = "https://api.runpod.io/graphql?api_key=$api_key";

sub gql {
    my ($payload) = @_;
    my $tmp_req  = "$ENV{TEMP}/runpod_cycle_req.json";
    my $tmp_resp = "$ENV{TEMP}/runpod_cycle_resp.json";
    open my $qh, '>', $tmp_req or die "open $tmp_req: $!";
    print $qh encode_json($payload);
    close $qh;
    my $rc = system(
        qq{curl -sS -X POST "$gql_url" -H "Content-Type: application/json" }
        . qq{--data-binary \@"$tmp_req" -o "$tmp_resp"}
    );
    die "curl rc=$rc\n" if $rc != 0;
    open my $rh, '<', $tmp_resp or die "open $tmp_resp: $!";
    my $raw = do { local $/; <$rh> };
    close $rh;
    my $parsed = decode_json($raw);
    die "gql errors: " . encode_json($parsed->{errors}) . "\n"
        if $parsed->{errors};
    return $parsed;
}

# 1. Fetch current endpoint state
my $list = gql({
    query => '{ myself { endpoints { id name templateId workersMin workersMax workersStandby idleTimeout gpuIds scalerType scalerValue networkVolumeId } } }',
});
my ($ep) = grep { $_->{id} eq $endpoint_id }
    @{ $list->{data}{myself}{endpoints} };
die "endpoint $endpoint_id not found\n" unless $ep;

warn sprintf(
    "[cycle] before: id=%s name=%s max=%d standby=%d min=%d\n",
    $ep->{id}, $ep->{name}, $ep->{workersMax}, $ep->{workersStandby}, $ep->{workersMin},
);

# Snapshot the values we need to restore
my %saved = (
    workersMin     => $ep->{workersMin} + 0,
    workersMax     => $ep->{workersMax} + 0,
    workersStandby => $ep->{workersStandby} + 0,
);

sub save_endpoint {
    my (%overrides) = @_;
    # Note: EndpointInput does NOT accept workersStandby — that field is
    # read-only from the query side. Only workersMin / workersMax can be
    # mutated. Setting workersMax=0 terminates ALL running+standby workers.
    my $input = {
        id              => $ep->{id},
        name            => $ep->{name},
        templateId      => $ep->{templateId},
        gpuIds          => $ep->{gpuIds},
        idleTimeout     => $ep->{idleTimeout} + 0,
        scalerType      => $ep->{scalerType},
        scalerValue     => $ep->{scalerValue} + 0,
        networkVolumeId => $ep->{networkVolumeId},
        workersMin      => exists $overrides{workersMin} ? $overrides{workersMin} + 0 : $ep->{workersMin} + 0,
        workersMax      => exists $overrides{workersMax} ? $overrides{workersMax} + 0 : $ep->{workersMax} + 0,
    };
    return gql({
        query => 'mutation SaveE($input: EndpointInput!) { saveEndpoint(input: $input) { id workersMin workersMax } }',
        variables => { input => $input },
    });
}

# 2. Drain to zero (workersMax=0 terminates all running+standby workers)
warn "[cycle] draining: max=0 min=0\n";
my $r1 = save_endpoint(workersMin => 0, workersMax => 0);
warn "[cycle] drained: " . encode_json($r1->{data}{saveEndpoint}) . "\n";

# 3. Wait for workers to actually terminate
my $wait = 25;
warn "[cycle] sleeping ${wait}s for termination...\n";
sleep $wait;

# 4. Restore
warn sprintf("[cycle] restoring: min=%d max=%d\n",
    $saved{workersMin}, $saved{workersMax});
my $r2 = save_endpoint(%saved);
warn "[cycle] restored: " . encode_json($r2->{data}{saveEndpoint}) . "\n";

print "OK — endpoint $endpoint_id cycled. Next render cold-boots on current template image.\n";
