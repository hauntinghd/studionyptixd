#!/usr/bin/env perl
# runpod_bump_image.pl — change the image tag on a RunPod template WITHOUT
# dropping any env vars. Reads the current template over RunPod GraphQL,
# then re-saves it identically with only imageName changed.
#
# Why this exists: the legacy runpod-serverless/save_runpod_template.pl
# rebuilds `env` from a local allowlist, so every bump silently dropped any
# env var not in that allowlist (e.g. GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET
# which live only on RunPod). This script reads-then-writes the exact env
# back, so nothing is lost.
#
# Usage:
#   export RUNPOD_API_KEY=...   # or: set -o allexport; source .env; set +o allexport
#   perl ops/runpod_bump_image.pl <template_id> <image_name>
#
# Example:
#   perl ops/runpod_bump_image.pl x0lzlstt97 \
#     ghcr.io/crypticsciencee/studio-backend:ecec2beb118bb734e30fe9a0e7d15f9377a6199e
use strict;
use warnings;
use JSON::PP;

my ($template_id, $image_name) = @ARGV;
die "usage: runpod_bump_image.pl <template_id> <image_name>\n"
    unless defined $template_id && defined $image_name;

my $api_key = $ENV{RUNPOD_API_KEY}
    or die "RUNPOD_API_KEY not set (source .env before running)\n";

# --- 1. Fetch current template via podTemplates (myself scope) ---
my $query_payload = encode_json({
    query => '{ myself { podTemplates { id name imageName containerDiskInGb dockerArgs ports volumeInGb env { key value } } } }',
});

# Write query to a temp file and curl it. Using system() + redirection
# avoids perl IPC complications on Windows.
my $tmp_query = "$ENV{TEMP}/runpod_bump_query.json";
my $tmp_resp  = "$ENV{TEMP}/runpod_bump_resp.json";
open my $qh, '>', $tmp_query or die "open $tmp_query: $!";
print $qh $query_payload;
close $qh;

my $gql_url = "https://api.runpod.io/graphql?api_key=$api_key";
my $curl_rc = system(
    qq{curl -sS -X POST "$gql_url" -H "Content-Type: application/json" }
    . qq{--data-binary \@"$tmp_query" -o "$tmp_resp"}
);
die "curl fetch failed (rc=$curl_rc)\n" if $curl_rc != 0;

open my $rh, '<', $tmp_resp or die "open $tmp_resp: $!";
my $resp_raw = do { local $/; <$rh> };
close $rh;
my $resp = decode_json($resp_raw);

my $tpls = $resp->{data}{myself}{podTemplates}
    or die "no podTemplates in response: $resp_raw\n";

my ($tpl) = grep { $_->{id} eq $template_id } @$tpls;
die "template id '$template_id' not found in account\n" unless $tpl;

warn sprintf(
    "[runpod_bump_image] fetched template: id=%s name=%s oldImage=%s envKeys=%d\n",
    $tpl->{id}, $tpl->{name}, $tpl->{imageName}, scalar @{$tpl->{env} || []},
);

# --- 2. Build saveTemplate mutation with identical env + new image ---
my $env_copy = [ map { { key => $_->{key}, value => $_->{value} } } @{$tpl->{env} || []} ];

my $save_payload = {
    query => 'mutation SaveT($input: SaveTemplateInput!) { saveTemplate(input: $input) { id name imageName } }',
    variables => {
        input => {
            id                => $tpl->{id},
            name              => $tpl->{name},
            imageName         => $image_name,
            containerDiskInGb => $tpl->{containerDiskInGb} + 0,
            dockerArgs        => $tpl->{dockerArgs} // "",
            ports             => $tpl->{ports} // "",
            volumeInGb        => $tpl->{volumeInGb} + 0,
            env               => $env_copy,
        }
    },
};

my $tmp_save = "$ENV{TEMP}/runpod_bump_save.json";
open my $sh, '>', $tmp_save or die "open $tmp_save: $!";
print $sh encode_json($save_payload);
close $sh;

my $save_resp_file = "$ENV{TEMP}/runpod_bump_save_resp.json";
my $save_rc = system(
    qq{curl -sS -X POST "$gql_url" -H "Content-Type: application/json" }
    . qq{--data-binary \@"$tmp_save" -o "$save_resp_file"}
);
die "curl save failed (rc=$save_rc)\n" if $save_rc != 0;

open my $srh, '<', $save_resp_file or die "open $save_resp_file: $!";
my $save_resp_raw = do { local $/; <$srh> };
close $srh;
my $save_resp = decode_json($save_resp_raw);

if ($save_resp->{errors}) {
    die "saveTemplate errors: " . encode_json($save_resp->{errors}) . "\n";
}
my $saved = $save_resp->{data}{saveTemplate}
    or die "no saveTemplate in response: $save_resp_raw\n";

print "OK — template bumped\n";
printf "  id:    %s\n", $saved->{id};
printf "  name:  %s\n", $saved->{name};
printf "  image: %s\n", $saved->{imageName};
printf "  env vars preserved: %d\n", scalar @$env_copy;
