#!/usr/bin/env perl
# save_runpod_template.pl — build a RunPod GraphQL `saveTemplate` payload and
# emit it to stdout. Pipe into curl against https://api.runpod.io/graphql.
#
# Usage:
#   set -o allexport; source .env; source runpod-serverless/.runpod.env; set +o allexport
#   perl runpod-serverless/save_runpod_template.pl x0lzlstt97 \
#     ghcr.io/crypticsciencee/studio-backend:<SHA> > /tmp/saveT.json
#   curl -sS -X POST "https://api.runpod.io/graphql?api_key=${RUNPOD_API_KEY}" \
#     -H "Content-Type: application/json" --data-binary @/tmp/saveT.json
#
# Env vars NOT in the allowlist below get DROPPED from the RunPod template on
# every save. Any feature-flag env the backend reads at cold-boot belongs here.
# Losing one silently degrades the container (e.g. SKELETON_REQUIRE_WAN22 getting
# reset to "1" on RunPod lit up the hosted-fallback banner + blocked skeleton gen).
use strict;
use warnings;
use JSON::PP;

my ($template_id, $image_name) = @ARGV;
die "usage: save_runpod_template.pl <template_id> <image_name>\n"
    unless defined $template_id && defined $image_name;

my @keys = qw(
    APP_DATA_DIR
    SITE_URL
    XAI_API_KEY
    ELEVENLABS_API_KEY
    FAL_AI_KEY
    SUPABASE_URL
    SUPABASE_ANON_KEY
    SUPABASE_JWT_SECRET
    SUPABASE_SERVICE_KEY
    PAYPAL_CLIENT_ID
    PAYPAL_CLIENT_SECRET
    PAYPAL_ENV
    PAYPAL_WEBHOOK_ID
    IMAGE_PROVIDER_ORDER
    RUNPOD_API_KEY
    YOUTUBE_API_KEY
    STUDIO_ERROR_WEBHOOK_URL
    SKELETON_REQUIRE_WAN22
);

my @env = ();
for my $k (@keys) {
    push @env, { key => $k, value => defined $ENV{$k} ? $ENV{$k} : "" };
}

my $payload = {
    query => 'mutation SaveT($input: SaveTemplateInput!) { saveTemplate(input: $input) { id name imageName } }',
    variables => {
        input => {
            id                 => $template_id,
            name               => "studio-backend-latest",
            imageName          => $image_name,
            containerDiskInGb  => 50,
            dockerArgs         => "",
            ports              => "",
            volumeInGb         => 0,
            env                => \@env,
        }
    },
};

print encode_json($payload);
