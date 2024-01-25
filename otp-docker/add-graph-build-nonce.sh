#!/bin/sh
apk add jq aws-cli curl
export nonce=$(jq -r .nonce /var/opentripplanner/otp-runner-graph-build-manifest.json)
touch /etc/caddy/static/status-new.json
cat /etc/caddy/static/status.json | jq --arg n $nonce '.nonce = $n' > /etc/caddy/static/status-new.json
rm /etc/caddy/static/status.json
mv /etc/caddy/static/status-new.json /etc/caddy/static/status.json

# Download files
jq -r ".baseFolderDownloads[] | .uri" /var/opentripplanner/otp-runner-graph-build-manifest.json | while read uri; do aws s3 cp $uri /var/opentripplanner/ || true; done
jq -r ".baseFolderDownloads[] | .uri" /var/opentripplanner/otp-runner-graph-build-manifest.json | while read uri; do curl "$uri" -O /var/opentripplanner/ || true; done
find ./ -type f -name '*.zip' | xargs -I '{}' mv '{}' '{}'.GTFS.zip
