#!/bin/bash
set -euo pipefail
trap 's=$?; echo "$0: Error on line "$LINENO": $BASH_COMMAND"; exit $s' ERR
IFS=$'\n\t'

echo "gcloud properties:"
gcloud config list
echo "Proceed?"
select yn in "Yes" "No"; do
  case $yn in
  Yes) echo "deploying container..." && break ;;
  No) echo "Aborting deploy" && exit ;;
  esac
done

# Run checks, then  build & deploy image to artifact registry
REPOSITORY="chellstats"
PROJECT=$(gcloud config list --format 'value(core.project)' 2>/dev/null)
TAG="us-east1-docker.pkg.dev/$PROJECT/chellstats/chellstats_data_etl"

sh run_checks.sh
docker-compose build etl
gcloud artifacts repositories create $REPOSITORY --repository-format=docker --location=us-east1
docker tag chellstats_data_etl "$TAG"
docker push "$TAG"
gcloud run deploy chellstats-data-etl  \
    --no-allow-unauthenticated \
    --max-instances=1 \
    --memory=2Gi \
    --image="$TAG" \
    --args=--method=to_bq \
    --ingress=all \
    --timeout=900 \
    --region=us-east1

# To avoid incurring charges, we delete the repository once we are finished deploying
gcloud artifacts repositories delete $REPOSITORY --location=us-east1 --quiet
