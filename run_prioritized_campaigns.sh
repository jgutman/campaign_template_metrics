#!/bin/bash

declare -a campaigns=(
    "Destination Dinner New Customer Offer"
    "Destination Dinner Reactivation"
    "July Reactivation Email + DM"
    "New Customer Phased Offer Email"
)
declare today=`date +%m_%d_%y`

for campaign in "${campaigns[@]}"
do
      # upload to redshift
      python process_campaign/upload_redshift.py "$campaign" \
      2> error_logs/upload_${campaign}_${today}.err \
      1> output_logs/upload_${campaign}_${today}.out
      # generate metrics
      python process_campaign/generate_sql_query.py "$campaign" \
      2> error_logs/metrics_${campaign}_${today}.err \
      1> output_logs/metrics_${campaign}_${today}.out
done
