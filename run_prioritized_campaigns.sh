#!/bin/bash

declare -a campaigns=(
<<<<<<< HEAD
    "December Gift Card Purchases"
    "Destination Dinner New Customer Offer"
    "Destination Dinner Reactivation"
    "January Reactivation Offer Test"
    "July Reactivation Email + DM"
    "New Customer Phased Offer Email"
=======
>>>>>>> 5c8ff22dc686692cb028ebe56e0b74eed0382d69
    "Unskip 1752"
    "Unskip 1801"
)
declare today=`date +%m_%d_%y`

for campaign in "${campaigns[@]}"
do
      # upload to redshift
      python process_campaign/upload_redshift.py "$campaign" \
      2>"error_logs/upload_${campaign}_${today}.err" \
      1>"output_logs/upload_${campaign}_${today}.out"

      # generate metrics
      python process_campaign/generate_sql_query.py "$campaign" \
      2>"error_logs/metrics_${campaign}_${today}.err" \
      1>"output_logs/metrics_${campaign}_${today}.out"
done
