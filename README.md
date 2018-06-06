# Campaign Reporting Automation

## Campaign lists
There should be a folder for each campaign, named with the campaign name.
This folder should contain an Excel file (ending in `template.xlsx`),
a `Test` subfolder, and a `Control` subfolder. All lists in the `Test` and
`Control` folders should be named according to their `target_name`.

- Folder: `campaign_name`
  Template file: `*template.xlsx`
  - Subfolder: `test_group` (`Test` or `Control`)
    - `target_name*.{csv/xls/xlsx}`

Full path to list: `campaign_name/test_group/target_name*.{csv/xls/xlsx}`

The filename must begin with the `target_name` as listed in the `template` file.
You can include additional information after the target name. Every `target_name`
should correspond to one and only one row of the test matrix for that campaign.

If multiple files begin with the same `target_name`, their lists will be combined.

The lists must be in either:
  - `.csv` format (with comma-separation, not tab separation)
  - `.xls/.xlsx` format

### Campaign list columns
The campaign lists must feature at least one of the following `user_identifier` columns:
- `user_id`
- `internal_user_id`
- `external_id`
- `prospect_id`
- `email`

Additional columns can be included in the campaign lists but will be ignored.
All campaign lists for the same campaign should feature the same user identifier.

## Campaign template

Please see example campaign templates for reference. Column names should be identical for all campaigns.

- Column names highlighted in yellow should be filled out for every row in the test matrix.
- Column names highlighted in blue only need to be filled out for the first row.

### Campaign information columns
(**single value per campaign**)

  - `campaign_name`: A plain English name for the campaign
  - `campaign_short_name`: A code-friendly name for the campaign. All lowercase, no spaces or special characters except underscores. May include a number but not as the first letter
  - `responder_action`: The defining action that differentiates responders from non-responders. The action must be taken during the promo period. May be one of the following values:

    - `reactivated`
    - `activated`
    - `ordered_{1st/2nd/3rd/nth}_box` for any number *n* indicating the user has received their nth order during the promo period
    - `desserts_ordered >= {n}` for any number *n* indicating the user has ordered at least this many desserts during the promo period
    - `total_boxes_ordered >= {n}` for any number *n* indicating the user has ordered at least this many boxes during the promo period
    - `upgraded` increased the number of nights, number of servings, or both in their base subscription plan
    - `offer_redeemed`
    - `ordered_ds_{n}` ordered a box during the delivery schedule named *n* during the promo period
    - `gift_card_purchase`
    - `ordered_week_4` ordered a box during the 4th week of their subscription (from 1st delivery)
    - `num_boxes_first_4_weeks >= {}` for any number *n* indicating the user has ordered at least this many boxes during the first four weeks of their subscription (from 1st delivery)
    - `used_the_app` used the iOS or Android app at least once during the promo period
    - `sent_referral` sent at least 1 referral during the promo period

  - `start_date`: the start date for the campaign and promo period
  - `promo_period_end_date`: the end date for the promo period, or `current_date` if still ongoing
  - `post_promo_period_end_date`: the end date for the post promo period, or `current_date` if still ongoing. May be left blank if no post promo period
  - `long_term_end_date`: the end date for the long term read period, or `current_date` if still ongoing. May be left blank if no long term read period

  - **Campaign KPIs:** `TRUE` if the KPI is desired and relevant to the campaign,
    `FALSE` if it can be omitted from the reporting.
    KPIs are cumulative--they are computed from the campaign start date to the end of the specified period.
    - `cancelations`: the percentage of users in this responder segment canceling at any point during the period (regardless of subsequent subscription changes)
    - `cancelation_rate`: the percentage of users in this responder segment canceling at any point during the period (regardless of subsequent subscription changes)
    - `new_activations`: the number of prospects in this responder segment activating at any point during the period (regardless of subsequent subscription changes)
    - `activation_rate`: the percentage of prospects in this responder segment activating at any point during the period (regardless of subsequent subscription changes)
    - `reactivations`: the number of users in this responder segment reactivating at any point during the period (regardless of subsequent subscription changes)
    - `reactivation_rate`: the percentage of users in this responder segment reactivating at any point during the period (regardless of subsequent subscription changes)
    - `total_boxes_ordered`: the number of boxes ordered by users in this responder segment during the period
    - `avg_boxes_ordered`: the average number of boxes ordered by users in this responder segment during the period
    - `gov`: the total gross order value of all boxes ordered by users in this responder segment during the period
    - `aov`: the average order value across all boxes ordered by users in this responder segment during the period
    - `desserts_ordered`: the number of boxes ordered by users in this responder segment containing at least 1 dessert item
    - `dessert_take_rate`: the percent of boxes ordered by users in this responder segment containing at least 1 dessert item
    - `ordered_nth_box`: [*VALUE SHOULD BE FALSE, OR A NUMBER, OR LIST OF NUMBERS*] the number of users in this responder segment ordering their nth box during the period (`n` must be specified in the template--for multiple values of `n`, list all values separated by commas)
    - `redeemed_offer_discount`: number of users in this responder segment who redeemed the offer or discount before it expired
    - `pct_redeemed`: percent of users in this responder segment who redeemed the offer or discount
    - `total_active_at_end`: number of users in this responder segment with an active subscription at the end of this period
    - `pct_active_at_end`: percent of users in this responder segment with an active subscription at end of this period
    - `total_upgrades`: the total number of users who upgraded their base plan (people, nights, or both) at any point during the period (regardless of subsequent base plan changes)
    - `total_downgrades`: the total number of users who downgraded their base plan (people, nights, or both) at any point during the period (regardless of subsequent base plan changes)
    - `ordered_ds`: [*VALUE SHOULD BE FALSE, OR THE NAME OF A DS*] the number of users in this responder segment ordering a box during the specified delivery schedule (delivery schedule must fall within the given period)
    - `gift_cards_purchased`: the total number of users in this responder segment purchasing at least one gift card during this period
    - `total_ordered_week_4`: the total number of users who placed an order during the 4th week of their subscription (relative to first delivery)
    - `pct_ordered_week_4`: the percent of users who placed an order during the 4th week of their subscription (note that this percentage is out of all users in this segment)
    - `avg_num_boxes_first_4_weeks`: the average number of boxes ordered by users in the first 4 weeks of their subscription (relative to first delivery)
    - `total_using_the_app`: the total number of users who used either the iOS or Android app at least once during the specified period
    - `num_referrals_sent`: the total number of referrals sent during the specified period, whether or not they were redeemed

### Test matrix information columns
(**single value per test matrix row**)

  - `test_group`: Test or Control
  - `segment_group`: any segment label desired, or blank if not needed
  - `offer_group`: a description of the offer sent, or no offer
  - `target_name`: target name that uniquely identifies this row in the test matrix. Must correspond exactly to the first part of the filename for all send list files in the campaign
  - `creative_template_name`: name of creative template sent to users, or blank if no message sent
  - `population_name`: prospect/user population lists are pulled from
  - `offer_campaign_name`: value of `Name` column in Admin/Offer Campaigns (for offer campaigns). Blank if mass discount or no offer sent
  - `discount_name`: value of `Discount Name` column in Admin/Offer Campaigns (*for offer campaigns*) or value of `Name` column in Admin/Discounts (*for mass discounts*). Blank if no offer sent
  - `message_offer`: message or offer description sent

## Formatting requirement reminders
- Please save the template file in Excel format (`.xlsx`) and not `.csv`
- Every name in target_name should match exactly to the filename of the corresponding send list file, without the csv part. If a particular target group happens to be split into multiple files, the target name should match exactly to the beginning of the filename
- Excel tends to incorrectly round/reformat `external_user_id`. Please remove any external_user_id columns from the send lists, and just use email
- If you want the data pulled up to today, please use `current_date`, not `current_day` or any other expression
- `ordered_box` is not a valid responder metric, use `total_boxes_ordered > 0` (or greater than `>`/ greater than or equal to `>=` however many number of boxes)
- `ordered_nth_box` should either be `FALSE`, or the number box/boxes you are interested in separated by commas (for example `5,10`)
- ordered_ds should either be `FALSE`, or the name of the DS you are interested in (for example `1815`)
- Please confirm we are using an up-to-date template, meaning that the metric columns go up to `num_referrals_sent` in the template
