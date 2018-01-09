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
    - `ordered_{1st/2nd/3rd/nth}_box` for any number n indicating the user has received their nth order during the promo period
    - `desserts_ordered >= {n}` for any number n indicating the user has ordered at least this many desserts during the promo period
    - `total_boxes_ordered >= {n}` for any number n indicating the user has ordered at least this many boxes during the promo period
    - `upgraded`
    - `offer_redeemed`
    - `ordered_ds_{n}` ordered a box during the delivery schedule named n during the promo period
    - `gift_card_purchase`
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

## Reporting metrics
