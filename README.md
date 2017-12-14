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

Please see example campaign templates for reference. Column names should be identical
for all campaigns.

- Column names highlighted in yellow should be filled out for every row in the test matrix.
- Column names highlighted in blue only need to be filled out for the first row.

### Campaign information columns
(**single value per campaign**)

  - `campaign_name`: A plain English name for the campaign
  - `campaign_short_name`: A code-friendly name for the campaign. All lowercase, no spaces or
    special characters except underscores. May include a number but not as the first letter
  - `responder_action`: The defining action that differentiates responders from non-responders.
    The action must be taken during the promo period
    May be one of the following values:
    - `reactivated`
    - `activated`
    - `ordered_{1st/2nd/3rd/nth}_box` for any number n indicating the user has received that
      number of boxes since joining Plated
    - `desserts_ordered >= {n}` for any number n indicating the user has ordered at least
      this many desserts during the promo period
    - `total_boxes_ordered >= {n}` for any number n indicating the user has ordered at least
      this many boxes during the promo period
  - `start_date`: the start date for the campaign and promo period
  - `promo_period_end_date`: the end date for the promo period, or `current_date` if ongoing
  - `post_promo_period_end_date`: the end date for the post promo period, or `current_date`
    if ongoing. May be left blank if no post promo period
  - `long_term_end_date`: the end date for the long term read period, or `current_date`
    if ongoing. May be left blank if no long term read period
  - **Campaign KPIs:** `TRUE` if the KPI is desired and relevant to the campaign,
    `FALSE` if it can be omitted from the reporting.
    KPIs are not cumulative--they are computed over the specified period only.
    - `cancelations`: the number of users canceling at any point during the period (regardless
      of subsequent reactivation)
    - `cancelation_rate`: the percentage of users canceling at any point out of all users in this
      responder segment
    - `new_activations`: the number of prospects activating at any point during the period
      (regardless of subsequent cancelation)
    - `activation_rate`: the percentage of users activating at any point out of all prospects in this
      responder segment
    - `reactivations`: the number of users reactivating at any point during the period (regardless
      of subsequent cancelation)
    - `reactivation_rate`: the percentage of users reactivating at any point out of all users in this
      responder segment
    - `total_boxes_ordered`: the total number of boxes ordered during the period
    - `avg_boxes_ordered`: the average number of boxes ordered during the period across all users
      in this responder segment
    - `gov`: the total gross order value of all boxes ordered during the period
    - `aov`: the average order value across all boxes ordered during the period
    - `desserts_ordered`: the total number of boxes ordered containing at least 1 dessert
    - `dessert_take_rate`: the percent of boxes ordered containing at least 1 dessert
    - `ordered_nth_box`: [*VALUE SHOULD BE FALSE, OR A NUMBER, OR LIST OF NUMBERS*] the total number
      of users ordering their nth box during the period (`n` must be specified
      in the template--for multiple values of `n`, list all separated by commas)
    - `redeemed_offer_discount`: number of users who redeemed the offer during the period
    - `pct_redeemed`: percent of users who redeemed the offer in this responder segment
    - `total_active_at_end`: total number of users still active at end of period
    - `pct_active_at_end`: percent of users still active at end of period

### Test matrix information columns
(**single value per test matrix row**)

  - `test_group`: Test or Control
  - `segment_group`: any segment label desired, or blank if not needed
  - `offer_group`: a description of the offer sent, or no offer
  - `target_name`: target name that uniquely identifies this row in the test
    matrix. Must correspond exactly to the first part of the filename for all
    send list files in the campaign
  - `creative_template_name`: name of creative template sent to users, or blank
    if no message sent
  - `population_name`: prospect/user population lists are pulled from
  - `offer_campaign_name`: value of `Name` column in Admin/Offer Campaigns (for
    offer campaigns). Blank if mass discount or no offer sent
  - `discount_name`: value of `Discount Name` column in Admin/Offer Campaigns
    (*for offer campaigns*) or value of `Name` column in Admin/Discounts (*for
    mass discounts*). Blank if no offer sent
  - `message_offer`: message or offer description sent
