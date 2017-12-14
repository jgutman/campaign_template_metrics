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
