# Loadshedding Region and Stage Query
TODO

## Variable Naming Convention
This project handles loadshedding queries using AWS backend, therefore variable names may be ambiguous if care is not
taken to follow a consistent variable naming scheme. This scheme is also used for filenames and other identifying
string wherever possible or required.

| suffix | description |
|-|-|
| region | [AWS Region](https://aws.amazon.com/about-aws/global-infrastructure/regions_az/) that describes where data is stored, function are executed and other computing resources |
| *_loadshedding | Prefix used to identify loadshedding specific variables |
| region_loadshedding | Municipality that is responsible for a specific geographic region consisting of multiple areas |
| area / area_loadshedding | Area within a `region_loadshedding`. Smallest geographic area region is handled as a single entity during loadshedding |
| schedule_stage | Planned loadshedding stages during specific time intervals. Refers to the schedule for a specific `region_loadshedding` or the nation schedule (`region_loadshedding=='national'`)
