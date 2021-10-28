![N|Solid](https://upload.wikimedia.org/wikipedia/commons/f/ff/Snowflake_Logo.svg)

***
# GuideWire-CDA

## Purpose
- **To Demonstrate a best practice method to ingest data from GuideWire CDA**

## How to Use This Repo

***
### Prerequisites

1. **A Snowflake account with accountadmin access**
2. **Access to an AWS Account**
> - Including permission to create buckets and IAM Users

***
### Getting Started
#### 1. AWS Setup
*At time of writing, Guidewire only provides CDA on AWS s3*
- Create an s3 bucket in the same region as your Snowflake Account
- Add a directory called "guidewire_cda" at the root of your bucket
- Drop the data from '/data/0 - historical load/' into s3
- Follow the [steps to create a storage integration to access your s3 bucket](https://docs.snowflake.com/en/user-guide/data-load-s3-config-storage-integration.html)

#### 2. One-Time Setup
- Update all parameters in `/demo scripts/0 - Setup-Reset.sql` with values appropriate for your setup.
- In your snowflake account, as a user with accountadmin, run `0 - Setup-Reset.sql`

#### 3. Test Demo Scripts
- Before getting into the demo flow, check that scripts 1 through 4 (in order) execute without error in your snowflake account
- Once scripts 1 through 4 have completed successfully, Re-run `0 - OneTime Setup.sql`

#### 4. Build Out SnowSight Dashboards (Optional)
- Leveraging an existing pre-configured account, copy the dashboard queries, chart configurations and dashboard layout. For now this is a manual process. There is no way to share or export/import dashboards.

***
### Demo Delivery
[Example Demo Deck](https://docs.google.com/presentation/d/1pFirdeOkxP_hfCMvueflLGOqQ1IC42-jQcSPdNbeXuA/edit?usp=sharing)

#### 1 - Bulk Historical Load
1. Show the initial historical data in s3 using the AWS Console
2. Then step through the Bulk Historical Load script
#### 2 - Incremental Auto-Load (Snowpipe)
1. Step through the Incremental Auto-Load Script
2. Then upload the data in `/data/1 - incremental load` to your bucket (do this on-screen, visible to audience)
3. Return to snowflake and explain the SYSTEM$PIPE_STATUS function, which running it and highlighting changes in 'pendingFileCount'
4. Once pendingFileCount returns to 0, run a select * and show that the rows have doubled, where half of them now have 'LOAD_TYPE'='INCREMENTAL'
#### 3 - Schema Change
1. Explain that schema changes are not automatic, but as long as GW notifies ahead of time, it takes only two commands (ALTER TABLE ADD COLUMN and REPLACE PIPE).
> This is because GW uses a non-destructive, additive-only approach. The two commands can be run any time ahead of the impending schema change.
2. Step through the Schema Change script up to and including the CREATE OR REPLACE PIPE statement.
3. Explain that the pipe and table are now ready whenever the schema change actually takes place.
4. Run the rest of the script to simulate data landing in a new 'fingerprint' folder with an updated schema (added column 'new_column')
5. Switch over to s3 to show that snowflake has unloaded data into a new fingerprint folder
6. Switch back to snowflake and show the Pipe Status while the data is loaded, then show the loaded data in the table.
7. Run `4 - Load bc_taccountpattern.sql`. No explanation required, mirrors the process already demo'd.
8. (Optional) Show statistical dashboard in Snowsight.
#### 4 - Automated Pipeline
This vignette in the demo builds out a typical Snowflake stream + task pipeline on top of the snowpipe built already.
It can be demo'd however is convenient, and comments in the code are fairly descriptive to explain each query being executed.
