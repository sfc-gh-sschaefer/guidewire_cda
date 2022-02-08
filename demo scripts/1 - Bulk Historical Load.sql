/* ============    BULK LOAD INITIAL HISTORICAL DATA    ============ */
USE ROLE GUIDEWIRE_ADMIN;
USE WAREHOUSE GW_CDA_LOAD_WH;

-- Inspect the schema of the external table using schema inference
  select *
    from table(
      INFER_SCHEMA(
      LOCATION=>'@GUIDEWIRE_CDA.RAW.gw_bc_chargept_nm_l10n_extstg/'
      , FILE_FORMAT=>'GUIDEWIRE_CDA.PUBLIC.FILE_FORMAT_PARQUET_DEFAULT'
        )
      )order by 1;

-- CREATE A TARGET TABLE TO LOAD THE RAW CDC DATA IN A STRUCTURED FORMAT 
  call guidewire_cda.public.migrate_s3_table('GUIDEWIRE_CDA.RAW.gw_bc_chargept_nm_l10n_extstg', '', 'parquet', 'GUIDEWIRE_CDA','RAW','BC_CHARGEPT_NM_L10N');

-- SIMPLE SELECT and INSERT TO LOAD (Can be built in to a scheduled task or executed in a pipeline)
  call guidewire_cda.public.bulk_load('GUIDEWIRE_CDA.RAW.gw_bc_chargept_nm_l10n_extstg', '', 'parquet', 'GUIDEWIRE_CDA','RAW','BC_CHARGEPT_NM_L10N');
-- Preview Raw table in Object Browser


-- Generally we would want to create a pipe to handle incremental loads immediately after performing the initial bulk load...
-- This is what that pipe would look like.

CREATE OR REPLACE PIPE GUIDEWIRE_CDA.RAW.bc_chargept_nm_l10n
AUTO_INGEST=TRUE 
AWS_SNS_TOPIC = 'arn:aws:sns:us-west-2:484577546576:sfc-mwies-bc_chargept_nm_l10n'
AS
  copy into "GUIDEWIRE_CDA"."RAW"."BC_CHARGEPT_NM_L10N"
  from '@GUIDEWIRE_CDA.RAW.gw_bc_chargept_nm_l10n_extstg'
      FILE_FORMAT = (TYPE = 'PARQUET'),
      MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
      PATTERN = '.*.parquet'
;

-- Show pipes to get the ARN for the s3 notifications
---- Use the "notification_channel" SQS ARN to set up an s3 Event Notification (may require guidewire admin)
    DESCRIBE PIPE GUIDEWIRE_CDA.RAW.bc_chargept_nm_l10n;

-- 
SELECT SYSTEM$PIPE_STATUS( 'GUIDEWIRE_CDA.RAW.bc_chargept_nm_l10n' );

/* ===========================================================
/* =========   REPEAT FOR TACCOUNTPATTERN TABLE   ========= */

-- CREATE A TARGET TABLE TO LOAD THE RAW CDC DATA IN A STRUCTURED FORMAT 
  call guidewire_cda.public.migrate_s3_table('GUIDEWIRE_CDA.RAW.gw_bc_taccountpattern_extstg', '', 'parquet', 'GUIDEWIRE_CDA','RAW','BC_TACCOUNTPATTERN');

-- SIMPLE SELECT and INSERT TO LOAD (Can be built in to a scheduled task or executed in a pipeline)
  call guidewire_cda.public.bulk_load('GUIDEWIRE_CDA.RAW.gw_bc_taccountpattern_extstg', '', 'parquet', 'GUIDEWIRE_CDA','RAW','BC_TACCOUNTPATTERN');

CREATE OR REPLACE PIPE GUIDEWIRE_CDA.RAW.bc_taccountpattern
AUTO_INGEST=TRUE 
AWS_SNS_TOPIC = 'arn:aws:sns:us-west-2:484577546576:sfc-mwies-taccountpattern'
AS
  copy into "GUIDEWIRE_CDA"."RAW"."BC_TACCOUNTPATTERN"
  from '@GUIDEWIRE_CDA.RAW.gw_bc_taccountpattern_extstg'
      FILE_FORMAT = (TYPE = 'PARQUET'),
      MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
      PATTERN = '.*.parquet'
;
-- Show pipes to get the ARN for the s3 notifications
---- Use the "notification_channel" SQS ARN to set up an s3 Event Notification (may require guidewire admin)
    DESCRIBE PIPE GUIDEWIRE_CDA.LANDING.TACCOUNTPATTERN;

-- 
SELECT SYSTEM$PIPE_STATUS( 'GUIDEWIRE_CDA.LANDING.TACCOUNTPATTERN' );