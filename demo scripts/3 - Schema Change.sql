/* ============    SCHEMA CHANGE... ADD "NEW_COLUMN"    ============ */ 
USE ROLE GUIDEWIRE_ADMIN;
USE WAREHOUSE GW_CDA_LOAD_WH;

-- ADD the new column to the table, and REPLACE the pipe to include the new column
-- ... Both of these can be done before the new column is added to the incremental loads
SELECT * FROM GUIDEWIRE_CDA.RAW.BC_CHARGEPT_NM_L10N;


ALTER TABLE "GUIDEWIRE_CDA"."RAW"."BC_CHARGEPT_NM_L10N" ADD COLUMN NEW_COLUMN NUMBER;
/* The Migration Proc DOES work as well, but is reactive rather than proactive */
-- call common.public.migrate_gw_table_def('GUIDEWIRE_CDA','RAW','BC_CHARGEPT_NM_L10N');

CREATE OR REPLACE PIPE GUIDEWIRE_CDA.LANDING.bc_chargept_nm_l10n
AUTO_INGEST=TRUE 
AS
  copy into "GUIDEWIRE_CDA"."RAW"."BC_CHARGEPT_NM_L10N"
  from (
  SELECT 
        CURRENT_TIMESTAMP::TIMESTAMP               AS load_ts
        ,'INCREMENTAL'                             AS load_type
        ,$1:beanversion::NUMBER                    AS beanversion 
        ,$1:gwcbi___connector_ts_ms::NUMBER        AS gwcbi___connector_ts_ms
        ,$1:gwcbi___lsn::NUMBER                    AS gwcbi___lsn
        ,$1:gwcbi___operation::NUMBER              AS gwcbi___operation
        ,$1:gwcbi___payload_ts_ms::NUMBER          AS gwcbi___payload_ts_ms
        ,$1:gwcbi___seqval_hex::STRING             AS gwcbi___seqval_hex
        ,$1:gwcbi___tx_id::NUMBER                  AS gwcbi___tx_id
        ,$1:id::NUMBER                             AS id
        ,$1:language::NUMBER                       AS language
        ,$1:owner::NUMBER                          AS owner
        ,$1:publicid::STRING                       AS publicid
        ,$1:value::STRING                          AS value
        ,$1:new_column::NUMBER                     AS new_column
   FROM @GUIDEWIRE_CDA.LANDING.s3_extstg_guidewire/bc_chargept_nm_l10n (
      file_format => COMMON.PUBLIC.FILE_FORMAT_PARQUET_DEFAULT, 
      PATTERN => '.*.parquet'
    )
  );

/* --- THAT'S IT. THE SCHEMA CHANGE IS COMPLETE. THE REST OF THIS SCRIPT IS TESTING/DEMONSTRATION --- */
/* ================================================================================================== */


-- SIMULATE A NEW FINGERPRINT BEING ADDED WITH A SCHEMA THAT INCLUDES THE NEW COLUMN
COPY INTO @GUIDEWIRE_CDA.LANDING.s3_extstg_guidewire/bc_chargept_nm_l10n/4fdc0fa344f7452a8a82f05b5c7cab28/2612480304263/ADDED_COLUMN_
FROM (
    -- Double-quotes maintain case, otherwise columns are upper-case
    SELECT DISTINCT
        BEANVERSION  AS "beanversion"
        ,gwcbi___connector_ts_ms AS "gwcbi___connector_ts_ms"
        ,gwcbi___lsn AS "gwcbi___lsn"
        ,gwcbi___operation AS "gwcbi___operation"
        ,gwcbi___payload_ts_ms AS "gwcbi___payload_ts_ms"
        ,gwcbi___seqval_hex AS "gwcbi___seqval_hex"
        ,gwcbi___tx_id AS "gwcbi___tx_id"
        ,id+100 AS "id"
        ,language AS "language"
        ,owner AS "owner"
        ,publicid AS "publicid"
        ,VALUE AS "value" 
        ,999 AS "new_column"
    FROM GUIDEWIRE_CDA.RAW.BC_CHARGEPT_NM_L10N
)
HEADER = TRUE 
OVERWRITE = TRUE;

-- SEE THE NEW DATA BEING QUEUED UP INTO THE PIPE
SELECT SYSTEM$PIPE_STATUS( 'GUIDEWIRE_CDA.LANDING.bc_chargept_nm_l10n' );
-- NOTICE THAT EXISTING DATA HAS A NULL, WHILE NEW DATA HAS THE ADDED COLUMN VALUE.
SELECT * FROM GUIDEWIRE_CDA.RAW.BC_CHARGEPT_NM_L10N;
