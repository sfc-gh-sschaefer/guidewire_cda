/* ================      SCHEMA CHANGE... ADD "NEW_COLUMN"      ================= */ 
-- Use the "migrate table" proc to update the table when there is a new field, and REPLACE the pipe
USE ROLE GUIDEWIRE_ADMIN;
USE WAREHOUSE GW_CDA_LOAD_WH;

-- Suspend the pipe to pause loading until the "new_column" has been migrated
ALTER PIPE GUIDEWIRE_CDA.RAW.bc_chargept_nm_l10n SET PIPE_EXECUTION_PAUSED = TRUE;

-- SIMLATE A NEW FINGERPRINT BEING ADDED WITH A SCHEMA THAT INCLUDES THE NEW COLUMN
COPY INTO @GUIDEWIRE_CDA.RAW.gw_bc_chargept_nm_l10n_extstg/4fdc0fa344f7452a8a82f05b5c7cab28/2612480304264/ADDED_COLUMN_1
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

-- Use the migrate table proc to capture the new column
    call guidewire_cda.public.migrate_s3_table('GUIDEWIRE_CDA.RAW.gw_bc_chargept_nm_l10n_extstg', '', 'parquet', 'GUIDEWIRE_CDA','RAW','BC_CHARGEPT_NM_L10N');
-- Note that the "new_colume" is all null for existing rows.
    SELECT * FROM GUIDEWIRE_CDA.RAW.BC_CHARGEPT_NM_L10N;
-- Resume the pipe to load the newly staged data with the "new_column"
    ALTER PIPE GUIDEWIRE_CDA.RAW.bc_chargept_nm_l10n SET PIPE_EXECUTION_PAUSED = FALSE;
-- Monitor the pipe status
    SELECT SYSTEM$PIPE_STATUS( 'GUIDEWIRE_CDA.RAW.bc_chargept_nm_l10n' );
-- Watch the raw table for the additional rows with a value in the new column
    SELECT * FROM GUIDEWIRE_CDA.RAW.BC_CHARGEPT_NM_L10N;