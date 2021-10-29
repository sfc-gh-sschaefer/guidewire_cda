/* ============    BULK LOAD INITIAL HISTORICAL DATA    ============ */
USE ROLE GUIDEWIRE_ADMIN;
USE WAREHOUSE GW_CDA_LOAD_WH;

-- WRITE AN EXTERNAL TABLE DEFN (This will simplify the load query later)
  CREATE OR REPLACE EXTERNAL TABLE GUIDEWIRE_CDA.LANDING.bc_chargept_nm_l10n_ext
    ( filename string AS METADATA$FILENAME)
    LOCATION = @GUIDEWIRE_CDA.LANDING.s3_extstg_guidewire/bc_chargept_nm_l10n/
    REFRESH_ON_CREATE =  TRUE
    AUTO_REFRESH = TRUE
    PATTERN = '.*.parquet'
    FILE_FORMAT = (type = parquet);
    
SELECT * FROM GUIDEWIRE_CDA.LANDING.bc_chargept_nm_l10n_ext;

-- Inspect the schema of the external table using schema inference
  select *
    from table(
      INFER_SCHEMA(
      LOCATION=>'@GUIDEWIRE_CDA.LANDING.s3_extstg_guidewire/bc_chargept_nm_l10n/'
      , FILE_FORMAT=>'GUIDEWIRE_CDA.PUBLIC.FILE_FORMAT_PARQUET_DEFAULT'
        )
      )order by 1;

-- CREATE A TARGET TABLE TO LOAD THE RAW CDC DATA IN A STRUCTURED FORMAT 
  call guidewire_cda.public.migrate_s3_table('GUIDEWIRE_CDA.LANDING.s3_extstg_guidewire','bc_chargept_nm_l10n/','Parquet','GUIDEWIRE_CDA','RAW','BC_CHARGEPT_NM_L10N');
  call guidewire_cda.public.bulk_load_from_s3('GUIDEWIRE_CDA.LANDING.s3_extstg_guidewire','bc_chargept_nm_l10n/','Parquet','GUIDEWIRE_CDA','RAW','BC_CHARGEPT_NM_L10N');

-- SIMPLE SELECT and INSERT TO LOAD (Can be built in to a scheduled task or executed in a pipeline)
    INSERT INTO GUIDEWIRE_CDA.RAW.bc_chargept_nm_l10n 
      SELECT
        $1:beanversion::NUMBER                    AS beanversion 
        ,$1:gwcbi___connector_ts_ms::NUMBER        AS gwcbi___connector_ts_ms
        ,$1:gwcbi___lsn::NUMBER                    AS gwcbi___lsn
        ,$1:gwcbi___operation::NUMBER              AS gwcbi___operation
        ,$1:gwcbi___payload_ts_ms::NUMBER          AS gwcbi___payload_ts_ms
        ,$1:gwcbi___seqval_hex::STRING             AS gwcbi___seqval_hex
        ,$1:gwcbi___tx_id::NUMBER                  AS gwcbi___tx_id
        ,$1:id::NUMBER                             AS id
        ,$1:language::NUMBER                       AS id
        ,$1:owner::NUMBER                          AS id
        ,$1:publicid::STRING                       AS id
        ,$1:value::STRING                          AS id
    FROM @GUIDEWIRE_CDA.LANDING.s3_extstg_guidewire/bc_chargept_nm_l10n/
      (file_format => 'guidewire_cda.public.file_format_parquet_default', 
      pattern => '.*.parquet'
       )
      ;

-- Preview Raw table in Object Browser

/* ===========================================================
/* =========   REPEAT FOR TACCOUNTPATTERN TABLE   ========= */

-- WRITE AN EXTERNAL TABLE DEFN (This will simplify the load query later)
  CREATE OR REPLACE EXTERNAL TABLE GUIDEWIRE_CDA.LANDING.bc_taccountpattern_ext
    ( filename string AS METADATA$FILENAME)
    LOCATION = @GUIDEWIRE_CDA.LANDING.s3_extstg_guidewire/bc_taccountpattern/
    REFRESH_ON_CREATE =  TRUE
    AUTO_REFRESH = TRUE
    PATTERN = '.*.parquet'
    FILE_FORMAT = (type = parquet);
    
SELECT * FROM GUIDEWIRE_CDA.LANDING.bc_taccountpattern_ext;

-- CREATE A TARGET TABLE TO LOAD THE RAW CDC DATA IN A STRUCTURED FORMAT 
  call guidewire_cda.public.migrate_s3_table('GUIDEWIRE_CDA.LANDING.s3_extstg_guidewire','bc_taccountpattern/','Parquet','GUIDEWIRE_CDA','RAW','BC_TACCOUNTPATTERN');
  call guidewire_cda.public.bulk_load_from_s3('GUIDEWIRE_CDA.LANDING.s3_extstg_guidewire','bc_taccountpattern/','Parquet','GUIDEWIRE_CDA','RAW','BC_TACCOUNTPATTERN');