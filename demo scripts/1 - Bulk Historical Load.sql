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
-- Create an external table to facilitate the load from external 
  call guidewire_cda.public.et_from_s3('GUIDEWIRE_CDA.LANDING.s3_extstg_guidewire','bc_chargept_nm_l10n/','Parquet','GUIDEWIRE_CDA','RAW','BC_CHARGEPT_NM_L10N');
-- LOAD THE RAW CDC DATA IN A STRUCTURED FORMAT 
  INSERT INTO GUIDEWIRE_CDA.RAW.BC_CHARGEPT_NM_L10N(
    BEANVERSION
    ,GWCBI___CONNECTOR_TS_MS
    ,GWCBI___LSN 
    ,GWCBI___OPERATION 
    ,GWCBI___PAYLOAD_TS_MS 
    ,GWCBI___SEQVAL_HEX
    ,GWCBI___TX_ID
    ,ID
    ,LANGUAGE
    ,OWNER
    ,PUBLICID
    ,VALUE
  ) SELECT
    BEANVERSION
    ,GWCBI___CONNECTOR_TS_MS
    ,GWCBI___LSN 
    ,GWCBI___OPERATION 
    ,GWCBI___PAYLOAD_TS_MS 
    ,GWCBI___SEQVAL_HEX
    ,GWCBI___TX_ID
    ,ID
    ,LANGUAGE
    ,OWNER
    ,PUBLICID
    ,VALUE::STRING
  FROM GUIDEWIRE_CDA.RAW.BC_CHARGEPT_NM_L10N_ET;

-- Preview Raw table in Object Browser

/* ===========================================================
/* =========   REPEAT FOR TACCOUNTPATTERN TABLE   ========= */



-- CREATE A TARGET TABLE TO LOAD THE RAW CDC DATA IN A STRUCTURED FORMAT 
  call guidewire_cda.public.migrate_s3_table('GUIDEWIRE_CDA.LANDING.s3_extstg_guidewire','bc_taccountpattern/','Parquet','GUIDEWIRE_CDA','RAW','BC_TACCOUNTPATTERN');
-- Create an external table to facilitate the load from external 
  call guidewire_cda.public.et_from_s3('GUIDEWIRE_CDA.LANDING.s3_extstg_guidewire','bc_taccountpattern/','Parquet','GUIDEWIRE_CDA','RAW','BC_TACCOUNTPATTERN');
-- LOAD THE RAW CDC DATA IN A STRUCTURED FORMAT 
  INSERT INTO GUIDEWIRE_CDA.RAW.BC_TACCOUNTPATTERN(
    BEANVERSION
    ,CHARGEPATTERNID
    ,CREATETIME
    ,CREATEUSERID
    ,GWCBI___CONNECTOR_TS_MS
    ,GWCBI___LSN 
    ,GWCBI___OPERATION 
    ,GWCBI___PAYLOAD_TS_MS 
    ,GWCBI___SEQVAL_HEX
    ,GWCBI___TX_ID
    ,ID
    ,PUBLICID
    ,RETIRED
    ,SUBTYPE
    ,SUFFIX
    ,TACCOUNTLAZYLOADED
    ,TACCOUNTNAME
    ,TACCOUNTOWNERPATTERNID
    ,TACCOUNTTYPE
    ,UPDATETIME
    ,UPDATEUSERID
  ) SELECT
    BEANVERSION
    ,CHARGEPATTERNID
    ,CREATETIME
    ,CREATEUSERID
    ,GWCBI___CONNECTOR_TS_MS
    ,GWCBI___LSN 
    ,GWCBI___OPERATION 
    ,GWCBI___PAYLOAD_TS_MS 
    ,GWCBI___SEQVAL_HEX
    ,GWCBI___TX_ID
    ,ID
    ,PUBLICID
    ,RETIRED
    ,SUBTYPE
    ,SUFFIX
    ,TACCOUNTLAZYLOADED
    ,TACCOUNTNAME
    ,TACCOUNTOWNERPATTERNID
    ,TACCOUNTTYPE
    ,UPDATETIME
    ,UPDATEUSERID
  FROM GUIDEWIRE_CDA.RAW.BC_TACCOUNTPATTERN_ET;