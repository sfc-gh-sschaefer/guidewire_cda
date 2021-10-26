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

-- Inpect the schema of the external table using schema inference
  select *
    from table(
      INFER_SCHEMA(
      LOCATION=>'@GUIDEWIRE_CDA.LANDING.s3_extstg_guidewire/bc_chargept_nm_l10n/'
      , FILE_FORMAT=>'GUIDEWIRE_CDA.COMMON.FILE_FORMAT_PARQUET_DEFAULT'
        )
      )order by 1;

-- CREATE A TARGET TABLE TO LOAD THE RAW CDC DATA IN A STRUCTURED FORMAT 
  call guidewire_cda.public.migrate_gw_table_def('GUIDEWIRE_CDA','RAW','BC_CHARGEPT_NM_L10N');
/*
  CREATE OR REPLACE TABLE GUIDEWIRE_CDA.RAW.bc_chargept_nm_l10n (
      load_ts                   TIMESTAMP
      ,beanversion              NUMBER 
      ,gwcbi___connector_ts_ms  NUMBER
      ,gwcbi___lsn              NUMBER
      ,gwcbi___operation        NUMBER
      ,gwcbi___payload_ts_ms    NUMBER
      ,gwcbi___seqval_hex       STRING
      ,gwcbi___tx_id            NUMBER
      ,id                       NUMBER
      ,language                 NUMBER
      ,owner                    NUMBER
      ,publicid                 STRING
      ,value                    STRING
      ,load_type                STRING
  );    
*/

-- SIMPLE SELECT and INSERT TO LOAD (Can be built in to a scheduled task or executed in a pipeline)
    INSERT INTO GUIDEWIRE_CDA.RAW.bc_chargept_nm_l10n
    SELECT 
        CURRENT_TIMESTAMP::TIMESTAMP               AS load_ts
        ,'HISTORIC'                                AS load_type
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
    FROM @GUIDEWIRE_CDA.LANDING.s3_extstg_guidewire/bc_chargept_nm_l10n/
    (file_format => guidewire_cda.public.file_format_parquet_default, pattern=>'.*.parquet')
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
  call guidewire_cda.public.migrate_gw_table_def('GUIDEWIRE_CDA','RAW','BC_TACCOUNTPATTERN');
/*
  CREATE OR REPLACE TABLE GUIDEWIRE_CDA.RAW.bc_taccountpattern (
      load_ts                   TIMESTAMP
      ,beanversion              NUMBER 
      ,chargepatternid          INTEGER
      ,createtime               TIMESTAMP
      ,gwcbi___connector_ts_ms  NUMBER
      ,gwcbi___lsn              NUMBER
      ,gwcbi___operation        NUMBER
      ,gwcbi___payload_ts_ms    NUMBER
      ,gwcbi___seqval_hex       STRING
      ,gwcbi___tx_id            NUMBER
      ,id                       NUMBER
      ,publicid                 STRING
      ,retired                  INTEGER
      ,subtype                  INTEGER
      ,suffix                   INTEGER
      ,taccountlazyloaded       STRING
      ,taccountname             STRING
      ,taccountownerpatternid   INTEGER
      ,taccounttype             INTEGER
      ,updatetime               TIMESTAMP
      ,load_type                STRING
  );    
*/
-- SIMPLE SELECT and INSERT TO LOAD ONE TIME
    INSERT INTO GUIDEWIRE_CDA.RAW.bc_taccountpattern
    SELECT 
        CURRENT_TIMESTAMP::TIMESTAMP               AS load_ts
        ,'HISTORIC'
        ,$1:beanversion::NUMBER                    AS beanversion 
        ,$1:chargepatternid::INTEGER               AS chargepatternid
        ,$1:createtime::TIMESTAMP                  AS createtime
        ,$1:createuserid::INTEGER                  AS createuserid
        ,$1:gwcbi___connector_ts_ms::NUMBER        AS gwcbi___connector_ts_ms
        ,$1:gwcbi___lsn::NUMBER                    AS gwcbi___lsn
        ,$1:gwcbi___operation::NUMBER              AS gwcbi___operation
        ,$1:gwcbi___payload_ts_ms::NUMBER          AS gwcbi___payload_ts_ms
        ,$1:gwcbi___seqval_hex::STRING             AS gwcbi___seqval_hex
        ,$1:gwcbi___tx_id::NUMBER                  AS gwcbi___tx_id
        ,$1:id::NUMBER                             AS id
        ,$1:publicid::STRING                       AS publicid
        ,$1:retired::INTEGER                       AS retired
        ,$1:subtype::INTEGER                       AS subtype
        ,$1:suffix::INTEGER                        AS suffix
        ,$1:taccountlazyloaded::STRING             AS taccountlazyloaded
        ,$1:taccountname::STRING                   AS taccountname
        ,$1:taccountownerpatternid::INTEGER        AS taccountownerpatternid
        ,$1:taccounttype::INTEGER                  AS taccounttype
        ,$1:updatetime::TIMESTAMP                  AS updatetime
        ,$1:updateuserid::INTEGER                  AS updateuserid
    FROM GUIDEWIRE_CDA.LANDING.bc_taccountpattern_ext;