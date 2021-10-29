/* ============    AUTO-INGEST FOR MICRO-BATCH LOADS    ========== */
USE ROLE GUIDEWIRE_ADMIN;
USE WAREHOUSE GW_CDA_LOAD_WH;
    
-- CREATE A PIPE... A SIMPLE COPY STATEMENT TO SELECT FROM THE STAGE AND INSERT INTO TARGET TABLE
    CREATE OR REPLACE PIPE GUIDEWIRE_CDA.LANDING.bc_chargept_nm_l10n
    AUTO_INGEST=TRUE 
    AWS_SNS_TOPIC = 'arn:aws:sns:us-west-2:484577546576:sfc-mwies-bc_chargept_nm_l10n'
    AS
      copy into "GUIDEWIRE_CDA"."RAW"."BC_CHARGEPT_NM_L10N"
      from (
      SELECT 
            $1:beanversion::NUMBER                    AS beanversion 
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
       FROM @GUIDEWIRE_CDA.LANDING.s3_extstg_guidewire/bc_chargept_nm_l10n (
          file_format => (COMMON.PUBLIC.FILE_FORMAT_PARQUET_DEFAULT), 
          PATTERN => '.*.parquet'
        )
      );
    
    -- Show pipes to get the ARN for the s3 notifications
    ---- Use the "notification_channel" SQS ARN to set up an s3 Event Notification (may require guidewire admin)
    DESCRIBE PIPE GUIDEWIRE_CDA.LANDING.BC_CHARGEPT_NM_L10N;

    -- 
    SELECT SYSTEM$PIPE_STATUS( 'GUIDEWIRE_CDA.LANDING.bc_chargept_nm_l10n' );
    SELECT * FROM GUIDEWIRE_CDA.RAW.BC_CHARGEPT_NM_L10N;


/* ===========================================================
/* =========   REPEAT FOR TACCOUNTPATTERN TABLE   ========= */
    -- CREATE A PIPE... A SIMPLE COPY STATEMENT TO SELECT FROM THE STAGE AND INSERT INTO TARGET TABLE
    CREATE OR REPLACE PIPE GUIDEWIRE_CDA.LANDING.TACCOUNTPATTERN
    AUTO_INGEST=TRUE 
    AWS_SNS_TOPIC = 'arn:aws:sns:us-west-2:484577546576:sfc-mwies-taccountpattern'
    AS
      copy into "GUIDEWIRE_CDA"."RAW"."BC_TACCOUNTPATTERN"
      from (
      SELECT 
             $1:beanversion::NUMBER                    AS beanversion 
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
      FROM @GUIDEWIRE_CDA.LANDING.s3_extstg_guidewire/bc_taccountpattern(
          file_format => (COMMON.PUBLIC.FILE_FORMAT_PARQUET_DEFAULT), 
          PATTERN => '.*.parquet'
          )
      );
-- Show pipes to get the ARN for the s3 notifications
---- Use the "notification_channel" SQS ARN to set up an s3 Event Notification (may require guidewire admin)
    DESCRIBE PIPE GUIDEWIRE_CDA.LANDING.TACCOUNTPATTERN;

-- 
SELECT SYSTEM$PIPE_STATUS( 'GUIDEWIRE_CDA.LANDING.TACCOUNTPATTERN' );
SELECT * FROM GUIDEWIRE_CDA.RAW.BC_TACCOUNTPATTERN;