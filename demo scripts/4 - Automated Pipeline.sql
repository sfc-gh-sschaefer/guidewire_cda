/* ============    STREAMS AND TASKS TO LOAD FROM RAW TO ENTERPRISE LAYER    ============ */ 
USE ROLE GUIDEWIRE_ADMIN;
USE WAREHOUSE GW_CDA_LOAD_WH;

-- create streams to track ingestion
create or replace stream GUIDEWIRE_CDA.RAW.BC_CHARGEPT_NM_L10N_STREAM on table GUIDEWIRE_CDA.RAW.BC_CHARGEPT_NM_L10N;

show streams in GUIDEWIRE_CDA.RAW;

-- the streams will keep track of changes (inserts) to the landing table
select count(*) from GUIDEWIRE_CDA.RAW.BC_CHARGEPT_NM_L10N_STREAM;
-- select * from GUIDEWIRE_CDA.RAW.BC_CHARGEPT_NM_L10N_STREAM limit 100;

/*--------------------------------------------------------------------------------
  Tasks allow us to define and orchestrate the ELT logic. We consume the records
  collected by the streams by running DML transactions (insert, merge). This
  resets the stream so we can capture changes going forward from here.
--------------------------------------------------------------------------------*/

-- create a warehouse to run tasks
create warehouse if not exists gw_cda_task_wh auto_suspend = 30 initially_suspended = true;

-- create a 'modelled' enterprise table which removes CDC complexity for most consumers
CREATE OR REPLACE TABLE GUIDEWIRE_CDA.ENTERPRISE.BC_CHARGEPT_NM_L10N (
    LOAD_TS             TIMESTAMP
    ,ID                 NUMBER
    ,LANGUAGE           STRING
    ,OWNER              STRING
    ,PUBLICID           STRING
    ,VALUE              STRING
    ,NEW_COLUMN         NUMBER);
    
    
/*--------------------------------------------------------------------------------
  ======                  RUN INITIAL ENTERPRISE LOAD                  =========
--------------------------------------------------------------------------------*/

-- This select statement uses an ad-hoc filter which uses the GWCBI___OPERATION field
---- to filter in just the INSERTED records (assume 2 = insert, 3 = delete)
---- It selects only the columns relevant to users who don't care about CDC
---- It also needs to be corrected to handle deletes properly.
INSERT INTO GUIDEWIRE_CDA.ENTERPRISE.BC_CHARGEPT_NM_L10N
  SELECT * FROM(
    WITH LATEST_INSERTS AS (
      SELECT MAX(LOAD_TS) AS LOAD_TS,ID
      FROM GUIDEWIRE_CDA.RAW.BC_CHARGEPT_NM_L10N
      WHERE GWCBI___OPERATION=2
      GROUP BY ID
      ), LATEST_DELETES AS (
      SELECT MAX(LOAD_TS) AS LOAD_TS,ID
      FROM GUIDEWIRE_CDA.RAW.BC_CHARGEPT_NM_L10N
      WHERE GWCBI___OPERATION=3
      GROUP BY ID
    ), ALL_TX AS (
    SELECT 
      LOAD_TS
      ,ID
      ,LANGUAGE
      ,OWNER
      ,PUBLICID
      ,VALUE
      ,NEW_COLUMN
    FROM "GUIDEWIRE_CDA"."RAW"."BC_CHARGEPT_NM_L10N"
    )
    SELECT DISTINCT ALL_TX.LOAD_TS,ALL_TX.ID,ALL_TX.LANGUAGE,ALL_TX.OWNER,ALL_TX.PUBLICID,ALL_TX.VALUE,ALL_TX.NEW_COLUMN
    FROM ALL_TX JOIN LATEST_INSERTS ON ALL_TX.ID=LATEST_INSERTS.ID AND ALL_TX.LOAD_TS=LATEST_INSERTS.LOAD_TS
    LEFT JOIN LATEST_DELETES ON ALL_TX.ID=LATEST_DELETES.ID AND LATEST_DELETES.LOAD_TS > LATEST_INSERTS.LOAD_TS
  )
  ;

-- Preview the data
select * From GUIDEWIRE_CDA.ENTERPRISE.BC_CHARGEPT_NM_L10N;

-- Create task to push transactions data into final transactions table
-- This tasks uses Snowflake's native MERGE function to handle upserting and deletes.
create or replace task GUIDEWIRE_CDA.RAW.BC_CHARGEPT_NM_L10N_LOAD_TASK 
  warehouse = gw_cda_task_wh
  schedule = '1 minute'
  when system$stream_has_data('GUIDEWIRE_CDA.RAW.BC_CHARGEPT_NM_L10N_STREAM')
  as
  merge into GUIDEWIRE_CDA.ENTERPRISE.BC_CHARGEPT_NM_L10N TARGET
  using (SELECT GWCBI___OPERATION,LOAD_TS,ID,LANGUAGE,OWNER,PUBLICID,VALUE,NEW_COLUMN FROM GUIDEWIRE_CDA.RAW.BC_CHARGEPT_NM_L10N_STREAM) SOURCE
  on SOURCE.ID=TARGET.ID
  WHEN MATCHED 
    AND SOURCE.GWCBI___OPERATION=2 
  THEN UPDATE SET
    TARGET.LOAD_TS=SOURCE.LOAD_TS,
    TARGET.LANGUAGE=SOURCE.LANGUAGE,
    TARGET.OWNER=SOURCE.OWNER,
    TARGET.PUBLICID=SOURCE.PUBLICID,
    TARGET.VALUE=SOURCE.VALUE,
    TARGET.NEW_COLUMN=SOURCE.NEW_COLUMN
  WHEN MATCHED
    AND SOURCE.GWCBI___OPERATION=3
  THEN DELETE
  WHEN NOT MATCHED
    THEN INSERT VALUES(SOURCE.LOAD_TS,SOURCE.ID,SOURCE.LANGUAGE,SOURCE.OWNER,SOURCE.PUBLICID,SOURCE.VALUE,SOURCE.NEW_COLUMN);

 
/*********************************************************************
  Enable the task so it will run and process the data
  in the streams
*********************************************************************/
 
alter task GUIDEWIRE_CDA.RAW.BC_CHARGEPT_NM_L10N_LOAD_TASK resume;

show tasks in GUIDEWIRE_CDA.RAW;

/*********************************************************************
 SIMULATE A NEW FINGERPRINT BEING ADDED WITH A SCHEMA THAT INCLUDES '888' IN THE NEW COLUMN
*********************************************************************/

COPY INTO @GUIDEWIRE_CDA.LANDING.s3_extstg_guidewire/bc_chargept_nm_l10n/4fdc0fa344f7452a8a82f05b5c7cab28/2612480304265/ADDED_COLUMN_
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
        ,id AS "id"
        ,language AS "language"
        ,owner AS "owner"
        ,publicid AS "publicid"
        ,VALUE AS "value" 
        ,888 AS "new_column"
    FROM GUIDEWIRE_CDA.RAW.BC_CHARGEPT_NM_L10N
    WHERE ID>200
)
HEADER = TRUE 
OVERWRITE = TRUE;


/*********************************************************************
  Now monitor the data pipeline...
*********************************************************************/

-- SEE THE NEW DATA BEING QUEUED UP INTO THE PIPE
SELECT SYSTEM$PIPE_STATUS( 'GUIDEWIRE_CDA.LANDING.bc_chargept_nm_l10n' );

select count(*) from GUIDEWIRE_CDA.RAW.BC_CHARGEPT_NM_L10N_STREAM;

-- how long to next task run?
select timestampdiff(second, current_timestamp, scheduled_time) next_run, scheduled_time, name, state
  from table(information_schema.task_history())
  where state = 'SCHEDULED' order by completed_time desc;
  
-- task run history
select * from table(information_schema.task_history())
  where scheduled_time > dateadd(minute, -5, current_time())
  and state <> 'SCHEDULED'
  order by completed_time desc;
  
-- check enterprise table
select * from "GUIDEWIRE_CDA"."ENTERPRISE"."BC_CHARGEPT_NM_L10N" limit 1000;

-- check the raw table
select * from "GUIDEWIRE_CDA"."RAW"."BC_CHARGEPT_NM_L10N" limit 1000;