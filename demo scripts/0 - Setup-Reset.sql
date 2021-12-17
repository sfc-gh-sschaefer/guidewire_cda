/* =========       RESET DB's + S3        ========== */

USE ROLE ACCOUNTADMIN;
DROP DATABASE IF EXISTS GUIDEWIRE_CDA;
DROP WAREHOUSE IF EXISTS GW_CDA_LOAD_WH;


/* =========       SET UP RBAC, WH + DB's     ======== */

-- Create the guidewire admin role 
CREATE ROLE IF NOT EXISTS GUIDEWIRE_ADMIN;
GRANT ROLE GUIDEWIRE_ADMIN TO ROLE SYSADMIN;
CREATE ROLE IF NOT EXISTS GW_ANALYST;
GRANT ROLE GW_ANALYST TO ROLE GUIDEWIRE_ADMIN;
GRANT CREATE DATABASE ON ACCOUNT TO ROLE GUIDEWIRE_ADMIN;
GRANT CREATE WAREHOUSE ON ACCOUNT TO ROLE GUIDEWIRE_ADMIN;
GRANT EXECUTE TASK ON ACCOUNT TO ROLE GUIDEWIRE_ADMIN;
GRANT MANAGE GRANTS ON ACCOUNT TO ROLE GUIDEWIRE_ADMIN;
USE ROLE GUIDEWIRE_ADMIN;
CREATE OR REPLACE WAREHOUSE GW_CDA_LOAD_WH;
CREATE OR REPLACE DATABASE GUIDEWIRE_CDA;
CREATE OR REPLACE SCHEMA GUIDEWIRE_CDA.RAW;
CREATE OR REPLACE SCHEMA GUIDEWIRE_CDA.MASTER;

GRANT USAGE ON DATABASE GUIDEWIRE_CDA TO ROLE GW_ANALYST;
GRANT USAGE ON ALL SCHEMAS IN DATABASE GUIDEWIRE_CDA TO ROLE GW_ANALYST;
GRANT USAGE ON FUTURE SCHEMAS IN DATABASE GUIDEWIRE_CDA TO ROLE GW_ANALYST;
GRANT SELECT ON ALL TABLES IN DATABASE GUIDEWIRE_CDA TO ROLE GW_ANALYST;
GRANT SELECT ON FUTURE TABLES IN DATABASE GUIDEWIRE_CDA TO ROLE GW_ANALYST;
GRANT SELECT ON ALL VIEWS IN DATABASE GUIDEWIRE_CDA TO ROLE GW_ANALYST;
GRANT SELECT ON FUTURE VIEWS IN DATABASE GUIDEWIRE_CDA TO ROLE GW_ANALYST;

CREATE OR REPLACE FILE FORMAT GUIDEWIRE_CDA.PUBLIC.FILE_FORMAT_PARQUET_DEFAULT 
    TYPE = 'PARQUET' COMPRESSION = 'AUTO' BINARY_AS_TEXT = TRUE;


/* =========       SET UP s3 ACCESS        ========== */

show storage integrations;

-- 1. STORAGE INTEGRATION: Defines a bucket and Credentials to use

-- REPLACE THIS INTEGRATION NAME WITH ONE YOU HAVE CREATED
DESC INTEGRATION USWEST2_S3_INTEGRATION;

-- 2. EXTERNAL STAGE: Defines a location we can access in s3
---- Stages are created at the table level to improve performance compared to a single stage for all tables
CREATE OR REPLACE STAGE GUIDEWIRE_CDA.RAW.gw_bc_chargept_nm_l10n_extstg
    url = 's3://sfc-mwies-extstg-uswest2/guidewire_cda/bc_chargept_nm_l10n/' -- REPLACE THIS LOCATION
    credentials = (AWS_KEY_ID = '<AWS ACCESS KEY ID>' AWS_SECRET_KEY = '<AWS SECRET ACCESS KEY>')
    directory = (
      enable = true
      auto_refresh = true
      aws_sns_topic = 'arn:aws:sns:us-west-2:484577546576:sfc-mwies-bc_chargept_nm_l10n'
    )
    file_format = ( TYPE = PARQUET)
;
ALTER STAGE GUIDEWIRE_CDA.RAW.gw_bc_chargept_nm_l10n_extstg REFRESH;
SELECT * FROM DIRECTORY( @GUIDEWIRE_CDA.RAW.gw_bc_chargept_nm_l10n_extstg );

CREATE OR REPLACE STAGE GUIDEWIRE_CDA.RAW.gw_bc_taccountpattern_extstg
    url = 's3://sfc-mwies-extstg-uswest2/guidewire_cda/bc_taccountpattern/' -- REPLACE THIS LOCATION
    credentials = (AWS_KEY_ID = '<AWS ACCESS KEY ID>' AWS_SECRET_KEY = '<AWS SECRET ACCESS KEY>')
    directory = (
      enable = true
      auto_refresh = true
      aws_sns_topic = 'arn:aws:sns:us-west-2:484577546576:sfc-mwies-taccountpattern'
    )
    file_format = (type = parquet);
ALTER STAGE GUIDEWIRE_CDA.RAW.gw_bc_taccountpattern_extstg REFRESH;
SELECT * FROM DIRECTORY( @GUIDEWIRE_CDA.RAW.gw_bc_taccountpattern_extstg );

-- THESE LINES OF CODE SIMPLY REMOVE DATA WHICH MAY BE CREATED IN A LATER SCRIPT
rm @GUIDEWIRE_CDA.RAW.gw_bc_chargept_nm_l10n_extstg/4fdc0fa344f7452a8a82f05b5c7cab27/1612480304263/;
rm @GUIDEWIRE_CDA.RAW.gw_bc_chargept_nm_l10n_extstg/4fdc0fa344f7452a8a82f05b5c7cab28/;

-- View the files located in your Guidewire s3 Location
list @GUIDEWIRE_CDA.RAW.gw_bc_chargept_nm_l10n_extstg;  


/* ==========       CREATE THE Migrate s3 Table Stored Proc    ========= */
USE ROLE GUIDEWIRE_ADMIN;
USE WAREHOUSE GW_CDA_LOAD_WH;

create or replace procedure guidewire_cda.public.migrate_s3_table(source_stage STRING, source_prefix STRING, source_file_format STRING, target_db STRING,target_schema STRING, target_table STRING)
  returns string
  language javascript
  as     
  $$
// Variable Initialization
    var db_nm = TARGET_DB
    var schema_nm = TARGET_SCHEMA
    var table_nm = TARGET_TABLE
    var fqtn = db_nm+"."+schema_nm+"."+table_nm
    var stage = SOURCE_STAGE
    var prefix = SOURCE_PREFIX
    var file_format = SOURCE_FILE_FORMAT.toLowerCase()
    var added_cols = ""
    var supported_file_formats = "parquet, avro, orc"
    var supported_file_formats_array = supported_file_formats.split(",")
    var create_table_command = ""
    var valid_format = 0;

// Error handling for non-supported file formats
      for(var i = 0; i < supported_file_formats_array.length; i++) {
        if (file_format.toUpperCase() == supported_file_formats_array[i].toUpperCase()){
           valid_format = 1;
         }
      }
      if (valid_format == 0){
       return "Please enter a supported file format. \n Supported File Formats are: "+supported_file_formats;
     }

// Store a comma-separated list of all of the columns that already exist in the table.
    var cols_curr_command = "SELECT COLUMN_NAME FROM "+db_nm+".INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '"+schema_nm+"' AND TABLE_NAME = '"+table_nm+"'";
    var cols_curr_statement = snowflake.createStatement( {sqlText: cols_curr_command});
    var cols_curr = cols_curr_statement.execute();

// Store the incoming schema information by running INFER_SCHEMA on the s3 bucket location
    var cols_in_command = "select COLUMN_NAME,TYPE,EXPRESSION from table( \
                            INFER_SCHEMA( LOCATION=>'@"+stage+"/"+prefix+"' , \
                            FILE_FORMAT=>'GUIDEWIRE_CDA.PUBLIC.FILE_FORMAT_PARQUET_DEFAULT' ))order by 1";
    var cols_in_statement = snowflake.createStatement( {sqlText: cols_in_command} );

// Make a list of current column names for comparison
    var cols_curr_list ="";
    while(cols_curr.next()){
        cols_curr_list = cols_curr_list+cols_curr.getColumnValue(1)+",";
    }
    var cols_curr_array = cols_curr_list.split(',');

//Check if the table exists. If it does not, create based on incoming schema.
    if(!cols_curr_list){
        var first_field = 1;
        create_table_command = "create table "+fqtn+"("
        var cols_in_details = cols_in_statement.execute();
        while (cols_in_details.next())  {
           var column_name = cols_in_details.getColumnValue(1);
           var column_type = cols_in_details.getColumnValue(2);
           if(first_field != 1){
                create_table_command = create_table_command+",";
           }
           create_table_command = create_table_command+column_name+" "+column_type
           first_field=0;
       }
       create_table_command = create_table_command+")";
       create_table_statement = snowflake.createStatement( {sqlText: create_table_command} );
       create_table_statement.execute();
    }
    else{
// If the table does already exist, first check if all existing fields are also in the incoming schema
//// Make a list of incoming column names for comparison
      var cols_in_list ="";
      var cols_in_details = cols_in_statement.execute();
      while (cols_in_details.next())  {
         cols_in_list = cols_in_list+cols_in_details.getColumnValue(1)+",";
      }
      var cols_curr_array = cols_curr_list.split(',');
      for(var i = 0; i < cols_curr_array.length-1; i++) {
        if (cols_in_list.toUpperCase().indexOf(cols_curr_array[i].toUpperCase()) == -1){
           return "Existing Column Missing from Incoming Schema: "+cols_curr_array[i]
         }
      }
// Loop through the incoming column names to check if they already exist and add them if they do not. 
      var cols_in_details = cols_in_statement.execute();
      while (cols_in_details.next())  {
         var column_name = cols_in_details.getColumnValue(1);
         var column_type = cols_in_details.getColumnValue(2);
         var column_expr = cols_in_details.getColumnValue(3);
         if (cols_curr_list.indexOf(column_name.toUpperCase()) == -1){
           var add_column_command = "alter table "+fqtn+" add column "+column_name+" "+column_type;
           var add_column_statement = snowflake.createStatement( {sqlText: add_column_command} );
           add_column_statement.execute();
           added_cols = added_cols+column_name+" "+column_type+", ";
         }
      }
    }

//Return a moderately helpful message
  if(create_table_command){
    return "Created Table "+fqtn+"\n -------------------------------- \n"+create_table_command;
  }
  else if (added_cols){
      return "Added the Following Columns to Table "+fqtn+"\n -------------------------------- \n"+added_cols;
  }
  else{
    return fqtn+" -- No new columns to add."
  }
  $$
  ;


/* ==========       CREATE THE BULK LOAD Stored Proc    ========= */
USE ROLE GUIDEWIRE_ADMIN;
USE WAREHOUSE GW_CDA_LOAD_WH;

create or replace procedure guidewire_cda.public.bulk_load(source_stage STRING, source_prefix STRING, source_file_format STRING, target_db STRING,target_schema STRING, target_table STRING)
  returns string
  language javascript
  as     
  $$
// Variable Initialization
    var db_nm = TARGET_DB
    var schema_nm = TARGET_SCHEMA
    var table_nm = TARGET_TABLE
    var fqtn = db_nm+"."+schema_nm+"."+table_nm
    var stage = SOURCE_STAGE
    var prefix = SOURCE_PREFIX
    var file_format = SOURCE_FILE_FORMAT.toLowerCase()
    var added_cols = ""
    var supported_file_formats = "parquet, avro, orc"
    var supported_file_formats_array = supported_file_formats.split(",")
    var insert_command = ""
    var valid_format = 0;
    var first_field = 1;

// Error handling for non-supported file formats
      for(var i = 0; i < supported_file_formats_array.length; i++) {
        if (file_format.toUpperCase() == supported_file_formats_array[i].toUpperCase()){
           valid_format = 1;
         }
      }
      if (valid_format == 0){
       return "Please enter a supported file format. \n Supported File Formats are: "+supported_file_formats;
     }

// Store a comma-separated list of all of the columns that already exist in the table.
    var cols_curr_command = "SELECT COLUMN_NAME FROM "+db_nm+".INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '"+schema_nm+"' AND TABLE_NAME = '"+table_nm+"'";
    var cols_curr_statement = snowflake.createStatement( {sqlText: cols_curr_command});
    var cols_curr = cols_curr_statement.execute();

// Store the incoming schema information by running INFER_SCHEMA on the s3 bucket location
    var cols_in_command = "select COLUMN_NAME,TYPE,EXPRESSION from table( \
                            INFER_SCHEMA( LOCATION=>'@"+stage+"/"+prefix+"' , \
                            FILE_FORMAT=>'GUIDEWIRE_CDA.PUBLIC.FILE_FORMAT_PARQUET_DEFAULT' ))order by 1";
    var cols_in_statement = snowflake.createStatement( {sqlText: cols_in_command} );
    var cols_in_details = cols_in_statement.execute();

// Make a list of current column names for comparison
    var cols_curr_list ="";
    while(cols_curr.next()){
        cols_curr_list = cols_curr_list+cols_curr.getColumnValue(1)+",";
    }
    var cols_curr_array = cols_curr_list.split(',');

//Check if the table exists. If it does not, exit with error.
    if(!cols_curr_list){
        return 'Table does not exist. Please run table migration function';
    }
    else{
// If the table does already exist, first check if all existing fields are also in the incoming schema
//// Make a list of incoming column names for comparison
      var cols_in_list ="";
      while (cols_in_details.next())  {
         cols_in_list = cols_in_list+cols_in_details.getColumnValue(1)+",";
      }
      for(var i = 0; i < cols_curr_array.length-1; i++) {
        if (cols_in_list.toUpperCase().indexOf(cols_curr_array[i].toUpperCase()) == -1){
           return "Existing Column Missing from Incoming Schema: "+cols_curr_array[i]
         }
      }
// Loop through the incoming column names to create insert statement. Fail if any columns do not exist in target table. 
      var cols_in_details = cols_in_statement.execute();
      insert_command = "INSERT INTO "+fqtn+" SELECT ";
      while (cols_in_details.next())  {
         var column_name = cols_in_details.getColumnValue(1);
         var column_type = cols_in_details.getColumnValue(2);
         var column_expr = cols_in_details.getColumnValue(3);
         if (cols_curr_list.indexOf(column_name.toUpperCase()) == -1){
           return "Incoming Column missing from existing schema. Re-run table migration function: "+cols_curr_array[i]
         }
        if(first_field != 1){
            insert_command = insert_command+",";
        }
        insert_command = insert_command+"$1:"+column_name+"::"+column_type+" AS "+column_name
        first_field=0;
      }
      insert_command = insert_command+" FROM @"+stage+"/"+prefix+" (file_format => 'guidewire_cda.public.file_format_parquet_default',pattern => '.*.parquet')";
      insert_statement = snowflake.createStatement( {sqlText: insert_command} );
      execution_status = insert_statement.execute();
    }

//Return a moderately helpful message
  if(insert_command){
    return "Inserted into table "+fqtn+"\n -------------------------------- \n"+insert_command;
  }
  else{
    return "INSERT statement wasn't created. Troubleshooting required."
  }
  $$
  ;

SELECT 'Reset Complete.' as Status;