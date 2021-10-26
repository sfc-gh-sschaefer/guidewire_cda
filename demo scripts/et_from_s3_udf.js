create or replace procedure common.public.et_from_s3(source_stage STRING, source_prefix STRING, source_file_format STRING, target_db STRING,target_schema STRING, target_table STRING)
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
                            FILE_FORMAT=>'COMMON.PUBLIC.FILE_FORMAT_PARQUET_DEFAULT' ))order by 1";
    var cols_in_statement = snowflake.createStatement( {sqlText: cols_in_command} );
    var cols_in_details = cols_in_statement.execute();

// Make a list of current column names for comparison
    var cols_curr_list ="";
    while(cols_curr.next()){
        cols_curr_list = cols_curr_list+cols_curr.getColumnValue(1)+",";
    }
    var cols_curr_array = cols_curr_list.split(',');

// Make a list of incoming column names for comparison
    var cols_in_list ="";
    while (cols_in_details.next())  {
    cols_in_list = cols_in_list+cols_in_details.getColumnValue(1)+",";
    }
    var cols_in_array = cols_in_list.split(',');

//Check if the table exists. If it does not, create based on incoming schema.
    if(!cols_curr_list){
        var first_field = 1;
        create_table_command = "create table "+fqtn+"("
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
      while (cols_in_details.next())  {
         cols_in_list = cols_in_list+cols_in_details.getColumnValue(1)+",";
      }
      var cols_curr_array = cols_curr_list.split(',');
      for(var i = 0; i < cols_curr_array.length-1; i++) {
        if (cols_in_list.toUpperCase().indexOf(cols_curr_array[i].toUpperCase()) == -1){
           return "Existing Column Missing from Incoming Schema: "+cols_curr_array[i]
         }
      }
// Loop through the incoming column names to check if they already exist and add them if they don't. 
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

// Grab the external table column expressions
    var col_expressions_command = "SELECT GENERATE_COLUMN_DESCRIPTION(ARRAY_AGG(OBJECT_CONSTRUCT(*)), 'external_table') AS COLUMNS \
    FROM TABLE ( INFER_SCHEMA( LOCATION=>'@"+stage+"/"+prefix+"' , \
    FILE_FORMAT=>'COMMON.PUBLIC.FILE_FORMAT_PARQUET_DEFAULT' ))";
    var col_expressions_statement = snowflake.createStatement( {sqlText: col_expressions_command});
    var col_expressions = col_expressions_statement.execute();

// Create and execute a statement to select from the stage and insert into the raw table
    var insert_select_command = "CREATE OR REPLACE EXTERNAL TABLE"+fqtn+"_ext \
    ( filename string AS METADATA$FILENAME) \
    LOCATION = @"+stage+"."+prefix+" \
    REFRESH_ON_CREATE =  TRUE \
    AUTO_REFRESH = TRUE \
    PATTERN = '.*."+file_format+"' \
    FILE_FORMAT = (type = "+file_format+")";
    return create_et_command;

var insert_select_statement = snowflake.createStatement( {sqlText: insert_select_command});
var insert_select = insert_select_statement.execute();

create_table_command = "create or replace external table "+fqtn+"(";
while (col_expressions.next())  {
    if (col_expressions.getColumnValue(1).toUpperCase() == "VALUE"){
        col_name = "value_in";
    }
    else{ col_name = col_expressions.getColumnValue(1)}
    create_table_command = create_table_command+col_expressions.getColumnValue(1);
}
create_table_command = create_table_command+") LOCATION = @COMMON.PUBLIC.USWEST2_S3_EXTSTG/guidewire/bc_chargept_nm_l10n/ \
                                                REFRESH_ON_CREATE =  TRUE \
                                                AUTO_REFRESH = TRUE \
                                                PATTERN = '.*."+file_format+"' \
                                                FILE_FORMAT = (type = "+file_format+")";
return create_table_command;

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

/* Example Usage 
--  Creates a table called TEST_TABLE in the schema COMMON.PUBLIC
--  The schema for this table is inferred from parquet data in an s3 stage called GUIDEWIRE_CDA.LANDING.S3_EXTSTG_GUIDEWIRE.
--  In this stage, the data is under a prefix (folder) 'bc_chargept_nm_l10n' in parquet format
--  All parquet files have the '.parquet' suffix
show stages;
show file formats;
ls @COMMON.PUBLIC.USWEST2_S3_EXTSTG/citibike_trips/2013/
SELECT $1 FROM @COMMON.PUBLIC.USWEST2_S3_EXTSTG/citibike_trips/2013/ (file_format => common.public.file_format_csv_dquote, PATTERN => '.*.csv.gz' );
*/
call common.public.et_from_s3('COMMON.PUBLIC.USWEST2_S3_EXTSTG','guidewire_cda/bc_chargept_nm_l10n/','Parquet','COMMON','PUBLIC','TEST_TABLE');
DROP TABLE COMMON.PUBLIC.TEST_TABLE;
ALTER TABLE COMMON.PUBLIC.TEST_TABLE ADD COLUMN NEW_COLUMN_2 VARCHAR;
















create or replace procedure common.public.auto_load_s3_table(source_stage STRING, source_prefix STRING, source_file_format STRING, target_db STRING,target_schema STRING, target_table STRING)
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
    var file_format = SOURCE_FILE_FORMAT
    var supported_file_formats = "parquet, avro, orc"
    var supported_file_formats_array = supported_file_formats.split(",")
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
                               FILE_FORMAT=>'COMMON.PUBLIC.FILE_FORMAT_PARQUET_DEFAULT' ))order by 1";
    var cols_in_statement = snowflake.createStatement( {sqlText: cols_in_command} );
    var cols_in_details = cols_in_statement.execute();

// Make a list of current column names for comparison
    var cols_curr_list ="";
    while(cols_curr.next()){
        cols_curr_list = cols_curr_list+cols_curr.getColumnValue(1)+",";
    }
    var cols_curr_array = cols_curr_list.split(',');

// Make a list of incoming column names for comparison
    var cols_in_list ="";
    while (cols_in_details.next())  {
       cols_in_list = cols_in_list+cols_in_details.getColumnValue(1)+",";
    }
    var cols_in_array = cols_in_list.split(',');

//Check if the table exists.
    if(!cols_curr_list){
        return "Target Table Does Not Exist"
    }
    else{
    
// If the table does already exist, first check if existing and incoming fields match      
// 1. Loop through the existing column names to check if the exist in the incoming schema and fail if they don't
      for(var i = 0; i < cols_curr_array.length-1; i++) {
        if (cols_in_list.toUpperCase().indexOf(cols_curr_array[i].toUpperCase()) == -1){
           return "Existing Column Missing from Incoming Schema: "+cols_curr_array[i]
         }
      }
// 2. Loop through the incoming column names to check if they already exist and fail if they don't. 
      for(var i = 0; i < cols_in_array.length-1; i++) {
        if (cols_curr_list.toUpperCase().indexOf(cols_in_array[i].toUpperCase()) == -1){
           return "Incoming Column Missing from Existing Schema: "+cols_in_array[i]
         }
      }
    }

    

//Return a moderately helpful message
    return "Done"
  $$
  ; 
DROP TABLE COMMON.PUBLIC.TEST_TABLE;
call common.public.migrate_s3_table('COMMON.PUBLIC.USWEST2_S3_EXTSTG','guidewire_cda/bc_chargept_nm_l10n/','Parquet','COMMON','PUBLIC','TEST_TABLE');
call common.public.auto_load_s3_table('COMMON.PUBLIC.USWEST2_S3_EXTSTG','guidewire_cda/bc_chargept_nm_l10n/','Parquet','COMMON','PUBLIC','TEST_TABLE');