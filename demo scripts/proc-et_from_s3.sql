create or replace procedure guidewire_cda.public.et_from_s3(source_stage STRING, source_prefix STRING, source_file_format STRING, target_db STRING,target_schema STRING, target_table STRING)
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

// Grab the external table column expressions
    var col_expressions_command = "SELECT GENERATE_COLUMN_DESCRIPTION(ARRAY_AGG(OBJECT_CONSTRUCT(*)), 'external_table') AS COLUMNS \
                            FROM TABLE ( INFER_SCHEMA( LOCATION=>'@"+stage+"/"+prefix+"' , \
                               FILE_FORMAT=>'COMMON.PUBLIC.FILE_FORMAT_PARQUET_DEFAULT' ))";
    var col_expressions_statement = snowflake.createStatement( {sqlText: col_expressions_command});
    var col_expressions = col_expressions_statement.execute();
    //create a new expressions string which skips the 'value' line
    col_expressions.next();
    var col_expressions_text = col_expressions.getColumnValue(1);
    var col_expressions_arr = col_expressions_text.split('\n');
    var col_expressions_no_value = '';
    for (let i=0;i < col_expressions_arr.length; i++) {
        if (col_expressions_arr[i].indexOf("value") == -1){
            col_expressions_no_value += col_expressions_arr[i]+'\n';
        }
    }

// Create the statement to create the simple semi-structured external table
var create_et_command = "CREATE OR REPLACE EXTERNAL TABLE GUIDEWIRE_CDA.LANDING."+table_nm+"_et \
( filename string AS METADATA$FILENAME) \
LOCATION = @GUIDEWIRE_CDA.LANDING.s3_extstg_guidewire/"+prefix+"  \
REFRESH_ON_CREATE =  TRUE \
AUTO_REFRESH = TRUE \
PATTERN = '.*."+file_format+"' \
FILE_FORMAT = (type = "+file_format+")";

var create_et_statement = snowflake.createStatement( {sqlText: create_et_command});
var create_et = create_et_statement.execute();

// Create the statement to create the structured external table
create_table_command = "create or replace external table "+fqtn+"_et(";
create_table_command += col_expressions_no_value.toUpperCase()+'\n';
create_table_command +=") LOCATION = @GUIDEWIRE_CDA.LANDING.s3_extstg_guidewire/bc_chargept_nm_l10n/  \
                                                REFRESH_ON_CREATE =  TRUE \
                                                AUTO_REFRESH = TRUE \
                                                PATTERN = '.*."+file_format+"' \
                                                FILE_FORMAT = (type = "+file_format+")";
var create_table_statement = snowflake.createStatement( {sqlText: create_table_command});
var create_table = create_table_statement.execute();

//Return a moderately helpful message
  if(create_table){
    return "Created Table "+fqtn+"_et \n -------------------------------- \n"+create_table_command;
  }
  else{
    return "Could not create table: "+fqtn+"_et"
  }
  $$
  ; 

/* Example Usage 
call guidewire_cda.public.et_from_s3('GUIDEWIRE_CDA.LANDING.s3_extstg_guidewire','bc_chargept_nm_l10n/','Parquet','GUIDEWIRE_CDA','RAW','BC_CHARGEPT_NM_L10N');
*/

SELECT 'Done' as Status;