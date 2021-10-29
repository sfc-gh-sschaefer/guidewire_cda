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
    var cols_in_details = cols_in_statement.execute();

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