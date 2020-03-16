import sys

from resources.config import get_param
from common.querybuilder import QueryBuilder
from common.queryresult import QueryResult
from services.glue import Glue
from services.mysql import Mysql
from resources.constants import Constants

params = get_param("../resources/configurations.ini","CONFIGURATIONS")

class MetaDataComparison:

    @staticmethod
    def endpoints_metadata(row):
        """
        Description: to select the source and terget end points on the basis of configurations and fetch the source and
                     target endpoints meta data by invoking different methods (get_mysql_table_metadata,
                     get_athena_table_metadata, get_filtered_list)
        Parameters:  row (active table row to fetch the scource_schema, source_table, target_schema, target_table,
                          excluded columns from source table and target table and columns mapping flag)
        Return:      1) follow_columns_mapping (either follow the columns mapping for the table or not)
                     2) filtered_source_columns and filtered_target_columns after excluding the nominated columns
        Exceptions:
        """
        source_columns = None
        target_columns = None
        source_schema = row[1]                              #fetching source schema
        source_table = row[2]                               #fetching source table
        target_schema = row[3]                              #fetching target schema
        target_table = row[4]                               #fetching target table
        source_excluded_columns = row[5].split(',')         #fetching excluded columns from source table
        target_excluded_columns = row[6].split(',')         #fetching excluded columns from target table
        follow_columns_mapping = row[7]                     #fetching columns mapping flag for active table
        #source endpoint selection
        if params["source_endpoint"] == Constants.mysql_endpoint:
            source_columns = MetaDataComparison.get_mysql_table_metadata(source_schema, source_table)
        elif params["source_endpoint"] == Constants.athena_endpoint:
            source_columns = MetaDataComparison.get_athena_table_metadata(source_schema, source_table)
        #target endpoint selection
        if params["target_endpoint"] == Constants.mysql_endpoint:
            target_columns = MetaDataComparison.get_mysql_table_metadata(target_schema, target_table)
        elif params["target_endpoint"] == Constants.athena_endpoint:
            target_columns = MetaDataComparison.get_athena_table_metadata(target_schema, target_table)
        #calling get_filtered_list method to exclude the nominated columns from the source columns and
        #target columns list
        filtered_source_columns, filtered_target_columns = MetaDataComparison.get_filtered_list(source_table,
                                                                                                source_columns,
                                                                                                source_excluded_columns,
                                                                                                target_table,
                                                                                                target_columns,
                                                                                                target_excluded_columns)
        return follow_columns_mapping, source_table, filtered_source_columns, target_table, filtered_target_columns

    @staticmethod
    def get_mysql_table_metadata(schema_name, table_name):
        """
        Description: to fetch the meta data of particlar mysql table
        parameters:  1) schema_name (to select the particular schema)
                     2) table_name (to select the particaular table under particular schema)
        Return:      mysql_table_columns (mysql table columns with column name and data type)
        Exceptions:
        """
        mysql_table_columns = []
        query_string = QueryBuilder.get_mysql_metadata_query(schema_name, table_name)
        query_result = Mysql.get_mysql_query_output(query_string)
        #to fetch the columns name and data type from the query result set
        for row in query_result:
            metadata_dic = {}
            metadata_dic["Name"] = row[0]
            metadata_dic["Type"] = row[1]
            mysql_table_columns.append(metadata_dic)
        return mysql_table_columns

    @staticmethod
    def get_athena_table_metadata(schema_name, table_name):
        """
        Description: to fetch the meta data of particlar athena table
        parameters:  1) schema_name (to select the particular schema)
                     2) table_name (to select the particaular table under particular schema)
        Return:      athena_columns (athena table columns with column name and data type)
        Exceptions:
        """
        athena_table_info = Glue.get_table_info(schema_name, table_name)
        athena_table_columns = athena_table_info['Table']['StorageDescriptor']['Columns']
        return athena_table_columns

    @staticmethod
    def get_filtered_list(source_table, source_columns, source_excluded_columns, target_table, target_columns,
                          target_excluded_columns):
        """
        Description: To exclude the nominated columns from the source and target columns list and prepare the filtered
                     columns list
        Parameters:  Accepts 6 following parameters:
                     1- source_table: to display the source table name just in case if nominated excluded column not
                        found from source table columns list
                     2- source_columns: all the source table columns list
                     3- source_excluded_columns: particular columns which needs to be excluded from the source table
                        columns list
                     4- target_table: to display the target table name just in case if nominated excluded column not
                        found from target table columns list
                     5- target_columns: all the target table columns list
                     6- target_excluded_columns: particular columns which needs to be excluded from the target table
                        columns list
        Return:      filtered source_columns and target_columns list after excluding the nominated columns
        Exceptions:
        """

        #exclude the nominated columns from the source columns list
        for excluded_col in source_excluded_columns:
            flag = False
            for col in source_columns:
                col_name = col["Name"]
                if excluded_col == col_name:
                    source_columns.remove(col)
                    flag = True
                    break
            if flag is False:
                sys.exit("Table:  {0}\nExluded Column '{1}' not found in the following source table"
                         .format(source_table, excluded_col))

        #exclude the nominated columns from the source columns list
        for excluded_col in target_excluded_columns:
            flag = False
            for col in target_columns:
                col_name = col["Name"]
                if excluded_col == col_name:
                    target_columns.remove(col)
                    flag = True
                    break
            if flag is False:
                sys.exit("Table:  {0}\nExluded Column '{1}' not found in the following target table"
                         .format(target_table, excluded_col))
        return source_columns, target_columns

    @staticmethod
    def get_columns_mapping(table_id):
        """
        Description: to fetch the columns mapping of particlar table
        parameters:  table_id (particular table id whose we fetch the columns mapping from columns_mapping_table)
        Return:      columns_mapping (particular table columns mapping)
        Exceptions:
        """
        columns_mapping = {}
        query_string = QueryBuilder.get_columns_mapping_query(params["mysql_db_name"],
                                                              params["mysql_athena_columns_mapping_table"], table_id)
        query_result = Mysql.get_mysql_query_output(query_string)
        for row in query_result:
            columns_mapping[row[1]] = row[2]
        return columns_mapping

    @staticmethod
    def compare_metadata():
        """
        Description: to compare the meta data of source table and target table by invoking different methods
                     (get_active_tables, get_dt_mapping, endpoints_metadata, get_columns_mapping)
        parameters:  none
        Return:      table_meta_data_results (final test case results includes test_status, source_table,
                     source table column names, source table data types, target_table, target table columns,
                     target table data types)
        Exceptions:
        """
        table_meta_data_results = []
        #getting all the active tables
        active_tables = QueryResult.get_active_tables()
        #getting data types mapping across different data bases
        datatype_mapping = QueryResult.get_dt_mapping()
        #getting active tables
        for row in active_tables:
            metadata_dic = {}
            column_names = []
            data_types = []
            column_names_test_status = "Pass"
            data_types_test_status = "Pass"
            #getting endpoints selection and metadata
            follow_columns_mapping, source_table, source_columns, target_table, target_columns = MetaDataComparison.\
                                                                                                endpoints_metadata(row)
            #getting columns from source and target tables
            for index in range(len(source_columns)):
                column_names_dic = {}
                data_types_dic = {}
                source_column_name = source_columns[index]["Name"]          #fetching source column name
                source_column_dt = source_columns[index]["Type"]            #fetching source column data type
                target_column_name = target_columns[index]["Name"]          #fetching target column name
                target_column_dt = target_columns[index]["Type"]            #fetching target data type
                #check if follow columns mapping flag is yes
                if follow_columns_mapping == "yes":
                    table_id = row[0]
                    #getting columns mapping against particular table id
                    columns_mapping = MetaDataComparison.get_columns_mapping(table_id)
                    mapping_target_column_name = columns_mapping.get(source_column_name)
                    #check if column name mismatch found (if follow columns mapping flag is yes)
                    if mapping_target_column_name != target_column_name:
                        column_names_test_status = "Fail"
                    column_names_dic["source_column_name"] = mapping_target_column_name
                    column_names_dic["target_column_name"] = target_column_name
                    column_names.append(column_names_dic)
                else:
                    #check if column name mismatch found (if follow columns mapping flag is no)
                    if source_column_name != target_column_name:
                        column_names_test_status = "Fail"
                    column_names_dic["source_column_name"] = source_column_name
                    column_names_dic["target_column_name"] = target_column_name
                    column_names.append(column_names_dic)
                #check if column data type mismatch found
                expected_dt = datatype_mapping.get(source_column_dt)
                if target_column_dt != expected_dt:
                    data_types_test_status = "Fail"
                data_types_dic["source_column_name"] = source_column_name
                data_types_dic["target_column_name"]= target_column_name
                data_types_dic["target_datatype"] = target_column_dt
                data_types_dic["expected_datatype"] = expected_dt
                data_types.append(data_types_dic)
            metadata_dic["source_table_name"] = source_table
            metadata_dic["target_table_name"] = target_table
            metadata_dic["column_names_test_status"] = column_names_test_status
            metadata_dic["data_types_test_status"] = data_types_test_status
            metadata_dic["column_names"] = column_names
            metadata_dic["data_types"] = data_types
            table_meta_data_results.append(metadata_dic)
        return table_meta_data_results
