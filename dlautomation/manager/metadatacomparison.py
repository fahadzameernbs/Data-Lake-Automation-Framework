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
    def get_filtered_list(source_table, source_columns, source_excluded_columns, target_table, target_columns, target_excluded_columns):
        """
        Description: To exclude the particular columns from the source and target tables and prepare the filtered columns list
        Parameters:  Accepts 6 following parameters:
                     1- source_table: to display the source table name just in case if nominated excluded column not found from source table columns list
                     2- source_columns: all the source table columns list
                     3- source_excluded_columns: particular columns which needs to be excluded from the source table columns list
                     4- target_table: to display the target table name just in case if nominated excluded column not found from target table columns list
                     5- target_columns: all the target table columns list
                     6- target_excluded_columns: particular columns which needs to be excluded from the target table columns list
        Return:      filtered source_columns and target_columns list
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
                sys.exit("Table:  {0}\nExluded Column '{1}' not found in the following source table".format(source_table, excluded_col))

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
                sys.exit("Table:  {0}\nExluded Column '{1}' not found in the following target table".format(target_table, excluded_col))
        return source_columns, target_columns

    @staticmethod
    def endpoints_metadata(row):
        """
        :param
        :return:
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
        filtered_source_columns, filtered_target_columns = MetaDataComparison.get_filtered_list(source_table, source_columns, source_excluded_columns, target_table, target_columns, target_excluded_columns)
        return follow_columns_mapping, filtered_source_columns, filtered_target_columns

    @staticmethod
    def get_mysql_table_metadata(schema_name, table_name):
        mysql_columns = []
        query_string = QueryBuilder.get_mysql_metadata_query(schema_name, table_name)
        query_result = Mysql.get_mysql_query_output(query_string)
        for row in query_result:
            metadata_dic = {}
            metadata_dic["Name"] = row[0]
            metadata_dic["Type"] = row[1]
            mysql_columns.append(metadata_dic)
        return mysql_columns

    @staticmethod
    def get_athena_table_metadata(schema_name, table_name):
        athena_table_info = Glue.get_table_info(schema_name, table_name)
        athena_columns = athena_table_info['Table']['StorageDescriptor']['Columns']
        return athena_columns

    @staticmethod
    def get_columns_mapping(row):
        columns_mapping = {}
        table_id = row[0]
        query_string = QueryBuilder.get_columns_mapping_query(params["mysql_db_name"], params["mysql_athena_columns_mapping_table"], table_id)
        query_result = Mysql.get_mysql_query_output(query_string)
        for row in query_result:
            columns_mapping[row[1]] = row[2]
        return columns_mapping

    @staticmethod
    def compare_metadata():
        test_status_flag = True
        mismatch_cols = []
        mismatch_datatypes = []
        active_tables = QueryResult.get_active_tables()
        datatype_mapping = QueryResult.get_dt_mapping()
        #getting active tables
        for row in active_tables:
            #getting endpoints selection and metadata
            follow_columns_mapping, source_columns, target_columns = MetaDataComparison.endpoints_metadata(row)
            a = source_columns
            b = target_columns
            #getting columns from source and target tables
            for index in range(len(source_columns)):
                source_column_name = source_columns[index]["Name"]
                source_column_dt = source_columns[index]["Type"]
                target_column_name = target_columns[index]["Name"]
                target_column_dt = target_columns[index]["Type"]
                #check if follow columns mapping flag is true
                if follow_columns_mapping == "yes":
                    columns_mapping = MetaDataComparison.get_columns_mapping(row)
                    mapping_target_column_name = columns_mapping.get(source_column_name)
                    if mapping_target_column_name != target_column_name:
                        test_status_flag = False
                        mismatch_metadata = {}
                        mismatch_metadata["source_column"] = mapping_target_column_name
                        mismatch_metadata["target_column"] = target_column_name
                        mismatch_cols.append(mismatch_metadata)
                else:
                    #column names comparison
                    if source_column_name != target_column_name:
                        test_status_flag = False
                        mismatch_metadata = {}
                        mismatch_metadata["source_column"] = source_column_name
                        mismatch_metadata["target_column"] = target_column_name
                        mismatch_cols.append(mismatch_metadata)
                #column datatypes comparison
                expected_dt = datatype_mapping.get(source_column_dt)
                if target_column_dt != expected_dt:
                    test_status_flag = False
                    mismatch_metadata = {}
                    mismatch_metadata["source_column"] = source_column_name
                    mismatch_metadata["target_column"]= target_column_name
                    mismatch_metadata["actual_datatype"] = target_column_dt
                    mismatch_metadata["expected_datatype"] = expected_dt
                    mismatch_datatypes.append(mismatch_metadata)
        return test_status_flag, mismatch_cols, mismatch_datatypes
