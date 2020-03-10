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
        #get filter source list
        for excluded_col in source_excluded_columns:
            flag = False
            for col in source_columns:
                col_name = col["Name"]
                if excluded_col == col_name:
                    source_columns.remove(col)
                    flag = True
                    break
            if flag is False:
                sys.exit("Table:  {0} \n Exluded Col '{1}' not found in the following table".format(source_table, excluded_col))
        #get filter target list
        for excluded_col in target_excluded_columns:
            flag = False
            for col in target_columns:
                col_name = col["Name"]
                if excluded_col == col_name:
                    target_columns.remove(col)
                    flag = True
                    break
            if flag is False:
                sys.exit("Table:  {0} \n Exluded Col '{1}' not found in the following table".format(target_table, excluded_col))
        return source_columns, target_columns

    @staticmethod
    def endpoints_metadata(row):
        source_columns = None
        target_columns = None
        source_schema = row[1]  #fetching source schema
        source_table = row[2]   #fetching source table
        target_schema = row[3]  #fetching target schema
        target_table = row[4]   #fetching target table
        source_excluded_columns = row[5].split(',')   #fetching excluded columns from source table
        target_excluded_columns = row[6].split(',')   #fetching excluded columns from target table
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
        return filtered_source_columns, filtered_target_columns

    @staticmethod
    def get_mysql_table_metadata(schema_name, table_name):
        mysql_columns = []
        mysql_excluded_columns = []
        mysql_query_string = QueryBuilder.get_mysql_metadata_query(schema_name, table_name)
        mysql_result = Mysql.get_mysql_query_output(mysql_query_string)
        for row in mysql_result:
            md_dic = {}
            md_dic["Name"] = row[0]
            md_dic["Type"] = row[1]
            mysql_columns.append(md_dic)
        return mysql_columns

    @staticmethod
    def get_athena_table_metadata(schema_name, table_name):
        glue_md = Glue.get_table_metadata(schema_name, table_name)
        glue_columns = glue_md['Table']['StorageDescriptor']['Columns']
        return glue_columns

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
            source_columns, target_columns = MetaDataComparison.endpoints_metadata(row)
            #getting columns from source and target tables
            for index in range(len(source_columns)):
                source_column_name = source_columns[index]["Name"]
                source_column_dt = source_columns[index]["Type"]
                target_column_name = target_columns[index]["Name"]
                target_column_dt = target_columns[index]["Type"]
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
