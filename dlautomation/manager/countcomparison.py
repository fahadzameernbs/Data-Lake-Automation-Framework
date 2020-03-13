from resources.config import get_param
from common.querybuilder import QueryBuilder
from common.queryresult import QueryResult
from services.mysql import Mysql
from services.athena import Athena
import pandas as pd

params = get_param("../resources/configurations.ini","CONFIGURATIONS")

class CountComparison:

    @staticmethod
    def endpoints_count(row):
        source_count = None
        target_count = None
        source_schema = row[1]  #fetching source schema
        source_table = row[2]   #fetching source table
        target_schema = row[3]  #fetching target schema
        target_table = row[4]   #fetching target table
        #source endpoint selection
        if params["source_endpoint"] == "mysql":
            source_count = CountComparison.get_mysql_table_count(source_schema, source_table)
        elif params["source_endpoint"] == "athena":
            source_count = CountComparison.get_athena_table_count(source_schema, source_table)
        #target endpoint selection
        if params["target_endpoint"] == "mysql":
            target_count = CountComparison.get_mysql_table_count(target_schema, target_table)
        elif params["target_endpoint"] == "athena":
            target_count = CountComparison.get_athena_table_count(target_schema, target_table)
        return source_table, source_count, target_table, target_count

    @staticmethod
    def get_mysql_table_count(schema_name, table_name):
        query_string = QueryBuilder.get_count_query(schema_name, table_name)
        resultset = Mysql.get_mysql_query_output(query_string)
        return resultset[0][0]

    @staticmethod
    def get_athena_table_count(schema_name, table_name):
        query_string = QueryBuilder.get_count_query(schema_name, table_name)
        fileobj = Athena.get_query_result(params["glue_db_name"], query_string, params["s3_athena_output_location_path"], params["s3_bucket_name"])
        file_body = pd.read_csv(fileobj['Body'])
        resultset = pd.DataFrame(file_body)
        return resultset.values[0][0]

    @staticmethod
    def compare_counts():
        mismatch_counts_list = []
        active_tables = QueryResult.get_active_tables()
        #getting active tables
        for row in active_tables:
            test_status = "Pass"
            #getting endpoints selection and counts
            source_table, source_count, target_table, target_count = CountComparison.endpoints_count(row)
            if source_count != target_count:
                test_status = "Fail"
            mismatch_count_dic = {}
            mismatch_count_dic["test_status"] = test_status
            mismatch_count_dic["source_table"] = source_table
            mismatch_count_dic["source_count"] = source_count
            mismatch_count_dic["target_table"] = target_table
            mismatch_count_dic["target_count"] = target_count
            mismatch_counts_list.append(mismatch_count_dic)
        return mismatch_counts_list

