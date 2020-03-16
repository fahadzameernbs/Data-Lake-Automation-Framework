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
        """
        Description: to select the source and terget end points on the basis of configurations and fetch the source and
                     target endpoints counts by invoking different methods (get_mysql_table_count,
                     get_athena_table_count)
        Parameters:  row (active table row to fetch the scource_schema, source_table, target_schema, target_table)
        Return:      1) source_table (source table name)
                     2) target_table_count (source table count)
                     3) target_table (target table name)
                     4) target_table_count (target table count)
        Exceptions:
        """
        source_table_count = None
        target_table_count = None
        source_schema = row[1]  #fetching source schema
        source_table = row[2]   #fetching source table
        target_schema = row[3]  #fetching target schema
        target_table = row[4]   #fetching target table
        #source endpoint selection
        if params["source_endpoint"] == "mysql":
            source_table_count = CountComparison.get_mysql_table_count(source_schema, source_table)
        elif params["source_endpoint"] == "athena":
            source_table_count = CountComparison.get_athena_table_count(source_schema, source_table)
        #target endpoint selection
        if params["target_endpoint"] == "mysql":
            target_table_count = CountComparison.get_mysql_table_count(target_schema, target_table)
        elif params["target_endpoint"] == "athena":
            target_table_count = CountComparison.get_athena_table_count(target_schema, target_table)
        return source_table, source_table_count, target_table, target_table_count

    @staticmethod
    def get_mysql_table_count(schema_name, table_name):
        """
        Description: to fetch the count of particlar mysql table
        parameters:  1) schema_name (to select the particular schema)
                     2) table_name (to select the particaular table under particular schema)
        Return:      mysql_table_count (mysql table count)
        Exceptions:
        """
        query_string = QueryBuilder.get_count_query(schema_name, table_name)
        resultset = Mysql.get_mysql_query_output(query_string)
        mysql_table_count = resultset[0][0]
        return mysql_table_count

    @staticmethod
    def get_athena_table_count(schema_name, table_name):
        """
        Description: to fetch the count of particlar athena table
        parameters:  1) schema_name (to select the particular schema)
                     2) table_name (to select the particaular table under particular schema)
        Return:      athena_table_count (athena table count)
        Exceptions:
        """
        query_string = QueryBuilder.get_count_query(schema_name, table_name)
        fileobj = Athena.get_query_result(params["glue_db_name"], query_string, params["s3_athena_output_location_path"]
                                          , params["s3_bucket_name"])
        file_body = pd.read_csv(fileobj['Body'])
        resultset = pd.DataFrame(file_body)
        athena_table_count = resultset.values[0][0]
        return athena_table_count

    @staticmethod
    def compare_counts():
        """
        Description: to compare the counts of source table and target table by invoking different methods
                     (get_active_tables, endpoints_count)
        parameters:  none
        Return:      table_count_results (final test case results includes test_status, source_table, source_table_count,
                     target_table, target_table_count)
        Exceptions:
        """
        table_count_results = []
        #getting all the active tables
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
            mismatch_count_dic["source_table_name"] = source_table
            mismatch_count_dic["source_table_count"] = source_count
            mismatch_count_dic["target_table_name"] = target_table
            mismatch_count_dic["target_table_count"] = target_count
            table_count_results.append(mismatch_count_dic)
        return table_count_results

