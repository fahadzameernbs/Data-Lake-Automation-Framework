from services.mysql import Mysql
from common.querybuilder import QueryBuilder
from resources.config import get_param

class QueryResult:

    active_tables = None
    dt_mapping = {}
    params = get_param("../resources/configurations.ini","CONFIGURATIONS")

    @staticmethod
    def get_active_tables():
        if(QueryResult.active_tables is None):
            query_string = QueryBuilder.get_active_tables_query(QueryResult.params["mysql_db_name"], QueryResult.params["mysql_tables_metadata_table"])
            QueryResult.active_tables = Mysql.get_mysql_query_output(query_string)
        return QueryResult.active_tables

    @staticmethod
    def get_dt_mapping():
        if not bool(QueryResult.dt_mapping):
            query_string = QueryBuilder.get_datatypes_mapping_query(QueryResult.params["mysql_db_name"], QueryResult.params["mysql_dt_mapping_table"], QueryResult.params["source_endpoint"], QueryResult.params["target_endpoint"])
            query_result = Mysql.get_mysql_query_output(query_string)
            for row in query_result:
                a = row
                QueryResult.dt_mapping[row[0]] = row[1]
        return QueryResult.dt_mapping

