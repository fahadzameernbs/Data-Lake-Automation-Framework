from services.mysql import Mysql
from common.querybuilder import QueryBuilder
from resources.config import get_param

params = get_param("../resources/configurations.ini","CONFIGURATIONS")

class QueryResult:

    active_tables = None               #preparing active tables list
    datatypes_mapping = {}             #preparing datatypes mapping dictionary

    @staticmethod
    def get_active_tables():
        """
        Description: to fetch all the active tables
        Parameters:  none
        Return:      active_tables (list of all the active tables)
        Exception:
        """
        if not bool(QueryResult.active_tables):
            query_string = QueryBuilder.get_active_tables_query(params["mysql_db_name"], params["mysql_tables_metadata_table"])
            QueryResult.active_tables = Mysql.get_mysql_query_output(query_string)
        return QueryResult.active_tables

    @staticmethod
    def get_dt_mapping():
        """
        Description: to fetch the data types mapping across different data bases
        Parameters:  none
        Return:      datatypes_mapping (list of data types mapping of across different data bases)
        Exception:
        """
        if not bool(QueryResult.datatypes_mapping):
            query_string = QueryBuilder.get_datatypes_mapping_query(params["mysql_db_name"], params["datatypes_mapping_table"], params["source_endpoint"], params["target_endpoint"])
            query_result = Mysql.get_mysql_query_output(query_string)
            for row in query_result:
                QueryResult.datatypes_mapping[row[0]] = row[1]
        return QueryResult.datatypes_mapping

