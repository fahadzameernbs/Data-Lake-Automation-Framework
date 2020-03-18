
class QueryBuilder:

    @staticmethod
    def get_count_query(db_name, table_name):
        """
        Description: preparing the query string to fetch the count
        Parameters:  db_name (to select the particular data base)
                     table_name (to select the particular table under particular data base)
        Return:      prepared query string
        Exception:   none
        """
        query_string = "select count(*) from {0}.{1}".format(db_name, table_name)
        return query_string

    @staticmethod
    def get_active_tables_query(db_name, table_name):
        """
        Description: preparing the query string to fetch all the active tables
        Parameters:  db_name (to select the particular data base)
                     table_name (to select the particular table under particular data base)
        Return:      prepared query string
        Exception:   none
        """
        query_string = "select * from {0}.{1} where is_active = 'yes'".format(db_name,table_name)
        return query_string

    @staticmethod
    def get_all_records_query(db_name, table_name):
        """
        Description: preparing the query string to fetch all the records
        Parameters:  db_name (to select the particular data base)
                     table_name (to select the particular table under particular data base)
        Return:      prepared query string
        Exception:   none
        """
        query_string = "select * from {0}.{1}".format(db_name,table_name)
        return query_string

    @staticmethod
    def get_mysql_metadata_query(db_name, table_name):
        """
        Description: preparing the query string to fetch the meta data of mysql table
        Parameters:  db_name (to select the particular data base)
                     table_name (to select the particular table under particular data base)
        Return:      prepared query string
        Exception:   none
        """
        query_string = "select COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH from information_schema.columns " \
                       "where table_schema = '{0}' and table_name = '{1}' order by ORDINAL_POSITION"\
                       .format(db_name, table_name)
        return query_string

    @staticmethod
    def get_datatypes_mapping_query(db_name, table_name, source_name, target_name):
        """
        Description: preparing the query string to fetch the data types mapping for selective endpoints
        Parameters:  db_name (to select the particular data base)
                     table_name (to select the particular table under particular data base)
                     source_name (to select the column for source endpoint)
                     target_name (to select the column for target endpoint)
        Return:      prepared query string
        Exception:   none
        """
        query_string = "select {0}, {1} from {2}.{3}".format(source_name, target_name, db_name, table_name)
        return query_string

    @staticmethod
    def get_columns_mapping_query(db_name, table_name, mapping_id):
        """
        Description: preparing the query string to fetch the columns mapping for selective tables
        Parameters:  db_name (to select the particular data base)
                     table_name (to select the particular table under particular data base)
                     mapping_id (to fetch the columns mapping against source and target endpoints mapping)
        Return:      prepared query string
        Exception:   none
        """
        query_string = "select * from {0}.{1} where mapping_id = {2}".format(db_name, table_name, mapping_id)
        return query_string
