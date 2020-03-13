
class QueryBuilder:

    @staticmethod
    def get_count_query(db_name, table_name):
        query = "select count(*) from {0}.{1}".format(db_name, table_name)
        return query

    @staticmethod
    def get_active_tables_query(db_name, table_name):
        query = "select * from {0}.{1} where is_active = 'yes'".format(db_name,table_name)
        return query

    @staticmethod
    def get_all_records_query(db_name, table_name):
        query = "select * from {0}.{1}".format(db_name,table_name)
        return query

    @staticmethod
    def get_mysql_metadata_query(db_name, table_name):
        query = "select COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH from information_schema.columns where table_schema = '{0}' and table_name = '{1}' order by ORDINAL_POSITION".format(db_name, table_name)
        return query

    @staticmethod
    def get_datatypes_mapping_query(db_name, table_name, source_name, target_name):
        query = "select {0}, {1} from {2}.{3}".format(source_name, target_name, db_name, table_name)
        return query

    @staticmethod
    def get_columns_mapping_query(db_name, table_name, mapping_id):
        query = "select * from {0}.{1} where mapping_id = {2}".format(db_name, table_name, mapping_id)
        return query
