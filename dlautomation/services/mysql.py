import mysql.connector
from services.systemsmanager import SystemsManager
from resources.config import get_param

params = get_param("../resources/configurations.ini","CONFIGURATIONS")

class Mysql:

    @staticmethod
    def get_mysql_query_output(query_string):
        credentials = SystemsManager.get_parameter_jr(params["credentails_key"], True)
        db_con = mysql.connector.connect(host=credentials["Host"], user=credentials["User"], passwd=credentials["Password"])
        cursor = db_con.cursor()
        cursor.execute(query_string)
        query_result = cursor.fetchall()
        db_con.close()
        return query_result
