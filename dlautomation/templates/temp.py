
import logging

logging.basicConfig(format='%(asctime)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S', level=logging.INFO)

class Temp:

    @staticmethod
    def get_temp(table_meta_data_results):
        temp = None
        for result in table_meta_data_results:
            temp = "\n----------------------------------------------------------------------------------------------------------" \
                   "----------------------------------------------------------------------------------------------------------\n" \
                   "%20s%15s%30s%20s%15s"%("Source Table Name:",result["source_table_name"],"","Target Table Name:",result["target_table_name"])
            temp += "\n"
            temp += "%35s%30s%20s%15s"%("Column Names Comparison","","Test Status",result["column_names_test_status"])
            temp += "\n" \
                    "%20s%20s%20s"%("Source Column Name","","Target Column Name")
            for column in result["column_names"]:
                temp += "%20s%20s%20s"%(column["source_column_name"],"",column["target_column_name"])
                temp += "\n"
            temp += "%35s%30s%20s%15s"%("Data Types Comparison","","Test Status",result["data_types_test_status"])
            temp += "\n" \
                    "%20s%20s%20s%20s%20s%20s%20s"%("Source Column Name","","Source Column Data Type","","Target Column Name","","Target Column Data Types")
            for datatype in result["data_types"]:
                temp += "%20s%20s%20s%20s%20s%20s%20s"%(datatype["source_column_name"],"",datatype["target_datatype"],"",datatype["target_column_name"],"",datatype["expected_datatype"])

            logging.info(temp)
