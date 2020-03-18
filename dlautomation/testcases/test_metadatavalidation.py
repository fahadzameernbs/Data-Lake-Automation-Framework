from managers.metadatacomparison import MetaDataComparison

import logging


logging.basicConfig(format='%(asctime)s - %(message)s', datefmt='%d-%b-%y', filename='../samplelogs.log', level=logging.INFO)


def test_metadatavalidation():
    table_meta_data_results = MetaDataComparison.compare_metadata()
    for result in table_meta_data_results:
        logging.info("\n-----------------------------------------------------------------------------------------------------"
              "----------------------------------------------------------------------------------------------------\n")
        logging.info("%20s%15s%30s%20s%15s"%("Source Table Name:",result["source_table_name"],"","Target Table Name:",result["target_table_name"]))
        logging.info("\n")
        logging.info("%35s%30s%20s%15s"%("Column Names Comparison","","Test Status",result["column_names_test_status"]))
        logging.info("\n")
        logging.info("%20s%20s%20s"%("Source Column Name","","Target Column Name"))
        for column in result["column_names"]:
            logging.info("%20s%20s%20s"%(column["source_column_name"],"",column["target_column_name"]))
        logging.info("\n")
        logging.info("%35s%30s%20s%15s"%("Data Types Comparison","","Test Status",result["data_types_test_status"]))
        logging.info("\n")
        logging.info("%20s%20s%20s%20s%20s%20s%20s"%("Source Column Name","","Source Column Data Type","","Target Column Name","","Target Column Data Types"))
        for datatype in result["data_types"]:
            logging.info("%20s%20s%20s%20s%20s%20s%20s"%(datatype["source_column_name"],"",datatype["target_datatype"],"",datatype["target_column_name"],"",datatype["expected_datatype"]))

test_metadatavalidation()
