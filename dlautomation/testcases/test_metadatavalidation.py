from managers.metadatacomparison import MetaDataComparison


def test_metadatavalidation():
    table_meta_data_results = MetaDataComparison.compare_metadata()
    for result in table_meta_data_results:
        print("\n---------------------------------------------------------------------------------------------------------------------\n")
        print("%20s%15s%30s%20s%15s"%("Source Table Name:",result["source_table_name"],"","Target Table Name:",result["target_table_name"]))
        print("\n")
        print("%35s%30s%20s%15s"%("Column Names Comparison","","Test Status",result["column_names_test_status"]))
        print("\n")
        print("%20s%20s%20s"%("Source Column Name","","Target Column Name"))
        for column in result["column_names"]:
            print("%20s%20s%20s"%(column["source_column_name"],"",column["target_column_name"]))
        print("\n")
        print("%35s%30s%20s%15s"%("Data Types Comparison","","Test Status",result["data_types_test_status"]))
        print("\n")
        print("%20s%20s%20s%20s%20s%20s%20s"%("Source Column Name","","Source Column Data Type","","Target Column Name","","Target Column Data Types"))
#        for column in result["column_names"]:
#            print("%20s%20s%20s"%(column["source_column_name"],"",column["target_column_name"]))



test_metadatavalidation()
