from managers.countcomparison import CountComparison


def test_countsvalidation():
    table_count_results = CountComparison.compare_counts()
    for result in table_count_results:
        print("\n-----------------------------------------------------------------------------------------------------"
              "----------------------------------------------------------------------------------------------------\n")
        print("%20s%15s%30s%20s%15s%30s%20s%15s"%("Source Table Name:",result["source_table_name"],"","Target Table Name:",result["target_table_name"],"","Test Status",result["test_status"]))
        print("\n")
        print("%30s%15s%30s%30s%15s"%("Source Table Count","",str(result["source_table_count"]),"","Target Table Count"),"",str(result["target_table_count"]))

test_countsvalidation()
