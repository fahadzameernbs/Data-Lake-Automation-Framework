from managers.countcomparison import CountComparison


def test_countsvalidation():
    table_count_results = CountComparison.compare_counts()
    for result in table_count_results:
        print("\n-----------------------------------------------------------------------------------------------------------------------------------------\n")
        print("Source Table: "+result["source_table"]+"      ||      Target Table: "+result["target_table"]+"      ||      Test Status: "+result["test_status"])
        print("Source Table Count: "+str(result["source_table_count"])+"      ||      Target Count: "+str(result["target_table_count"]))

test_countsvalidation()
