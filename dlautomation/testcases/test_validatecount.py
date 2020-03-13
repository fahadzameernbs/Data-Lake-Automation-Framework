from manager.countcomparison import CountComparison


def test_countsvalidation():
    mismatch_counts_list = CountComparison.compare_counts()
    for mismatch_col in mismatch_counts_list:
        print("\n-----------------------------------------------------------------------------------------------------------------------------------------\n")
        print("Source Table: "+mismatch_col["source_table"]+"      ||      Target Table: "+mismatch_col["source_table"]+"      ||      Test Status: "+mismatch_col["test_status"])
        print("Source Count: "+str(mismatch_col["source_count"])+"      ||      Target Count: "+str(mismatch_col["target_count"]))

test_countsvalidation()
