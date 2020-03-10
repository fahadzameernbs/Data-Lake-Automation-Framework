from manager.countcomparison import CountComparison


def test_countsvalidation():
    test_status_flag, mismatchcounts = CountComparison.compare_counts()
    print(test_status_flag)
    if mismatchcounts:
        table_name = None
        for mismatch_col in mismatchcounts:
            if table_name is None or table_name != mismatchcounts["table_name"]:
                table_name = mismatchcounts["table_name"]
                print("\n--------------------------------------"+mismatchcounts["table_name"]+"--------------------------------------\n")
            print(mismatch_col["source_column"]+"   |   "+mismatch_col["target_column"])


test_countsvalidation()
