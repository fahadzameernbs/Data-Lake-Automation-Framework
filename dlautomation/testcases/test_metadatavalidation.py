from manager.metadatacomparison import MetaDataComparison


def test_metadatavalidation():
    test_status, mismatch_cols, mismatch_datatypes = MetaDataComparison.compare_metadata()
    print(test_status)
    if mismatch_cols:
        print("\nColumn Names Mismatches Found")
        table_name = None
        for mismatch_col in mismatch_cols:
            """
            if table_name is None or table_name != mismatch_col["table_name"]:
                table_name = mismatch_col["table_name"]
                print("\n--------------------------------------"+mismatch_col["table_name"]+"--------------------------------------\n")
            """
            print(mismatch_col["source_column"]+"   |   "+mismatch_col["target_column"])
    if mismatch_datatypes:
        a = mismatch_datatypes
        print("\nColumns Datatypes Mismatches Found")
        table_name = None
        for mismatch_datatype in mismatch_datatypes:
            """
            if table_name is None or table_name != mismatch_datatype["table_name"]:
                table_name = mismatch_datatype["table_name"]
                print("\n--------------------------------------"+mismatch_datatype["table_name"]+"--------------------------------------\n")
            """
            print(mismatch_datatype["source_column"]+"  |  "+mismatch_datatype["actual_datatype"]+"   |    "+mismatch_datatype["target_column"]+"   |    "+mismatch_datatype["expected_datatype"])



test_metadatavalidation()
