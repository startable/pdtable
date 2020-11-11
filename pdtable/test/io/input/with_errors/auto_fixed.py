"""
    module containing auto-fixed input files from this directory
    need to be updated:
        1) when additional test-input are added
        2) when default ParseFixer are modified
        3) redo all.csv:
            rm -f all.csv
            cat *.csv > all.csv
    the file all.csv is not included here
"""
autoFixed = {
    "colname1.csv": """
        **farm_colname1;
        your_farm my_farm farms_galore
        species;-missing-;avg_weight
        text;-;kg
        chicken;2.0;3.0
        pig;4.0;39.0
        goat;4.0;-
        zybra;4.0;-
        cow;-;200.0
        goose;2.0;9.0
    """,
    "cols1.csv": """
        **farm_cols1;
        your_farm my_farm farms_galore
        species;num;flt;dt;flt_fixed_000;flt_fixed_001
        text;-;kg;datetime;kg;kg
        chicken;2.0;3.0;2020-07-01 00:00:00;3.21;1.0
        pig;4.0;39.0;2020-07-02 00:00:00;39.1;2.1
        goat;4.0;-;-;1.1;3.2
        zybra;4.0;-;-;2.1;4.3
        cow;-;200.0;-;200.2;5.4
        goose;2.0;9.0;-;9.1;6.5
        1234;-;-;-;7.11;7.6
    """,
    "ex0.csv": """
        **farm_animals0;
        your_farm my_farm farms_galore
        species;n_legs;avg_weight
        text;-;kg
        chicken;2.0;3.0
        pig;4.0;30.0
        goat;4.0;-
        zybra;4.0;-
        cow;-;200.0
        goose;2.0;9.0
    """,
    "ex1.csv": """
        **farm_animals1;
        your_farm my_farm farms_galore
        species;n_legs;avg_weight
        text;-;kg
        chicken;2.0;3.0
        pig;4.0;39.0
        goat;4.0;-
        zybra;-;-
        cow;-;200.0
        goose;2.0;9.0
    """,
    "ex2.csv": """
        **farm_animals2;
        your_farm my_farm farms_galore
        species;n_legs;avg_weight
        text;-;kg
        chicken;2.0;-
        pig;4.0;39.0
        goat;4.0;-
        zybra;4.0;-
        cow;-;200.0
        goose;2.0;9.0
    """,
    "ex3.csv": """
        **farm_animals3;
        your_farm my_farm farms_galore
        species;n_legs;avg_weight
        text;-;kg
        chicken;2.0;-
        pig;4.0;39.0
        goat;4.0;-
        zybra;-;-
        cow;-;200.0
        goose;2.0;9.0
    """,
    "json1.csv": """
        **farm_json1;
        your_farm my_farm farms_galore
        species;dt;num;flt;dt2;encode
        text;datetime;-;kg;datetime;onoff
        "chicken";-;2.0;-;2020-07-01 00:00:00;1
        pig;2020-07-02 00:00:00;4.0;39.0;2020-07-02 00:00:00;0
        goat;-;-;-;-;0
        z'ybra;-;4.0;-;-;1
        'cow';-;-;200.0;-;1
        goose;-;2.0;9.0;-;0
    """,
    "row1.csv": """
        **farm_row1;
        your_farm my_farm farms_galore
        species;n_legs;avg_weight
        text;-;kg
        chicken;2.0;3.0
        pig;4.0;39.0
        goat;4.0;-
        zybra;-;-
        cow;-;200.0
        goose;2.0;9.0
    """,
    "row2.csv": """
        **farm_row2;
        your_farm my_farm farms_galore
        species;n_legs;avg_weight
        text;-;kg
        chicken;2.0;3.0
        pig;4.0;39.0
        goat;4.0;-
        zybra;4.0;3.0
        cow;-;200.0
        goose;2.0;9.0
    """,
    "types1.csv": """
        **farm_types1;
        your_farm my_farm farms_galore
        species;num;flt;log
        text;-;kg;onoff
        chicken;2.0;3.0;1
        pig;4.0;39.0;0
        goat;4.0;-;1
        zybra;4.0;-;0
        cow;-;200.0;1
        goose;2.0;9.0;0
    """,
    "types2.csv": """
        **farm_types2;
        your_farm my_farm farms_galore
        species;num;flt;dt
        text;-;kg;datetime
        chicken;2.0;3.0;2020-07-01 00:00:00
        pig;4.0;39.0;2020-07-02 00:00:00
        goat;4.0;-;-
        zybra;4.0;-;-
        cow;-;200.0;-
        goose;2.0;9.0;-
    """,
    "types3.csv": """
        **farm_types3;
        your_farm my_farm farms_galore
        species;dt;num;flt;log;dt2;flt2;log2
        text;datetime;-;kg;onoff;datetime;kg;onoff
        chicken;-;2.0;-;0;2020-07-01 00:00:00;-;0
        pig;2020-07-02 00:00:00;4.0;39.0;0;2020-07-02 00:00:00;39.0;0
        goat;-;-;-;1;-;-;1
        zybra;-;4.0;-;0;-;-;0
        cow;-;-;200.0;1;-;200.0;1
        goose;-;2.0;9.0;0;-;9.0;0
    """,
    "units1.csv": """
        **farm_units1;
        your_farm my_farm farms_galore
        species;num;flt
        text;-;kg
        chicken;2.0;3.0
        pig;4.0;39.0
        goat;4.0;-
        zybra;4.0;-
        cow;-;200.0
        goose;2.0;9.0
        1234;-;-
    """,
}
