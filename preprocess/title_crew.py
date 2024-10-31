import os
import datasets_paths as paths
from useful_functions import (get_statistics,
                            camel_to_snake,
                            str_to_arr_type,
                            create_folder)
import columns.columns_title_crew as columns_title_crew 
from schemas import schema_title_crew, schema_title_crew_final


def load_title_crew_df(path, spark_session, f, t):
    arrayed_cols_names = [columns_title_crew.directors, columns_title_crew.writers]

    if os.path.exists(paths.PATH_TITLE_CREW_MOD):
        print(f"You already saved title_crew df !")
        df_ready = spark_session.read.csv(path,
                                sep=r"\t", 
                                header=True, 
                                nullValue="\\N", 
                                schema=schema_title_crew_final)
        return str_to_arr_type(df_ready, arrayed_cols_names, ',', f)

    crew_df = spark_session.read.csv(path,
                                    sep=r"\t", 
                                    header=True, 
                                    nullValue="\\N",
                                    schema=schema_title_crew)
    # crew_df.show(40)
    # crew_df.printSchema()
    # crew_df.describe().show()
    """
    Бачимо, що у нашому df всього 10_393_623 записів, з яких not null:
        - tconst = 10_393_623           [  100%  ]
        - directors = 5_971_561         [  57.45% ]
        - writers = 5_383_366           [  51.79%  ]         

    Щодо колонок directors, writers які мають меншу к-сть null значень лишимо їх як null 
    та переведемо в тип ArrayType(StringType) з використанням методу str_to_arr_type
"""

        # Save to csv file
    create_folder(paths.PATH_TITLE_CREW_MOD, 'title_crew')
    print(f'Saving to {paths.PATH_TITLE_CREW_MOD} ...')
    crew_df.write.csv(paths.PATH_TITLE_CREW_MOD, header=True, mode='overwrite', sep='\t')
    print(f"title_crew's been modified and saved successfully!")

    crew_df_with_array_type = str_to_arr_type(crew_df, arrayed_cols_names, ',', f)
    
    return crew_df_with_array_type
