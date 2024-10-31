import os
import datasets_paths as paths
from useful_functions import (get_statistics,
                            camel_to_snake,
                            str_to_arr_type,
                            create_folder)

import columns.columns_title_basics as columns_title_basics

from schemas import schema_title_basics, schema_title_basics_final


def load_title_basics_df(path, spark_session, f):
    arrayed_cols_names = [columns_title_basics.genres]

    if os.path.exists(paths.PATH_TITLE_BASICS_MOD):
        print(f"You've already saved title_basics df!")

        df_ready = spark_session.read.csv(paths.PATH_TITLE_BASICS_MOD,
                                    sep=r"\t", 
                                    header=True, 
                                    nullValue="\\N", 
                                    schema=schema_title_basics_final)
        return str_to_arr_type(df_ready, arrayed_cols_names, ',', f)

    title_basics_df = spark_session.read.csv(path,
                                            sep=r"\t", 
                                            header=True, 
                                            nullValue="\\N", 
                                            schema=schema_title_basics)
                                            
    cols = title_basics_df.columns 
    new_cols_names = [camel_to_snake(c) for c in cols]

    # Rename columns to 'snake' case  -  using 'withColumnRenamed(old, new)'
    for i, old_col in enumerate(cols):
        title_basics_df = title_basics_df.withColumnRenamed(old_col, new_cols_names[i])

    # get_statistics(title_basics_df, "TITLE BASICS changede columns name")

    """
        Бачимо, що у нашому df всього 10371207 записів, з яких not null:
            - titleType = 10371207       [   100%   ]
            - primaryTitle = 10371207    [   100%   ]
            - titleType = 10371207       [   100%   ]
            - originalTitle = 10371207   [   100%   ]
            - isAdult = 10371206         [  99.99%  ]
            - startYear = 8984192        [  86.62%  ]
            - endYear = 115522           [   1.11%  ]         
            - runtimeMinutes = 3116853   [  30.05%  ]
            - genres = 9909020           [  95.54%  ]

        Отож, прийнято рішення видалити колонку (endYear) через надто малу к-сть not null значень і не здатність заповнити пропущені значення
    """
    
    ''' В колонці is_adult змінимо некоректні записи на Null та перетворимо її на BoolType '''
    title_basics_df = title_basics_df.withColumn(
        'is_adult',
        f.when(f.col('is_adult') == 1, True)
        .when(f.col('is_adult') == 0, False)
        .otherwise(None)
    )

    # title_basics_df = title_basics_df.fillna(-1, subset=['start_year', 'runtime_minutes']) 
    # title_basics_df = title_basics_df.fillna('not stated', subset=arrayed_cols_names)   

    ''' Видалимо колонки end_year '''
    title_basics_df = title_basics_df.drop('end_year')

    # title_basics_df.printSchema()

    # get_statistics(title_basics_df, "TITLE BASICS")

    # Save to csv file
    create_folder(paths.PATH_TITLE_BASICS_MOD, 'title_basics_mod')
    print(f'Saving to {paths.PATH_TITLE_BASICS_MOD} ...')
    title_basics_df.write.csv(paths.PATH_TITLE_BASICS_MOD, header=True, mode='overwrite', sep='\t')
    print(f"title_basics's been modified and saved successfully!")

    title_basics_df_with_array_type = str_to_arr_type(title_basics_df, arrayed_cols_names, ',', f)
    
    return title_basics_df_with_array_type