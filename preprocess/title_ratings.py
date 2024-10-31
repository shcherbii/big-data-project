import os
import datasets_paths as paths
from useful_functions import (get_statistics,
                              camel_to_snake,
                              str_to_arr_type,
                              create_folder)
import columns.columns_title_ratings as columns_title_ratings

from schemas import schema_title_basics, schema_title_basics_final, schema_title_ratings, schema_title_ratings_final

from pyspark.sql import Window
from pyspark.sql.functions import col, sum


def load_title_ratings_df(spark_session, path, f):
    if os.path.exists(paths.PATH_TITLE_RATINGS_MOD):
        print(f"Title ratings already preprocessed")

        df = spark_session.read.csv(paths.PATH_TITLE_RATINGS_MOD,
                                          sep=r"\t",
                                          header=True,
                                          nullValue="\\N",
                                          schema=schema_title_ratings_final)

        return str_to_arr_type(df, [], ',', f)

    title_ratings_df = spark_session.read.csv(path,
                                            sep=r"\t",
                                            header=True,
                                            nullValue="\\N",
                                            schema=schema_title_ratings)

    columns = title_ratings_df.columns 
    renamed_columns = [camel_to_snake(c) for c in columns]

    for i, column in enumerate(columns):
        title_ratings_df = title_ratings_df.withColumnRenamed(column, renamed_columns[i])
    
    # get_statistics(title_ratings_df, 'title_ratings_df')

    title_ratings_df = title_ratings_df.na.drop(subset=title_ratings_df.columns)

    # title_ratings_df.show(30, truncate=False)
    # title_ratings_df.printSchema()

    create_folder(paths.PATH_TITLE_RATINGS_MOD, 'title_ratings_mod')
    print(f'Saving to {paths.PATH_TITLE_RATINGS_MOD} ...')
    title_ratings_df.write.csv(paths.PATH_TITLE_RATINGS_MOD, header=True, mode='overwrite', sep='\t')

    # return str_to_arr_type(title_ratings_df, [], ',', f)
    return title_ratings_df