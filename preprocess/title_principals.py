from pyspark.sql import SparkSession  # сесія на основі модуля sql
from pyspark import SparkConf
import columns.columns_title_principals as columns_title_principals
import re
import datasets_paths as paths
from pyspark.sql.functions import col
import os
from useful_functions import (get_statistics,
                            camel_to_snake,
                            str_to_arr_type,
                            create_folder)
from schemas import schema_title_principals, schema_title_principals_final

def load_title_principals_df(path, spark_session, f):
    arrayed_cols_names = [columns_title_principals.characters]

    if os.path.exists(paths.PATH_TITLE_PRINCIPALS_MOD):
        print(f"You already saved title_principals df !")
        
        df_ready = spark_session.read.csv(paths.PATH_TITLE_PRINCIPALS_MOD,
                                    sep=r"\t", 
                                    header=True, 
                                    nullValue="\\N", 
                                    schema=schema_title_principals_final)
        
        return str_to_arr_type(df_ready, arrayed_cols_names, ',', f)

    title_principals_df = spark_session.read.csv(path,
                                            sep=r"\t", 
                                            header=True, 
                                            nullValue="\\N", 
                                            schema=schema_title_principals)
                                            
    """
        Бачимо, що у нашому df всього 59367768 записів, з яких not null:
            - tconst = 59367768           [  100%  ]
            - ordering = 59367768         [  100% ]
            - nconst = 59367768           [  100%  ]         
            - job = 9746971               [  16.42 %  ]
            - characters = 28572846       [  48.13 %  ]

        Через малу к-сть not null значень і не здатність заповнити пропущені значення видалимо колонку job   
    
        Щодо колонки characters, яка має меншу к-сть null значень () вирішено замінити їх на 
        значення ('not_stated', 'not_stated') відповідно.  
    """
    
    ''' Видалимо колонки job '''
    title_principals_df = title_principals_df.drop('job')

    title_principals_df = title_principals_df.withColumn("characters",
                                            f.regexp_replace("characters", "[\\[\\]\"\']", ""))

    title_principals_df = title_principals_df.fillna('not stated', subset=arrayed_cols_names)

    title_principals_df.show(truncate=False)
    title_principals_df.printSchema()


    # # Specify the columns you want to check
    # column_to_check = columns_title_principals.characters
    # # Create a condition to filter rows with a comma in the specified column
    # condition = col(column_to_check).like("%,%")  # You can also use rlike(".*[,].*") for regular expression matching
    # # Apply the filter condition to the DataFrame
    # result_df = title_principals_df.filter(condition)
    # # Show the resulting DataFrame
    # result_df.show(truncate=False)

    # print(f'Showing first 30 rows')
    # title_principals_df.show(30, truncate=False)

    # Save to csv file
    create_folder(paths.PATH_TITLE_PRINCIPALS_MOD, 'title_principals_mod')
    print(f'Saving to {paths.PATH_TITLE_PRINCIPALS_MOD} ...')
    title_principals_df.write.csv(paths.PATH_TITLE_PRINCIPALS_MOD, header=True, mode='overwrite', sep='\t')
    print(f"title_principals's been modified and saved successfully!")

    
    title_principals_df_with_array_type = str_to_arr_type(title_principals_df, arrayed_cols_names, ',', f)

    return title_principals_df_with_array_type


