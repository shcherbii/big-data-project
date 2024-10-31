import os
import datasets_paths as paths
from useful_functions import (get_statistics,
                            camel_to_snake,
                            str_to_arr_type,
                            create_folder)
import columns.columns_name_basics as columns_name_basics

from schemas import (schema_name_basics,
                    schema_name_basics_final)


def load_name_basics_df(path, spark_session, f, t):
    arrayed_cols_names = [columns_name_basics.primary_profession,
                        columns_name_basics.known_for_titles]
    
    if os.path.exists(paths.PATH_NAME_BASICS_MOD):
        print(f"You've already saved name_basics df !")

        df_ready = spark_session.read.csv(paths.PATH_NAME_BASICS_MOD,
                                    sep=r"\t", 
                                    header=True, 
                                    nullValue="\\N", 
                                    schema=schema_name_basics_final)
        return str_to_arr_type(df_ready, arrayed_cols_names, ',', f)

    name_basics_df = spark_session.read.csv(path,
                                            sep=r"\t", 
                                            header=True, 
                                            nullValue="\\N", 
                                            schema=schema_name_basics)
                                            
    cols = name_basics_df.columns 
    new_cols_names = [camel_to_snake(c) for c in cols]

    # Rename columns to 'snake' case  -  using 'withColumnRenamed(old, new)'
    for i, old_col in enumerate(cols):
        name_basics_df = name_basics_df.withColumnRenamed(old_col, new_cols_names[i])

    # get_statistics(name_basics_df, 'name_basics_df')

    """
        Бачимо, що у нашому df всього 13045749 записів, з яких not null:
            - primary_name = 13045749       [  100%  ]
            - birth_year = 598439           [  4.59% ]
            - death_year = 222539           [  1.7%  ]         
            - primary_profession = 10446409 [  80.07 %  ]
            - known_for_titles = 11590804   [  88.85 %  ]

        Отож, прийнято рішення видалити колонки (birth_year, death_year) через надто велику к-сть null значень і не здатність заповнити пропущені значення
        
        Щодо колонок які мають невелику к-сть null значень (primary_profession, known_for_titles) вирішено замінити їх на значення null на 'not_stated'  
    """

    name_basics_df = name_basics_df.fillna('not stated', subset=arrayed_cols_names)
    

    for col_name in arrayed_cols_names:   
        # Витягнення рядків, де значення в колонці col_name дорівнює "not stated"
        not_stated = name_basics_df.filter(name_basics_df[col_name] == "not stated")
        # Виведення результату
        print(f"not stated [{col_name}] = ", not_stated.count())
    '''
    RESULT:
        not stated [primary_profession] =  2599340 (13045749 - 10446409 ✅)                                
        not stated [known_for_titles] =  1454945   (13045749 - 11590804 ✅)              
    '''
    # get_statistics(name_basics_df, 'name_basics_df') # screen 1


    name_basics_df.show(30, truncate=False)


    ''' Видалимо колонки birth_year, death_year '''
    name_basics_df = name_basics_df.drop(columns_name_basics.birth_year, columns_name_basics.death_year)

    name_basics_df.show(truncate=False)
    name_basics_df.printSchema()

    # get_statistics(name_basics_df, 'name_basics_df', True, False, False) # no sense, cuz no numeric columns

    # Save to csv file
    create_folder(paths.PATH_NAME_BASICS_MOD, 'name_basics')
    print(f'Saving to {paths.PATH_NAME_BASICS_MOD} ...')
    name_basics_df.write.csv(paths.PATH_NAME_BASICS_MOD, header=True, mode='overwrite', sep='\t')
    print(f"name_basis's been modified and saved successfully!")

    name_basics_df_with_array_type = str_to_arr_type(name_basics_df, arrayed_cols_names, ',', f)
    
    return name_basics_df_with_array_type