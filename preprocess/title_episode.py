import os
import datasets_paths as paths
from useful_functions import (get_statistics,
                              camel_to_snake,
                              str_to_arr_type,
                              create_folder)
import columns.columns_title_episode as columns_title_episode

from schemas import schema_title_episode, schema_title_episode_final


def load_title_episode_df(path, spark_session, f):
    if os.path.exists(paths.PATH_TITLE_EPISODE_MOD):
        print(f"You've already saved title_episode df !")

        df_ready = spark_session.read.csv(paths.PATH_TITLE_EPISODE_MOD,
                                            sep=r"\t", 
                                            header=True, 
                                            nullValue="\\N", 
                                            schema=schema_title_episode_final)
        return df_ready

    title_episode_df = spark_session.read.csv(path,
                                            sep=r"\t",
                                            header=True,
                                            nullValue="\\N",
                                            schema=schema_title_episode)

    cols = title_episode_df.columns
    new_cols_names = [camel_to_snake(c) for c in cols]

    # Rename columns to 'snake' case  -  using 'withColumnRenamed(old, new)'
    for i, old_col in enumerate(cols):
        title_episode_df = title_episode_df.withColumnRenamed(old_col, new_cols_names[i])

    # get_statistics(title_episode_df, 'title_episode_df')

    """
         Бачимо, що у нашому df всього 7849091 записів, з яких not null:
             - tconst = 7849091                  [  100%   ]
             - parent_tconst = 7849091           [  100%   ]
             - season_number = 6254529           [  79.68% ]         
             - episode_number = 6254529          [  79.68% ]

         Щодо колонок які мають невелику к-сть null значень (season_number, episode_number) вирішено замінити їх на значення null на -1  
     """

    title_episode_df = title_episode_df.fillna(-1, subset=['season_number', 'episode_number'])

    for col_name in ['season_number', 'episode_number']:
        # Витягнення рядків, де значення в колонці col_name дорівнює "-1"
        not_stated = title_episode_df.filter(title_episode_df[col_name] == -1)
        # Виведення результату
        print(f"-1 [{col_name}] = ", not_stated.count())

    # title_episode_df.show(30, truncate=False)

    '''
    RESULT:
        not stated [season_number] =   1594562  (7849091 - 6254529 ✅)                                
        not stated [episode_number] =  1594562  (7849091 - 6254529 ✅)              
    '''
    #get_statistics(title_episode_df, 'title_episode_df') # screen 1

    # Save to csv file
    create_folder(paths.PATH_TITLE_EPISODE_MOD, "title_episode")
    print(f'Saving to {paths.PATH_TITLE_EPISODE_MOD} ...')
    title_episode_df.write.csv(paths.PATH_TITLE_EPISODE_MOD, header=True, mode='overwrite', sep='\t')
    print(f"title episode's been modified and saved successfully!")

    return title_episode_df