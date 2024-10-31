import os
import datasets_paths as paths
from useful_functions import (get_statistics,
                            camel_to_snake,
                            str_to_arr_type,
                            create_folder)

import columns.columns_title_akas as columns_title_akas

from schemas import (schema_title_akas,
                     schema_title_akas_final)


def load_title_akas_df(path, spark_session, f, t, Window):
    if os.path.exists(paths.PATH_TITLE_AKAS_MOD):
        print(f"You've already saved title_akas df !")

        df_ready = spark_session.read.csv(paths.PATH_TITLE_AKAS_MOD,
                                    sep=r"\t", 
                                    header=True, 
                                    nullValue="\\N", 
                                    schema=schema_title_akas_final)
        return df_ready

    title_akas_df = spark_session.read.csv(path,
                                        sep=r"\t", 
                                        header=True, 
                                        nullValue="\\N",
                                        schema = schema_title_akas
                                        )

    # *******************          Rename columns to 'snake' case                *******************
    cols = title_akas_df.columns 
    new_cols_names = [camel_to_snake(c) for c in cols]
    for i, old_col in enumerate(cols):
        title_akas_df = title_akas_df.withColumnRenamed(old_col, new_cols_names[i])
    # title_akas_df.show()
    # title_akas_df.printSchema()
    # get_statistics(title_akas_df, 'title_akas_df')

    """
        Бачимо, що у нашому df всього 37973032 записів, з яких not null:
            - title_id          = 37973032       [  100%  ]
            - ordering          = 37973032       [  100%  ]
            - title             = 37973032       [  100%  ]
            - region            = 36059967       [  94.96%  ]
            - language          = 31140396       [  82.00%  ]
            - types             = 5739676        [  15.12% ]
            - attributes        = 222539         [  0.59%  ]         
            - is_original_title = 37970949       [  99.99%  ]

        Отож, прийнято рішення видалити колонки (types, attributes) через надто малу к-сть not null значень і не здатність заповнити пропущені значення
        
        Колонку is_original_title - dropna
        
        Щодо колонок які мають невелику к-сть null значень (region, language) вирішено дослідити їхні значення у парі:
            - Сутності у яких region && language == isNull - видаляємо
            - Сутності у яких region isNotNull && language isNull - відновлюємо language з region
            - Сутності у яких region isNull && language isNotNull - відновлюємо region з language

    """

    # *******************            Видалимо колонки types, attributes                      *******************
    title_akas_df = title_akas_df.drop(columns_title_akas.types, columns_title_akas.attributes)

    # *******************            dropna  колонки is_original_title                      *******************
    title_akas_df = title_akas_df.dropna(subset=[columns_title_akas.is_original_title])
    # print('Результат видалення рядків, де is_original_title == null : ', title_akas_df.count())
    # title_akas_df.count() = 37970949 (видалилось: 37973032 - 37970949 = 2_083)


    # *******************   видалимо усі рядки де "region"=="language"== Null (одночасно)   *******************
    title_akas_df = title_akas_df.filter(~(f.col(columns_title_akas.region).isNull() 
                                        & f.col(columns_title_akas.language).isNull()))
    # print('Результат видалення рядків, в яких "region", "language" == Null (одночасно) : ', title_akas_df.count())
    # title_akas_df.count() = 36057887 (видалилось: 37970949 - 36057887 = 1_913_062)


    # *******************       Перетворимо колонку is_original_title з int -> bool             *******************
    # title_akas_df.groupBy('is_original_title').count().show()   # BEFORE -->
    title_akas_df = title_akas_df.withColumn(columns_title_akas.is_original_title,
                                            f.col(columns_title_akas.is_original_title).cast(t.BooleanType()))
    # print('Result of converting "is_original_title" from "int" to "bool" : ')
    # title_akas_df.show()
    # title_akas_df.groupBy('is_original_title').count().show()   #   <-- AFTER


    #  *******************             Згенеруємо df щоб відновити language з region            *******************
    # ^ 1) Дістанемо усі рядки з title_akas_df, де "region" та "language" не є Null
    title_akas_df_not_nulls = title_akas_df.filter(f.col(columns_title_akas.region).isNotNull() 
                                                    & f.col(columns_title_akas.language).isNotNull())
    # title_akas_df_not_nulls.show()
    # print('Df filtered ["region" & "language" isNotNull() count = ', title_akas_df_not_nulls.count())
    # title_akas_df_not_nulls.count() = 31140396 (36057887 - 31140396 = 4_917_491 рядків де є нали )

    # ^ 2) Виконаємо групування по "region", "language" і порахуємо к-сть кожної з комбінацій
    region_lang_grouped_df = (title_akas_df_not_nulls.groupBy(columns_title_akas.region, columns_title_akas.language)
                                                    .count()
                                                    .orderBy([columns_title_akas.region, columns_title_akas.language]))
    # print('count of rows: ', region_lang_grouped_df.count()) # 303
    # region_lang_grouped_df.show() 
    # DESCRIPTION: Бачимо, що є такі регіони (напр. ʼAFʼ), де є декілька мов ('de', 'en', 'prs') 
    # DESCRIPTION: Тому беремо таку мову, яка найбільш часта для тої країни:

    # ^ 3) Для цього використаємо ф-ю window, а саме:
    # ^    Погрупуємо записи з утвор. таблиці з попер. кроку по "region" і до кожного запису допишемо нову 
    # ^    колонку "max_count_by_region" - яка показуватиме максимальний "count" в межах його ГРУПИ!
    window = Window.partitionBy(columns_title_akas.region).orderBy(f.col('count').desc())
    region_lang_grouped_max_df = region_lang_grouped_df.withColumn('max_count_by_region', f.max(f.col('count')).over(window))
    # region_lang_grouped_max_df.show()

    # ^ 4) Далі дістанемо усі записи де "max_count_by_region" == "count":
    region_lang_grouped_max_filtered_df = region_lang_grouped_max_df.filter(f.col('count')==f.col('max_count_by_region'))
    # ^    також виконаємо dropDuplicates по колонках 'region', 'count' (адже є такі мови де їх к-сть для деякого
    # ^    регіону однакова)
    region_lang_grouped_max_filtered_df = region_lang_grouped_max_filtered_df.dropDuplicates([
        columns_title_akas.region, 'count'])
    # region_lang_grouped_max_filtered_df.show()
    # ^    Дістанемо лише ті колонки, які нам треба далі:
    region_lang_grouped_max_filtered_df = region_lang_grouped_max_filtered_df.select(columns_title_akas.region, columns_title_akas.language)
    # print('Result dataframe [region; most_common_language] count: ', region_lang_grouped_max_filtered_df.count())
    # -- count = 119 --> 
    # % RESULT: Ми отримали датафрейм "region_lang_grouped_max_filtered_df" , який містить колонки: 
    # %         [region] [most_common_language_for_this_region]


    #  *******************                    Відновимо language з region                       *******************
    # ^ 1) Дістанемо рядки, де 'language' = null, а 'region' = NOT null
    title_akas_df_null_language = title_akas_df.filter((f.col(columns_title_akas.language).isNull())
                                                     & (f.col(columns_title_akas.region).isNotNull()))
    # print("[language === null] Number of entries:", title_akas_df_null_language.count()) #^ count = 4_917_491
    # title_akas_df_null_language.show()
    # stats_df = title_akas_df_null_language.describe() #^ count = 4_917_491 (but of language == 0 бо всі там нали)
    # stats_df.show()
    # ^ 2) Оскільки вся колонка "language" заповнена налами - видалимо її
    title_akas_df_null_language = title_akas_df_null_language.drop(columns_title_akas.language);

    # ^ 3) Виконаємо LEFT JOIN нашого основного df (де lang == null) з нашим згенерованим df ([region] [most_common_language]) 
    joined_title_akas_df = title_akas_df_null_language.join(region_lang_grouped_max_filtered_df,
                                                            columns_title_akas.region,
                                                            'left');
    # joined_title_akas_df.show()
    # stats_df = joined_title_akas_df.describe()
    # stats_df.show() 
    #^  count(language) = 4_894_394 (notNull values !!!) - зі 4_917_491 ми відновили 4_894_394 (99.53 %) 
    #^  (23097 language - є null)

    needed = [columns_title_akas.title_id, columns_title_akas.region,
            columns_title_akas.ordering, columns_title_akas.language]
    needed_new = [c + '_new' for c in needed]
    joined_title_akas_df = joined_title_akas_df.select([f.col(old).alias(new) for old, new in zip(needed, needed_new)])

    join_condition = ((title_akas_df[columns_title_akas.title_id] == joined_title_akas_df['title_id_new']) & 
                      (title_akas_df[columns_title_akas.ordering] == joined_title_akas_df['ordering_new']))
    title_akas_joined_df = title_akas_df.join(joined_title_akas_df, on=join_condition, how="left_outer")
    # title_akas_joined_df.show()

    for c_new in needed_new[:-1]:
        title_akas_joined_df = title_akas_joined_df.drop(c_new)

    title_akas_joined_df = title_akas_joined_df.withColumn(columns_title_akas.language,
                                                        f.when(f.col(columns_title_akas.language).isNull(),
                                                                f.col('language_new'))
                                                        .otherwise(f.col(columns_title_akas.language)))
    title_akas_joined_df = title_akas_joined_df.drop(needed_new[-1])

    # title_akas_joined_df.show()
    # title_akas_joined_df.describe().show() # count(region) = 36_057_887; count(language) = 36_034_790 (23097 language - є null)

    #! У нас лишилося 23_097 рядків де language == Null. Спробуємо просто видалити їх
    title_akas_joined_df = title_akas_joined_df.dropna(subset=[columns_title_akas.language])
    # print(title_akas_joined_df.count()) # 36_034_790 (36_057_887 - 23_097)

    #  *******************                    Відновимо region з language                       *******************
    # # Дістання рядків, де 'language' = не null, а 'region' = null
    # df_null_region = title_akas_df.filter((f.col(columns_title_akas.language).isNotNull()) &
    #                                     (f.col(columns_title_akas.region).isNull()))
    # print("[null region] Number of entries:", df_null_region.count())
    # df_null_region.show() 
    #^ Немає таких !!!

    # Save to csv file
    create_folder(paths.PATH_TITLE_AKAS_MOD, 'title_akas')
    print(f'Saving to {paths.PATH_TITLE_AKAS_MOD} ...')
    title_akas_joined_df.write.csv(paths.PATH_TITLE_AKAS_MOD, header=True, mode='overwrite', sep='\t')
    print(f"title_akas's been modified and saved successfully!")
    
    return title_akas_joined_df