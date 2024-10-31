from pyspark import SparkConf 
from pyspark.sql import SparkSession  

import pyspark.sql.types as t
import pyspark.sql.functions as f
from preprocess import (name_basics,
                          title_akas,
                          title_basics,
                          title_crew,
                          title_episode,
                          title_principals,
                          title_ratings)
import datasets_paths as paths
from useful_functions import (init_datasets_folders,
                              check_folder_content)
from pyspark.sql import Window

init_datasets_folders() # Створяться папки dataset, datasets_mod, де зберігатимуться сирі та оброблені dfs відповідно
                        # !NOTE! Проте, щоб запустити обробку dfs та запити, потрібно власноруч завантажити сирі датафрейми [https://datasets.imdbws.com/] у папку ʼdatasetsʼ (створену програмою) та назвати їх:
                        # name.basics.tsv
                        # title.akas.tsv
                        # ...

check_folder_content()  # Перевірити, чи  завантажині усі потрібні датасети

# підняти кластер (тобто створити нашу точку входу в spark application - це буде наша спарк сесія)
spark_session = (
    SparkSession.builder.master("local")  # посилання на кластер
    .appName("first app")
    .config(conf=SparkConf())  # default conf
    # .config("spark.executor.cores", "4")
    .getOrCreate()
)  # якщо сесія вже запущена то її отримати, якщо немає то створити


name_basics_df = name_basics.load_name_basics_df(paths.PATH_NAME_BASICS, spark_session, f, t)
title_akas_df = title_akas.load_title_akas_df(paths.PATH_TITLE_AKAS, spark_session, f, t, Window)
title_basics_df = title_basics.load_title_basics_df(paths.PATH_TITLE_BASICS, spark_session, f)
title_episode_df = title_episode.load_title_episode_df(paths.PATH_TITLE_EPISODE, spark_session, f)
title_principals_df = title_principals.load_title_principals_df(paths.PATH_TITLE_PRINCIPALS, spark_session, f)
title_ratings_df = title_ratings.load_title_ratings_df(spark_session, paths.PATH_TITLE_RATINGS, f)
title_crew_df = title_crew.load_title_crew_df(paths.PATH_TITLE_CREW, spark_session, f, t)

# # name_basics_df.show()
# name_basics_df.printSchema()

# name_basics_df.show()
# name_basics_df.printSchema()

# title_akas_df.show()
# title_akas_df.printSchema()

# title_basics_df.show()
# title_basics_df.printSchema()

# title_episode_df.show()
# title_episode_df.printSchema()

# title_principals_df.show()
# title_principals_df.printSchema()

# title_ratings_df.show()
# title_ratings_df.printSchema()

# title_crew_df.show()
# title_crew_df.printSchema()