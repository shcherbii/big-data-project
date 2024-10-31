import pyspark.sql.types as t
import columns as c
import columns.columns_title_principals as columns_title_principals
import columns.columns_title_ratings as columns_title_ratings
import columns.columns_title_basics as columns_title_basics
import columns.columns_title_episode as columns_title_episode
import columns.columns_title_crew as columns_title_crew
# --------------------------------------- NAME BASICS -----------------------------------------

# Схема, яка використовуватиметься для першого зчитування датасету
schema_name_basics = t.StructType(
    [
        t.StructField("nconst", t.StringType(), True),
        t.StructField("primaryName", t.StringType(), True),
        t.StructField("birthYear", t.IntegerType(), True),
        t.StructField("deathYear", t.IntegerType(), True),
        t.StructField("primaryProfession", t.StringType(), True),
        t.StructField("knownForTitles", t.StringType(), True),
    ]
)

# Схема, яка використовуватиметься для вже обробленого датасету
schema_name_basics_final = t.StructType(
    [
        t.StructField(c.columns_name_basics.nconst, t.StringType(), True),
        t.StructField(c.columns_name_basics.primary_name, t.StringType(), True),
        t.StructField(c.columns_name_basics.primary_profession, t.StringType(), True),
        t.StructField(c.columns_name_basics.known_for_titles, t.StringType(), True),
    ]
)

# --------------------------------------- TITLE AKAS -----------------------------------------

# Схема, яка використовуватиметься для першого зчитування датасету
schema_title_akas = t.StructType(
    [
        t.StructField("titleId", t.StringType(), True),
        t.StructField("ordering", t.IntegerType(), True),
        t.StructField("title", t.StringType(), True),
        t.StructField("region", t.StringType(), True),
        t.StructField("language", t.StringType(), True),
        t.StructField("types", t.StringType(), True),
        t.StructField("attributes", t.StringType(), True),
        t.StructField("isOriginalTitle", t.IntegerType(), True),
    ]
)

# Схема, яка використовуватиметься для вже обробленого датасету
schema_title_akas_final = t.StructType(
    [
        t.StructField(c.columns_title_akas.title_id, t.StringType(), True),
        t.StructField(c.columns_title_akas.ordering, t.IntegerType(), True),
        t.StructField(c.columns_title_akas.title, t.StringType(), True),
        t.StructField(c.columns_title_akas.region, t.StringType(), True),
        t.StructField(c.columns_title_akas.language, t.StringType(), True),
        t.StructField(c.columns_title_akas.is_original_title, t.BooleanType(), True),
    ]
)



# --------------------------------------- TITLE PRINCIPLES -----------------------------------------

# Схема, яка використовуватиметься для першого зчитування датасету
schema_title_principals = t.StructType(
    [
        t.StructField("tconst", dataType=t.StringType()),  # поле в структурі
        t.StructField("ordering", dataType=t.IntegerType()),  # поле в структурі
        t.StructField("nconst", dataType=t.StringType()),  # поле в структурі
        t.StructField("category", dataType=t.StringType()),  # поле в структурі
        t.StructField("job", dataType=t.StringType()), # поле в структурі
        t.StructField("characters", dataType=t.StringType()), # поле в структурі
    ]
)

# Схема, яка використовуватиметься для вже обробленого датасету
schema_title_principals_final = t.StructType(
    [
        t.StructField(columns_title_principals.tconst, t.StringType(), True),
        t.StructField(columns_title_principals.ordering, t.IntegerType(), True),
        t.StructField(columns_title_principals.nconst, t.StringType(), True),
        t.StructField(columns_title_principals.category, t.StringType(), True),
        t.StructField(columns_title_principals.characters, t.StringType(), True),
    ]
)
#  --------------------------------------- TITLE CREW -----------------------------------------

schema_title_crew = t.StructType(
    [
        t.StructField("tconst", t.StringType(), True),
        t.StructField("directors", t.StringType(), True),
        t.StructField("writers", t.StringType(), True),
    ]
)
schema_title_crew_final = t.StructType(
    [
        t.StructField(columns_title_crew.tconst, t.StringType(), True),
        t.StructField(columns_title_crew.directors, t.StringType(), True),
        t.StructField(columns_title_crew.writers, t.StringType(), True),
    ]
)

# --------------------------------------- TITLE RATINGS -----------------------------------------

# Схема, яка використовуватиметься для першого зчитування датасету

schema_title_ratings = t.StructType(
    [
        t.StructField("tconst", dataType=t.StringType()),  # поле в структурі
        t.StructField("averageRating", dataType=t.FloatType()),  # поле в структурі
        t.StructField("numVotes", dataType=t.IntegerType()),  # поле в структурі
    ]
)

# Схема, яка використовуватиметься для вже обробленого датасету
schema_title_ratings_final = t.StructType(
    [
        t.StructField(columns_title_ratings.tconst, t.StringType(), True),
        t.StructField(columns_title_ratings.averageRating, t.FloatType(), True), 
        t.StructField(columns_title_ratings.numVotes, t.IntegerType(), True),
    ]
)

# --------------------------------------- TITLE BASICS -----------------------------------------

# Схема, яка використовуватиметься для першого зчитування датасету
schema_title_basics = t.StructType(
    [
        t.StructField("tconst", t.StringType(), True),
        t.StructField("titleType", t.StringType(), True),
        t.StructField("primaryTitle", t.StringType(), True),
        t.StructField("originalTitle", t.StringType(), True),
        t.StructField("isAdult", t.IntegerType(), True),
        t.StructField("startYear", t.IntegerType(), True),
        t.StructField("endYear", t.IntegerType(), True),
        t.StructField("runtimeMinutes", t.IntegerType(), True),
        t.StructField("genres", t.StringType(), True),
    ]
)

# Схема, яка використовуватиметься для вже обробленого датасету
schema_title_basics_final = t.StructType(
    [
        t.StructField(columns_title_basics.tconst, t.StringType(), True),
        t.StructField(columns_title_basics.title_type, t.StringType(), True),
        t.StructField(columns_title_basics.primary_title, t.StringType(), True),
        t.StructField(columns_title_basics.original_title, t.StringType(), True),
        t.StructField(columns_title_basics.is_adult, t.BooleanType(), True),
        t.StructField(columns_title_basics.start_year, t.IntegerType(), True),
        t.StructField(columns_title_basics.runtime_minutes, t.IntegerType(), True),
        t.StructField(columns_title_basics.genres, t.StringType(), True),
    ]
)

# --------------------------------------- TITLE EPISODE -----------------------------------------

# Схема, яка використовуватиметься для першого зчитування датасету
schema_title_episode = t.StructType(
    [
        t.StructField("tconst", t.StringType(), True),
        t.StructField("parentTconst", t.StringType(), True),
        t.StructField("seasonNumber", t.IntegerType(), True),
        t.StructField("episodeNumber", t.IntegerType(), True)
    ]
)

# Схема, яка використовуватиметься для вже обробленого датасету
schema_title_episode_final = t.StructType(
    [
        t.StructField(columns_title_episode.tconst, t.StringType(), True),
        t.StructField(columns_title_episode.parent_tconst, t.StringType(), True),
        t.StructField(columns_title_episode.season_number, t.IntegerType(), True),
        t.StructField(columns_title_episode.episode_number, t.IntegerType(), True),
    ]
)

