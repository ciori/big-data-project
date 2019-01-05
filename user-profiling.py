import csv
import datetime
from pyspark import SparkContext
from pyspark.sql import SparkSession
from operator import add

print("Starting at: " + str(datetime.datetime.now()))

# output users profiles csv
output_csv = open("/home/ciori/Unitn/Big Data/tweets-database/user-profile/user_profile_top_10.csv", "a")
output_reader = csv.writer(output_csv)

# SPARK SESSION
ss = SparkSession \
    .builder \
    .config("spark.driver.extraClassPath", "/home/ciori/Projects/big-data-project/postgresql-42.2.5.jar") \
    .master("local[*]") \
    .appName("Big Data Project Python - SQL") \
    .getOrCreate()

# SPARK CONTEXT
sc = ss.sparkContext

# split processing in batches of users
for u in range(0, 8):

    print("Start processing next 10'000 users at: " + str(datetime.datetime.now()))

    # return list of user_id batch
    user_ids = ss.read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/tweetsdb") \
        .option("user", "postgres") \
        .option("query", "select distinct user_id from public.users_keywords order by user_id offset " + str(u) + "0000 limit 10000") \
        .load()

    min_id = user_ids.agg({"user_id": "min"}).collect()
    max_id = user_ids.agg({"user_id": "max"}).collect()

    print(min_id[0][0])
    print(max_id[0][0])

    # read keywords
    users_keywords = ss.read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/tweetsdb") \
        .option("user", "postgres") \
        .option("query", "select * from public.users_keywords where user_id>='" + str(min_id[0][0]) + "' and user_id<='" + str(max_id[0][0]) + "'") \
        .load()

    # init counts function
    def init_counts(line):
        new_lines = []
        user_id = str(line[0])
        text = str(line[1])
        for keyword in text.split(" "):
            new_line = [user_id + "_" + keyword, 1]
            #new_line = [user_id, keyword, 1]
            #new_line = [keyword, 1]
            new_lines.append(new_line)
        return new_lines

    initial_keywords_counts = users_keywords.rdd.flatMap(lambda x: init_counts(x))
    keywords_counts = initial_keywords_counts.reduceByKey(add)

    # init lists function
    def init_lists(line):
        user_id = str(line[0].split("_")[0])
        keywords = [[str(line[0].split("_")[1]), line[1]]]
        return [user_id, keywords]

    users_lists = keywords_counts.map(lambda x: init_lists(x))
    users_profiles_raw = users_lists.reduceByKey(lambda x,y: x + y)

    # save user profile into csv
    output_reader.writerows(users_profiles_raw.collect())   

    print("    done at: " + str(datetime.datetime.now()))

print("Finished at: " + str(datetime.datetime.now()))