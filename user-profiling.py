import csv
import datetime
import operator
from pyspark import SparkContext
from pyspark.sql import SparkSession

print("Starting at: " + str(datetime.datetime.now()))

output_path = "/home/ciori/Unitn/Big Data/tweets-database/user-profile/user_profile_top_10.csv"
output_file = open(output_path, "a")
output_writer = csv.writer(output_file)

# SPARK SESSION
ss = SparkSession \
    .builder \
    .config("spark.driver.extraClassPath", "/home/ciori/Projects/big-data-project/postgresql-42.2.5.jar") \
    .master("local[4]") \
    .appName("Big Data Project Python - SQL") \
    .getOrCreate()

# SPARK CONTEXT
sc = ss.sparkContext

# split processing in batches of users
for u in range(0, 40):

    print("Start processing next 200'000 users at: " + str(datetime.datetime.now()))

    limit = 200000
    offset = u * limit
    
    min_max_id = ss.read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/tweetsdb") \
        .option("user", "postgres") \
        .option("query", "select min(user_id) as min, max(user_id) as max from (select distinct user_id from public.users_keywords order by user_id offset " + str(offset) + " limit " + str(limit) + ") as a") \
        .load()

    min_id = min_max_id.collect()[0][0]
    max_id = min_max_id.collect()[0][1]

    # read keywords
    users_keywords = ss.read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/tweetsdb") \
        .option("user", "postgres") \
        .option("query", "select * from public.users_keywords where user_id>='" + str(min_id) + "' and user_id<='" + str(max_id) + "'") \
        .load()

    # init counts function
    def init_counts(line):
        new_lines = []
        user_id = str(line[0])
        text = str(line[1])
        for keyword in text.split(" "):
            new_line = [user_id + "_" + keyword, 1]
            new_lines.append(new_line)
        return new_lines

    users_keywords_rdd = users_keywords.rdd
    users_keywords_rdd = users_keywords_rdd.repartition(4)

    #print("    START init count flatmap: " + str(datetime.datetime.now()))
    initial_keywords_counts = users_keywords_rdd.flatMap(lambda x: init_counts(x))
    #print("    END init count flatmap: " + str(datetime.datetime.now()))
    keywords_counts = initial_keywords_counts.reduceByKey(operator.add)
    #print("    END init count reduce: " + str(datetime.datetime.now()))

    # init lists function
    def init_lists(line):
        user_id = str(line[0].split("_")[0])
        keywords = [(str(line[0].split("_")[1]), line[1])]
        return [user_id, keywords]

    #print("    START init lists map: " + str(datetime.datetime.now()))
    users_lists = keywords_counts.map(lambda x: init_lists(x))
    #print("    END init lists map: " + str(datetime.datetime.now()))
    users_profiles_raw = users_lists.reduceByKey(lambda x,y: x + y)
    #print("    END init lists reduce: " + str(datetime.datetime.now()))

    # top 10 most used keywords function
    def top_10_keywords(line):
        user_id = str(line[0])
        keywords = line[1]
        keywords.sort(key=operator.itemgetter(1), reverse=True)
        top_10 = keywords[:10]
        return [user_id, top_10]

    #print("    START top 10 map: " + str(datetime.datetime.now()))
    users_profiles_top_10 = users_profiles_raw.map(lambda x: top_10_keywords(x))
    #print("    END top 10 map: " + str(datetime.datetime.now()))

    # save user profile into csv
    #users_profiles_top_10.saveAsTextFile("/home/ciori/Unitn/Big Data/tweets-database/user-profile/user_profile_top_10_part" + str(u) + ".csv")
    output_writer.writerows(users_profiles_top_10.collect())

    print("    done at: " + str(datetime.datetime.now()))

print("Finished at: " + str(datetime.datetime.now()))