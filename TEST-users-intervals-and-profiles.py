import csv
import datetime
import operator
import pgdb
from pyspark import SparkContext
from pyspark.sql import SparkSession

# User Profiling: create an "interests" profile for each user 

print("Starting at: " + str(datetime.datetime.now()))

# Spark Session
ss = SparkSession \
    .builder \
    .config("spark.driver.extraClassPath", "/home/ciori/Projects/big-data-project/postgresql-42.2.5.jar") \
    .master("local[4]") \
    .appName("Big Data Project Python - SQL") \
    .getOrCreate()

# Spark Context
sc = ss.sparkContext

# set users profilings output csv
output_path = "/home/ciori/Unitn/Big Data/tweets-database/user-profile/TEST_users_intervals_and_profiles.csv"
output_file = open(output_path, "a")
output_writer = csv.writer(output_file)

# connect to the PostgreSQL database
connection = pgdb.connect(host="localhost", user="postgres", password="", database="tweetsdb")

# iterations values
num_of_iterations = 6
users_per_iteration = 200000

# for each decided iteration
for u in range(0, num_of_iterations):

    offset = u * users_per_iteration
    
    print("Iteration " + str(u+1) + "/" + str(num_of_iterations) + " started at " + str(datetime.datetime.now()))

    # get users interval
    min_id = 0
    max_id = 0
    cur = connection.cursor()
    cur.execute("select min(user_id) as min, max(user_id) as max from " + 
                "(select distinct user_id " + 
                 "from public.users_keywords " + 
                 "order by user_id offset " + str(offset) + " limit " + str(users_per_iteration) + ") as a")
    for minid, maxid in cur.fetchall():
        min_id = minid
        max_id = maxid
    cur.close()

    # query the database with spark to obtain the actual interval of users' keywords
    users_keywords = ss.read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/tweetsdb") \
        .option("user", "postgres") \
        .option("query", "select * from public.users_keywords where user_id>='" + str(min_id) + "' and user_id<='" + str(max_id) + "'") \
        .load()
    
    # prepare the rdd from the dataframe and partition it to work with 4 executors (to use 4 cores)
    users_keywords_rdd = users_keywords.rdd.repartition(4)

    # function to initially set each keyword with a count of 1
    def init_keywords_and_counts(line):
        user_id = str(line[0])
        text = str(line[1])
        keywords_and_counts = []
        for keyword in text.split(" "):
            keyword_and_count = (keyword, 1)
            keywords_and_counts.append(keyword_and_count)
        return [user_id, keywords_and_counts]

    # map each [user_id, "keyword1 keyword2 ..."] record to a new [user_id, [(keyword1, 1),(keyword2, 1),...]] line
    initial_keywords_and_counts = users_keywords_rdd.map(lambda x: init_keywords_and_counts(x))

    # function to count the number identical keyword and return them as a reduction step
    def count_keywords_raw(list1, list2):
        reduced_list = dict([])
        for l in list1:
            if l[0] in reduced_list:
                reduced_list[l[0]] += l[1]
            else:
                reduced_list[l[0]] = l[1]
        for l in list2:
            if l[0] in reduced_list:
                reduced_list[l[0]] += l[1]
            else:
                reduced_list[l[0]] = l[1]
        return list(reduced_list.items())

    # reduce multiple lines for each user_id into a single one with counts of keywords updated
    users_profiles_raw = initial_keywords_and_counts.reduceByKey(lambda x,y: count_keywords_raw(x, y))

    # function to order the (keyword, count) pairs by count and keep the 10 most frequent ones
    def keep_top_10(line):
        line[1].sort(key=operator.itemgetter(1), reverse=True)
        top_10 = line[1][:10]
        return [line[0], top_10]

    # an additional map to order the keywords by count and keep only the 10 most frequent ones,
    # therefore creating a partition of all the user profiles
    users_profiles_top_10 = users_profiles_raw.map(lambda x: keep_top_10(x))

    # save the user profile into the output csv
    output_writer.writerows(users_profiles_top_10.collect())

    print("    done at: " + str(datetime.datetime.now()))

# close the database connection
connection.close()

print("Finished at: " + str(datetime.datetime.now()))