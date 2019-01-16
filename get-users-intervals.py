import csv
import datetime
import pgdb

# Get User Intervals: produces pairs of ids representing partitions of users 
#                     to ease the computation on a single machine

print("Starting at: " + str(datetime.datetime.now()))

# open output CSV users_intervals
output_path = "/home/ciori/Unitn/Big Data/tweets-database/user-profile/users_intervals.csv"
output_file = open(output_path, "a")
output_writer = csv.writer(output_file)

# connect to the PostgreSQL database
connection = pgdb.connect(host="localhost", user="postgres", password="", database="tweetsdb")

# iterations values
num_of_iterations = 100 # 40 for data_03
users_per_iteration = 200000
intervals = []

# for each decided iteration
for u in range(0, num_of_iterations):
    
    print("Iteration " + str(u+1) + "/" + str(num_of_iterations) + " started at " + str(datetime.datetime.now()))

    offset = u * users_per_iteration    

    # query to the database table users_keywords
    cur = connection.cursor()
    cur.execute("select min(user_id) as min, max(user_id) as max from " + 
                "(select distinct user_id " + 
                 "from public.users_keywords " + 
                 "order by user_id offset " + str(offset) + " limit " + str(users_per_iteration) + ") as a")
    for min_id, max_id in cur.fetchall():
        print((min_id, max_id))
        # add the interval to the list
        intervals.append((min_id, max_id))
    cur.close()

    print("    done at: " + str(datetime.datetime.now()))

# close the database connection
connection.close()

# save the list of intervals into the output CSV
output_writer.writerows(intervals)

print("Finished at: " + str(datetime.datetime.now()))