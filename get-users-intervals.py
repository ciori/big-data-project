import csv
import datetime
import pgdb

print("Starting at: " + str(datetime.datetime.now()))

output_path = "/home/ciori/Unitn/Big Data/tweets-database/user-profile/users_intervals.csv"
output_file = open(output_path, "a")
output_writer = csv.writer(output_file)

connection = pgdb.connect(host="localhost", user="postgres", password="", database="tweetsdb")

num_of_iterations = 40
users_per_iteration = 200000

intervals = []

for u in range(0, num_of_iterations):
    
    print("Iteration " + str(u+1) + "/" + str(num_of_iterations) + " started at " + str(datetime.datetime.now()))

    offset = u * users_per_iteration    

    cur = connection.cursor()
    cur.execute("select min(user_id) as min, max(user_id) as max from (select distinct user_id from public.users_keywords order by user_id offset " + str(offset) + " limit " + str(users_per_iteration) + ") as a")
    for min_id, max_id in cur.fetchall():
        intervals.append((min_id, max_id))
    cur.close()

    print(intervals)

    print("    done at: " + str(datetime.datetime.now()))

connection.close()
output_writer.writerows(intervals)

print("Finished at: " + str(datetime.datetime.now()))