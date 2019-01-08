'''# init counts function
    def init_counts(line):
        new_lines = []
        user_id = str(line[0])
        text = str(line[1])
        for keyword in text.split(" "):
            new_line = [user_id + "_" + keyword, 1]
            new_lines.append(new_line)
        return new_lines    

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
    #print("    END top 10 map: " + str(datetime.datetime.now()))'''

# ------------------------------------------------------

'''num_of_iterations = 40
users_per_iteration = 200000

# split processing in batches of users
for u in range(0, num_of_iterations):

    print("Iteration " + str(u) + "/" + str(num_of_iterations) + " started at " + str(datetime.datetime.now()))

    offset = u * users_per_iteration
    
    min_max_id = ss.read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/tweetsdb") \
        .option("user", "postgres") \
        .option("query", "select min(user_id) as min, max(user_id) as max from (select distinct user_id from public.users_keywords order by user_id offset " + str(offset) + " limit " + str(users_per_iteration) + ") as a") \
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
    
    users_keywords_rdd = users_keywords.rdd
    users_keywords_rdd = users_keywords_rdd.repartition(4)

    def init_keywords_and_counts(line):
        user_id = str(line[0])
        text = str(line[1])
        keywords_and_counts = []
        for keyword in text.split(" "):
            keyword_and_count = (keyword, 1)
            keywords_and_counts.append(keyword_and_count)
        return [user_id, keywords_and_counts]

    initial_keywords_and_counts = users_keywords_rdd.map(lambda x: init_keywords_and_counts(x))

    def count_keywords_top_10(list1, list2):
        # reduce keywords and counts
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
        # keep top 10
        ordered_list = list(reduced_list.items())
        ordered_list.sort(key=operator.itemgetter(1), reverse=True)
        top_10 = ordered_list[:10]
        return top_10

    users_profiles_top_10 = initial_keywords_and_counts.reduceByKey(lambda x,y: count_keywords_top_10(x, y))

    # save user profile into csv
    output_writer.writerows(users_profiles_top_10.collect())

    print("    done at: " + str(datetime.datetime.now()))'''