import csv
import re

# Pre-Processing: takes tweet_text CSV files to transform each text into a list of keywords

# each partition corresponds to a single data_xx folder of data
for partition in range(1,2):                        # used during testing to compute only one partition
#for partition in range(1,29):                      # used when computing the whole Twitter database

    # open input CSV tweet_text_xx
    csv_input_file = open("/home/ciori/Unitn/Big Data/tweets-database/tweet-text/tweet_text_" + str(partition) + ".csv", 'r')
    csv_input_reader = csv.reader(csv_input_file)

    # open output CSV tweet_keywords_xx
    csv_output_file = open("/home/ciori/Unitn/Big Data/tweets-database/tweet-keyword/tweet_keywords_" + str(partition) + ".csv", 'w')
    csv_output_writer = csv.writer(csv_output_file)

    # process each tweet text
    for row in csv_input_reader:
        cleaned_text = ''                           # cleaned text (list of keywords)
        # split by spaces
        for word1 in row[2].split():
            # remove 'RT' in text
            if not word1 == 'RT':
                # remove links
                if not (word1.startswith('http:')
                or word1.startswith('https:')
                or word1.startswith('ftp:')
                or word1.startswith('mailto:')
                or word1.startswith('www.')):
                    # first split of words by punctuations
                    for word2 in re.compile("[^a-zA-Z0-9@#_/'`]+").split(word1):
                        # keep hashtags as single keywords (without the # symbol)
                        if word2.startswith('#'):
                            word2 = word2.replace('#', '')
                            cleaned_text = cleaned_text + " " + word2
                        else:
                            # remove tags
                            if not word2.startswith('@'):
                                # second split of words by symbols that were present in hashtags and tags and can now be removed
                                for word3 in re.compile('[_/]+').split(word2):
                                    # add resulting keyword to the cleaned text
                                    cleaned_text = cleaned_text + " " + word3
        # remove unwanted whitespaces
        cleaned_text = cleaned_text.replace(' ', '', 1)
        cleaned_text = ' '.join(cleaned_text.split())
        # save cleaned text into the output CSV
        csv_output_writer.writerow([row[1], cleaned_text])
