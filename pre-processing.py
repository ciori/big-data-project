import csv
import re

# Pre-process tweet_text CSV files to transform each text into a list of keywords

# open input CSV
csv_input_file = open('tweets_sample.csv', 'r')
#csv_input_file = open('/home/ciori/Unitn/Big Data/tweets-database/tweet-keyword/tweet_text_03.csv', 'r')
csv_input_reader = csv.reader(csv_input_file)

# open output CSV
csv_output_file = open('keywords_sample.csv', 'w')
#csv_output_file = open('/home/ciori/Unitn/Big Data/tweets-database/tweet-keyword/tweet_keywords_latest.csv', 'w')
csv_output_reader = csv.writer(csv_output_file)

# process tweets
for row in csv_input_reader:
    cleaned_text = '' # cleaned text (keywords)
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
                # first split of words
                for word2 in re.compile("[^a-zA-Z0-9@#_/'`]+").split(word1):
                    # keep hashtags as single keywords
                    if word2.startswith('#'):
                        word2 = word2.replace('#', '')
                        cleaned_text = cleaned_text + " " + word2
                    else:
                        # remove quotes
                        if not word2.startswith('@'):
                            # second split of words
                            for word3 in re.compile('[_/]+').split(word2):
                                # add resulting keyword to the cleaned text
                                cleaned_text = cleaned_text + " " + word3
    # remove unwanted whitespaces
    cleaned_text = cleaned_text.replace(' ', '', 1)
    cleaned_text = ' '.join(cleaned_text.split())
    # print texts
    #print(row[2])
    #print(cleaned_text)
    #print()
    # save cleaned text into the output CSV
    csv_output_reader.writerow([row[1], cleaned_text])
