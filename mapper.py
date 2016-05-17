import json
import sys

pron_list = ["han", "hon", "den", "det", "denna", "denne", "hen"]
for line in sys.stdin:
    line = line.strip()
    try:
        tweet = json.loads(line)
        text =  tweet['text'].lower().split()

        if not 'retweeted_status' in tweet:
                print '%s\t%s' % ("****UNIQUE_TWITTER_POSTS****", 1)

                for word in text:
                        if word in pron_list:
                                 print '%s\t%s' % (word, 1)

    except:
        pass