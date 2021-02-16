from pyspark.sql import SparkSession
import re


def word_formatter(line):
    return re.findall(r'\b[a-z]{1,15}\b', line[0].lower())


if __name__ == "__main__":
    shakespeare_path = "data/shakespeare.txt"
    desired_words = set([word.lower() for word in
                         ["SHAKESPEARE", "GUTENBERG", "WILLIAM", "LIBRARY", "COLLEGE", "WORLD", "THIS"]])

    spark = SparkSession.builder.appName("word_count").getOrCreate()
    words = spark.read.text(shakespeare_path).rdd.flatMap(word_formatter)

    desired_words_list = words.map(lambda word: (word, 1) if word in desired_words else ("other", 1))
    desired_words_count = desired_words_list.reduceByKey(lambda v1, v2: v1 + v2)
    other_rdd = spark.sparkContext.parallelize([("other", 0)])
    desired_words_count_filtered = desired_words_count.subtractByKey(other_rdd).collect()

    word_counts = words.map(lambda word: (word, 1)).reduceByKey(lambda v1, v2: v1 + v2)
    word_counts_by_count = word_counts.map(lambda kv_tuple: (kv_tuple[1], kv_tuple[0]))

    top_20 = word_counts_by_count.sortByKey(ascending=False).take(20)
    bottom_20 = word_counts_by_count.sortByKey(ascending=True).take(20)

    for desired_word_tuple in desired_words_count_filtered:
        print("The word \"{}\" appeared {} times".format(desired_word_tuple[0].upper(), desired_word_tuple[1]))

    print("\nThe top 20 words are as follows:")
    for i in range(len(top_20)):
        print("#{}: \"{}\" appeared {} times".format(i + 1, top_20[i][1], top_20[i][0]))

    print("\nThe bottom 20 words are as follows:")
    for i in range(len(bottom_20)):
        print("#{}: \"{}\" appeared {} time".format(i + 1, bottom_20[i][1], bottom_20[i][0]))
