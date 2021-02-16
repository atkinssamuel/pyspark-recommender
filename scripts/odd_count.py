from pyspark.sql import SparkSession


# counts the odd and even numbers in 'integer.txt'


def is_even(number):
    if number % 2 == 0:
        return 0, 1
    return 1, 1


def add_values(v1, v2):
    return v1 + v2


if __name__ == "__main__":
    integer_path = "data/integer.txt"
    spark = SparkSession.builder.appName("odd_count").getOrCreate()
    numbers = spark.read.text(integer_path).rdd.map(lambda line: int(line[0]))
    even_odd_mapper = numbers.map(is_even)
    even_odd_counter = even_odd_mapper.reduceByKey(add_values)
    output = even_odd_counter.collect()

    for output_value in output:
        if output_value[0] == 0:
            print("Number of even numbers = {}".format(output_value[1]))
        else:
            print("Number of odd numbers = {}".format(output_value[1]))

