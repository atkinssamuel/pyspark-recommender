from pyspark.sql import SparkSession


def department_salary_mapper(line):
    line_array = line[0].split(" ")
    return line_array[0], int(line_array[1])


if __name__ == "__main__":
    salary_path = "data/salary.txt"
    spark = SparkSession.builder.appName("dept_salary").getOrCreate()
    lines = spark.read.text(salary_path).rdd.map(department_salary_mapper)
    departmentSalarySums = lines.reduceByKey(lambda v1, v2: v1 + v2)
    output = departmentSalarySums.collect()

    for output_value in output:
        print("The individuals in the {} department were paid a total of ${:,}".format(output_value[0], output_value[1]))
