from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import lag, col, sum, trim, to_date, date_format
import json


def load_data(file_path):

    spark = SparkSession.builder.appName("turnstile-analysis").getOrCreate()

    data_df = spark.read.format("csv") \
        .option("header", "true") \
        .load(file_path)

    return data_df


def process_data(turnstile_file_path, station_file_path):

    turnstile_df = load_data(turnstile_file_path)

    # Eliminate the spaces
    turnstile_df = turnstile_df.withColumnRenamed(
        "EXITS                                                               ", "EXITS")
    turnstile_df = turnstile_df.withColumn("EXITS", trim(turnstile_df["EXITS"])) \
        .withColumn("date", date_format(to_date("date", "MM/dd/yyyy"), "MM-dd-yyyy"))

    stations_df = load_data(station_file_path)

    # Filter the station in Manhattan
    manhattan_stations_df = stations_df.filter(stations_df.borough == "Manhattan")

    # Join
    combined_df = turnstile_df.join(manhattan_stations_df.alias("manhattan_stations"), [
        turnstile_df.STATION == manhattan_stations_df.station,
        turnstile_df.LINENAME == manhattan_stations_df.line_names,
        turnstile_df.DIVISION == manhattan_stations_df.division
    ])

    # Calculate the number of entrances and exits for each station at each date and time
    window_spec = Window.partitionBy("manhattan_stations.station", "linename", "manhattan_stations.division", "scp").orderBy("date", "time")
    selected_df = combined_df \
        .select(
            col("manhattan_stations.station").alias("station"),
            col("linename"),
            col("manhattan_stations.division").alias("division"),
            col("scp"),
            col("date"),
            col("time"),
            col("ENTRIES").cast("int"),
            col("EXITS").cast("int").alias("exit"),
            (col("ENTRIES") - lag("ENTRIES", 1).over(window_spec)).alias("enter_num"),
            (col("EXITS") - lag("EXITS", 1).over(window_spec)).alias("exit_num")
        )\
        .na.drop()  # drop the first line

    aggregated_df = selected_df.groupBy("station", "linename", "division", "date", "time")\
        .agg(sum("enter_num").cast("int").alias("enter"), sum("exit_num").cast("int").alias("exit"))\
        .orderBy("station", "linename", "division", "date", "time")

    # Reorder
    aggregated_df = aggregated_df.select("station", "linename", "division", "enter", "exit", "date", "time")
    aggregated_df.show()
    print("aggregated_df.count()", aggregated_df.count())

    # Get the number of stations
    count_df = aggregated_df.groupBy("station", "linename", "division").count()
    num_rows = count_df.count()
    print("the number of stations: ", num_rows)

    # print("orderBy(count, ascending=False).show()")
    # aggregated_df.groupBy("station", "linename", "division").count().orderBy("count", ascending=False).show()

    results = aggregated_df \
        .toJSON() \
        .map(lambda x: json.loads(x)) \
        .collect()

    return results
