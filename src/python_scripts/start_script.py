from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lpad, to_date, lit, when
# from converted_pyspark import convert_me
from optmised_code import convert_me

# Initialize Spark session
spark = SparkSession. \
            builder \
            .appName("Start-Script") \
            .getOrCreate()

# TODO: Can put into different YAML file later on for extensibility
# Defining data paths
BASE_PATH = "data/"
data_files = {
    "dfCannibalizationFactors": "dfCannibalizationFactors.csv",
    "dfMonthlySales": "dfMonthlySales.csv",
    "dfReinvestmentFactors": "dfReinvestmentFactors.csv",
    "dfReinvestmentProjects": "dfReinvestmentProjects.csv",
    "dfSalesDaysFuture": "dfSalesDaysFuture.csv"
}

# Load all CSVs into a dictionary of DataFrames
dataFrames = {
    name: spark.read.option("header", True).option("inferSchema", True).csv(BASE_PATH + fname)
    for name, fname in data_files.items()
}
# print(dataFrames)

def format_dates_and_pad_loc(df, loc_col="loc_num", date_cols=None):
    """
    Pads key column named loc_col to 5 digits & 
    parses date columns.

    Args:
        df: Input DataFrame.
        loc_col: Column name to pad. Currently 'loc_num'.
        date_cols: List of date columns to convert.

    Returns:
        Transformed DataFrame.
    """
    if date_cols:
        for col_name in date_cols:
            df = df.withColumn(col_name, to_date(col(col_name)))
    return df.withColumn(loc_col, lpad(col(loc_col), 5, "0"))

# Clean and transform each DataFrame
dataFrames["dfSalesDaysFuture"] = (
        dataFrames["dfSalesDaysFuture"]
        .withColumn("close_date", when(col("close_date") == "null", "2200-01-01").otherwise(col("close_date")))
        .transform(lambda df: format_dates_and_pad_loc(df, date_cols=["open_date", "close_date"]))
        .withColumn("months_predict", lit(120))
    )

dataFrames["dfCannibalizationFactors"] = dataFrames["dfCannibalizationFactors"] \
                                            .withColumn("month", to_date("month"))

# TODO: Check if: 
#       close date will should be null
#       sales column values need to be rounded 
#       integers needed or float
dataFrames["dfMonthlySales"] = format_dates_and_pad_loc(
    dataFrames["dfMonthlySales"], date_cols=["month", "open_date"]
)

# TODO: Check if:
#      factor col should be float
dataFrames["dfReinvestmentFactors"] = format_dates_and_pad_loc(
    dataFrames["dfReinvestmentFactors"], date_cols=["month"]
)


dataFrames["dfReinvestmentProjects"] = format_dates_and_pad_loc(
    dataFrames["dfReinvestmentProjects"], date_cols=["shutdown", "reopen"]
)

# print("\nCleansed DataFrame: ")
# for name, df in dataFrames.items():
#     print(f"\n{name} schema:")
#     df.printSchema()
#     df.show(5, False) # comment later; cause lazy eval


final_df = convert_me(dataFrames["dfSalesDaysFuture"],
           dataFrames["dfMonthlySales"],
           dataFrames["dfReinvestmentProjects"],
           dataFrames["dfCannibalizationFactors"],
           dataFrames["dfReinvestmentFactors"])

final_df.coalesce(1) \
    .write \
    .mode("overwrite") \
    .option("header", True) \
    .csv("output/py_final_sales_days_forecast.csv")
