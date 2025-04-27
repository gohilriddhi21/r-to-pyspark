from pyspark.sql import functions as F
from pyspark.sql import Window
from helper_pyspark import (
    fun_fill_day_details,
    fun_reinvestment_days_lost,
    fun_get_actual_reported_day,
    fun_annualized_inflation,
    fun_long_run_inflation,
    fun_sales_days_between,
    fun_months_between_exact
)

def convert_me(dfSalesDaysFuture, dfSalesMonthly, dfReinvestmentProjects, dfCannibalization, dfReinvestFactors, strScope="forecast"):
    dfDayDetails = fun_fill_day_details(dfSalesMonthly)
    actual_cutoff = fun_get_actual_reported_day()

    dfSalesDaysFuture = dfSalesDaysFuture.withColumn(
        "nMonthStart",
        F.when(F.dayofmonth("date_forecast") < F.lit(actual_cutoff), -1).otherwise(0)
    ).withColumn("month_open", F.trunc("open_date", "month")) \
     .withColumn("month_close", F.trunc("close_date", "month"))

    dfSeq = dfSalesDaysFuture.withColumn(
        "month_offset_arr", F.sequence("nMonthStart", F.col("months_predict") - 1)
    ).withColumn("offset", F.explode("month_offset_arr")).drop("month_offset_arr") \
     .withColumn("month", F.add_months(F.trunc("date_forecast", "month"), "offset")) \
     .drop("offset") \
     .filter((F.col("month") >= F.col("month_open")) & (F.col("month") <= F.col("month_close"))) \
     .withColumn("concept_code", F.col("location_type_code")) \
     .withColumn("jan1_day", F.date_format(F.trunc("month", "year"), "EEEE")) \
     .withColumn("month_num", F.month("month")) \
     .withColumn("leap_year", ((F.year("month") % 4 == 0) & ((F.year("month") % 100 != 0) | (F.year("month") % 400 == 0))))

    dfFuture = dfSeq.join(dfDayDetails, on=["jan1_day", "month_num", "leap_year"], how="inner") \
                    .drop("jan1_day", "month_num", "leap_year", "year_ref")

    window_loc = Window.partitionBy("loc_num").orderBy("month")
    dfHist = dfSalesMonthly.withColumn("month_num", F.month("month")) \
        .withColumn("nMonthsHistory", F.count("*").over(Window.partitionBy("loc_num"))) \
        .withColumn("rn", F.row_number().over(window_loc)) \
        .filter((F.col("nMonthsHistory") >= 12) & (F.col("rn") >= F.col("nMonthsHistory") - 11)) \
        .select("loc_num", "month_num", "days").dropDuplicates(["loc_num", "month_num"])

    dfFuture = dfFuture.withColumn("month_num", F.month("month")) \
                       .join(dfHist, on=["loc_num", "month_num"], how="left") \
                       .withColumn(
                           "days_max",
                           F.when((F.col("location_type_code") == "OCL") & F.col("location_type_code").isNotNull(),
                                  F.least(F.coalesce(F.col("days"), F.col("days_max")), F.col("days_max"))
                           ).otherwise(F.col("days_max"))
                       ).drop("days")

    if dfFuture.filter(F.col("location_type_code").isNull() | F.col("concept_code").isNull()).count() > 0:
        raise Exception("Missing location type codes or concept codes")

    dfOpenClose = dfSalesDaysFuture.select(
        "loc_num",
        fun_sales_days_between(F.trunc("open_date", "month"), "open_date").alias("days_before_open"),
        fun_sales_days_between("close_date", F.last_day("close_date")).alias("days_after_close")
    )

    dfFuture = dfFuture.join(dfOpenClose, on="loc_num", how="left") \
                       .fillna({"days_before_open": 0, "days_after_close": 0})

    dfReinvDays = fun_reinvestment_days_lost(dfReinvestmentProjects, dfSalesMonthly)
    dfFuture = dfFuture.join(dfReinvDays, on=["loc_num", "month"], how="left") \
                       .fillna({"days_lost": 0})

    dfFuture = dfFuture.withColumn("days_max", F.col("days_max") - F.col("days_lost")) \
                       .withColumn("days_max", F.when(F.col("month") == F.col("month_open"),
                                                      F.col("days_max") - (F.col("days_before_open") - 1))
                                    .otherwise(F.col("days_max"))) \
                       .withColumn("days_max", F.when(F.col("month") == F.col("month_close"),
                                                      F.col("days_max") - F.col("days_after_close"))
                                    .otherwise(F.col("days_max"))) \
                       .withColumn("days_max", F.greatest(F.lit(0), F.col("days_max"))) \
                       .withColumn("age", fun_months_between_exact("month_open", "month")) \
                       .withColumnRenamed("days_max", "days") \
                       .drop("days_lost", "days_before_open", "days_after_close", "open_date", "close_date", "month_open", "month_close", "month_num")

    dfFuture = dfFuture.withColumn(
        "reported_month",
        F.add_months(F.date_trunc("month", F.date_sub("date_forecast", fun_get_actual_reported_day() - 1)), -1)
    )

    dfInfl = dfSalesMonthly.select("loc_num", F.col("month").alias("reported_month"), F.col("inflation_factor_ending").alias("inflation_factor"))
    dfFuture = dfFuture.join(dfInfl, on=["loc_num", "reported_month"], how="left")

    dfInc = dfFuture.select("date_forecast", "month").distinct() \
        .withColumn("months_out", fun_months_between_exact("date_forecast", "month") + 1) \
        .withColumn("begin_inflation_rate",
                    F.when(F.col("month") < F.lit("2025-05-01").cast("date"), 1.0)
                     .otherwise(fun_annualized_inflation(strScope == "forecast"))) \
        .withColumn("begin_inflation_rate_monthly", F.pow("begin_inflation_rate", 1/12)) \
        .withColumn("begin_inflation_rate_monthly",
                    F.when(F.col("months_out") == 0, 1.0).otherwise(F.col("begin_inflation_rate_monthly"))) \
        .withColumn("end_inflation_rate", F.lit(fun_long_run_inflation())) \
        .withColumn("slope", (F.col("begin_inflation_rate") - F.col("end_inflation_rate")) / 12) \
        .withColumn("M", fun_months_between_exact("date_forecast", F.lit("2026-01-01").cast("date"))) \
        .withColumn("intermediate_inflation_rate", F.col("begin_inflation_rate") + F.col("slope") * (F.col("M") - F.col("months_out"))) \
        .withColumn("intermediate_inflation_rate",
                    F.when(F.col("intermediate_inflation_rate") < F.col("begin_inflation_rate"),
                           F.col("begin_inflation_rate"))
                     .when(F.col("intermediate_inflation_rate") > F.col("end_inflation_rate"),
                           F.col("end_inflation_rate"))
                     .otherwise(F.col("intermediate_inflation_rate"))) \
        .withColumn("intermediate_inflation_rate_monthly", F.pow("intermediate_inflation_rate", 1/12)) \
        .withColumn("intermediate_inflation_rate_monthly",
                    F.when(F.col("months_out") == 0, 1.0).otherwise(F.col("intermediate_inflation_rate_monthly")))

    window_forecast = Window.partitionBy("date_forecast").orderBy("month").rowsBetween(Window.unboundedPreceding, 0)
    dfInc = dfInc.withColumn("incremental_time_inflation",
                             F.exp(F.sum(F.log("intermediate_inflation_rate_monthly")).over(window_forecast))) \
                 .select("month", "date_forecast", "incremental_time_inflation")

    dfFuture = dfFuture.join(dfInc, on=["month", "date_forecast"], how="inner") \
                       .withColumn("inflation_factor", F.col("inflation_factor") * F.col("incremental_time_inflation"))

    dfCann = dfCannibalization.withColumn("loc_num", F.lpad("loc_num", 5, '0')) \
                               .withColumn("month", F.to_date("month"))
    dfFuture = dfFuture.join(dfCann, on=["loc_num", "month"], how="left") \
                       .withColumn("gocf", F.coalesce("gocf", F.lit(0.0)))

    window_fill = Window.partitionBy("loc_num", "date_forecast").orderBy("month").rowsBetween(Window.unboundedPreceding, 0)
    dfFuture = dfFuture.withColumn("gocf", F.last("gocf", True).over(window_fill))

    dfReinvF = dfReinvestFactors.withColumn("loc_num", F.lpad("loc_num", 5, '0')) \
                                .withColumn("month", F.to_date("month")) \
                                .withColumnRenamed("factor", "reinvestment_factor")
    dfFuture = dfFuture.join(dfReinvF, on=["loc_num", "month"], how="left") \
                       .withColumn("reinvestment_factor", F.coalesce("reinvestment_factor", F.lit(0.0))) \
                       .withColumn("reinvestment_factor", F.last("reinvestment_factor", True).over(window_fill))

    dfFuture = dfFuture.withColumn("reinvestment_factor", F.round("reinvestment_factor", 2)) \
                       .withColumn("gocf", F.round("gocf", 15)) \
                       .withColumn("age_years", F.round(F.col("age") / 12, 1)) \
                       .withColumn("age_group",
                                   F.when(F.col("age_years") < 1, "<1")
                                    .when((F.col("age_years") >= 1) & (F.col("age_years") < 5), "1-5")
                                    .when((F.col("age_years") >= 5) & (F.col("age_years") < 10), "5-10")
                                    .when((F.col("age_years") >= 10) & (F.col("age_years") < 15), "10-15")
                                    .when((F.col("age_years") >= 15) & (F.col("age_years") < 20), "15-20")
                                    .when((F.col("age_years") >= 20) & (F.col("age_years") < 30), "20-30")
                                    .when((F.col("age_years") >= 30) & (F.col("age_years") < 50), "30-50")
                                    .otherwise("50+"))

    if dfFuture.filter(F.col("days").isNull()).count() > 0:
        raise Exception("NA day predictions exist")

    final_cols = [
        "loc_num", "month", "date_forecast", "days", "inflation_factor",
        "age", "concept_code", "location_type_code",
        "gocf", "reinvestment_factor", "age_years", "age_group"
    ]
    
    dfFuture = dfFuture.orderBy("loc_num").select(final_cols)
    dfFuture.show(5, truncate=False)
    dfFuture.coalesce(1).write.mode("overwrite").csv("/Users/riddhi_gohil/Desktop/personal_git_repos/r-to-pyspark/output/optmised_temp", header=True)    
    return dfFuture
