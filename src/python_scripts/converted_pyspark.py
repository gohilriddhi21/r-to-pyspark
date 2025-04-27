from pyspark.sql import functions as F
from pyspark.sql.window import Window
import helper_pyspark as hp

def convert_me(dfSalesDaysFuture, dfSalesMonthly, dfReinvestmentProjects, dfCannibalization, dfReinvestFactors, strScope="forecast"):
    dfDayDetails = hp.fun_fill_day_details(dfSalesMonthly)

    reported_day = hp.fun_get_actual_reported_day()

    dfExpanded = dfSalesDaysFuture \
        .withColumn("month_open", F.trunc("open_date", "month")) \
        .withColumn("month_close", F.when(
            F.col("close_date").isNull() | (F.date_add("date_forecast", 360) < F.col("close_date")),
            F.lit("2200-01-01").cast("date")
        ).otherwise(F.trunc("close_date", "month"))) \
        .withColumn("nMonthStart", F.when(F.dayofmonth("date_forecast") < reported_day, F.lit(-1)).otherwise(F.lit(0))) \
        .withColumn("months_array", F.expr("sequence(nMonthStart, months_predict - 1)")) \
        .withColumn("month", F.explode(F.expr("transform(months_array, x -> add_months(trunc(date_forecast, 'month'), x))"))) \
        .filter((F.col("month") >= F.col("month_open")) & (F.col("month") <= F.col("month_close"))) \
        .withColumn("jan1_day", F.date_format(F.trunc("month", "year"), "EEEE")) \
        .withColumn("month_num", F.month("month")) \
        .withColumn("leap_year", ((F.year("month") % 4 == 0) & ((F.year("month") % 100 != 0) | (F.year("month") % 400 == 0)))) \
        .join(dfDayDetails, ["jan1_day", "month_num", "leap_year"], "inner")

    salesMonthlyWindow = Window.partitionBy("loc_num").orderBy(F.col("month").desc())
    dfHistory = dfSalesMonthly \
        .withColumn("month_num", F.month("month")) \
        .withColumn("rank", F.row_number().over(salesMonthlyWindow)) \
        .filter("rank <= 12") \
        .select("loc_num", "month_num", "days") \
        .distinct()

    dfExpanded = dfExpanded.join(dfHistory, ["loc_num", "month_num"], "left") \
        .withColumn("days_max", F.when((F.col("location_type_code") == "OCL") & (F.col("days").isNotNull()), F.least("days", "days_max")).otherwise("days_max"))

    dfOpenCloseDaysLost = dfExpanded.groupBy("loc_num").agg(
        hp.fun_sales_days_between(
            F.first(F.trunc("month_open", "month")),
            F.first("open_date")
        ).alias("days_before_open"),
        hp.fun_sales_days_between(
            F.first("close_date"),
            F.last_day(F.first("month_close"))
        ).alias("days_after_close")
    )

    # Adjust for Reinvestment and Open/Close days
    dfExpanded = dfExpanded \
        .join(hp.fun_reinvestment_days_lost(dfReinvestmentProjects, dfSalesMonthly), ["loc_num", "month"], "left") \
        .join(dfOpenCloseDaysLost, ["loc_num"], "left") \
        .fillna(0, subset=["days_lost", "days_before_open", "days_after_close"]) \
        .withColumn("days", (F.col("days_max") - F.col("days_lost")
            - F.when(F.col("month") == F.col("month_open"), F.col("days_before_open") - 1).otherwise(0)
            - F.when(F.col("month") == F.col("month_close"), F.col("days_after_close")).otherwise(0)).cast("int")) \
        .withColumn("age", hp.fun_months_between_exact("month_open", "month"))

    sales_monthly_alias = dfSalesMonthly.select(
        F.col("loc_num").alias("sales_loc_num"),
        F.col("month").alias("sales_month"),
        F.col("inflation_factor_ending").alias("inflation_factor"),
        F.col("concept_code").alias("sales_concept_code")
    )

    dfExpanded = dfExpanded \
        .withColumn("reported_month", F.add_months(F.trunc(F.date_sub("date_forecast", reported_day - 1), "month"), -1)) \
        .join(
            sales_monthly_alias,
            (F.col("loc_num") == F.col("sales_loc_num")) & (F.col("reported_month") == F.col("sales_month")),
            "left"
        ) \
        .withColumn("concept_code", F.col("sales_concept_code"))  # restoring concept_code after join


    inflationWindow = Window.partitionBy("reported_month", "concept_code")
    dfExpanded = dfExpanded.withColumn("inflation_factor", F.coalesce("inflation_factor", F.avg("inflation_factor").over(inflationWindow)))

    fillWindow = Window.partitionBy("loc_num", "date_forecast").orderBy("month").rowsBetween(Window.unboundedPreceding, 0)

    dfExpanded = dfExpanded \
        .join(dfCannibalization.select("loc_num", "month", "gocf"), ["loc_num", "month"], "left") \
        .withColumn("gocf", F.last("gocf", ignorenulls=True).over(fillWindow)) \
        .join(dfReinvestFactors.select("loc_num", "month", F.col("factor").alias("reinvestment_factor")), ["loc_num", "month"], "left") \
        .withColumn("reinvestment_factor", F.last("reinvestment_factor", ignorenulls=True).over(fillWindow))

    dfExpanded = dfExpanded.withColumn("age_years", F.round(F.col("age")/12, 1))

    dfExpanded = dfExpanded.withColumn("age_group", 
        F.when(F.col("age_years") < 1, "<1")
         .when((F.col("age_years") >= 1) & (F.col("age_years") < 5), "1-5")
         .when((F.col("age_years") >= 5) & (F.col("age_years") < 10), "5-10")
         .when((F.col("age_years") >= 10) & (F.col("age_years") < 15), "10-15")
         .when((F.col("age_years") >= 15) & (F.col("age_years") < 20), "15-20")
         .when((F.col("age_years") >= 20) & (F.col("age_years") < 30), "20-30")
         .when((F.col("age_years") >= 30) & (F.col("age_years") < 50), "30-50")
         .otherwise("50+")
    )

    dfExpanded.show(5, False)  # For debugging purposes
    finalCols = ["loc_num", "month", "date_forecast", "days", "inflation_factor", "age", "concept_code", "location_type_code", "gocf", "reinvestment_factor","age_years","age_group"]
    return dfExpanded.select(finalCols)