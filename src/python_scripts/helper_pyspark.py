from pyspark.sql.functions import (
    col, lit, expr, month, year, trunc, date_format, size, sequence,
    explode, when, dayofweek, broadcast
)
from pyspark.sql import functions as F
from pyspark.sql import Window

def fun_fill_day_details(df_sales_monthly):
    df = df_sales_monthly \
        .withColumn("leap_year", ((year("month") % 4 == 0) & ((year("month") % 100 != 0) | (year("month") % 400 == 0)))) \
        .withColumn("jan1_day", date_format(trunc("month", "YEAR"), "EEEE")) \
        .withColumn("month_num", month("month")) \
        .filter(expr(f"add_months(month, 1) + interval {fun_get_actual_reported_day() - 1} days <= current_date()"))

    window_spec_latest = Window.partitionBy("jan1_day", "month_num", "leap_year")
    df_with_latest = df.withColumn("year_ref", F.max("month").over(window_spec_latest))

    df_main = df_with_latest.groupBy("jan1_day", "month_num", "leap_year", "year_ref") \
        .agg(F.expr("percentile_approx(days, 0.5, 10000)").cast("int").alias("days_max"))

    df_manual_add = df_main.filter(
        ((col("month_num") <= 2) & (~col("leap_year")) & (col("jan1_day") == "Wednesday")) |
        ((col("month_num") > 2) & (~col("leap_year")) & (col("jan1_day") == "Thursday"))
    ).withColumn(
        "leap_year", lit(True)
    ).withColumn(
        "jan1_day", when((col("month_num") > 2) & (col("jan1_day") == "Thursday"), lit("Wednesday")).otherwise(col("jan1_day"))
    ).withColumn(
        "days_max", col("days_max") + when((col("month_num") == 2), 1).otherwise(0)
    )
    
    # can change later depedinding on the size of the df_main. 
    # should or should not broadcast the df_main
    df_manual_final = df_manual_add.join(broadcast(df_main), ["jan1_day", "month_num", "leap_year"], "anti")
    
    final_df = df_main.unionByName(df_manual_final)

    print("\n\nfun_fill_day_details:")
    final_df.orderBy("jan1_day", "month_num", "leap_year").show(5, truncate=False)
    return final_df

def fun_reinvestment_days_lost(df_reinvestment, df_sales_monthly):
    df_day_details = fun_fill_day_details(df_sales_monthly)

    df = df_reinvestment \
        .withColumn("month_shutdown", trunc("shutdown", "month")) \
        .withColumn("month_reopen", trunc("reopen", "month")) \
        .withColumn("month_between", fun_months_between_exact("month_shutdown", "month_reopen")) \
        .withColumn("repeat_idx", explode(sequence(lit(0), col("month_between")))) \
        .withColumn("month", expr("add_months(month_shutdown, repeat_idx)")) \
        .withColumn("jan1_day", date_format(trunc("month", "year"), "EEEE")) \
        .withColumn("month_num", month("month")) \
        .withColumn("leap_year", ((year("month") % 4 == 0) & ((year("month") % 100 != 0) | (year("month") % 400 == 0)))) \
        # .join(df_day_details, on=["jan1_day", "month_num", "leap_year"], how="inner") \
        # .drop("month_idx", "month_between")
    
    df = df.join(broadcast(df_day_details), on=["jan1_day", "month_num", "leap_year"], how="inner")
        
    df = df.withColumn(
        "days_lost",
        when(
            (col("month") != col("month_shutdown")) & (col("month") != col("month_reopen")),
            col("days_max")
        ).when(
            col("month") == col("month_shutdown"),
            size(expr("""
                filter(
                    sequence(shutdown, least(last_day(month_shutdown), reopen)), 
                    x -> date_format(x, 'EEEE') != 'Sunday'
                )
            """))
        ).when(
            col("month") == col("month_reopen"),
            size(expr("""
                filter(
                    sequence(month_reopen, reopen), 
                    x -> date_format(x, 'EEEE') != 'Sunday'
                )
            """))
        ).otherwise(0)
    )

    final_df = df.groupBy("loc_num", "month").agg(F.max("days_lost").alias("days_lost"))
    print("\n\nfun_reinvestment_days_lost:")
    final_df.orderBy("loc_num", "month").show(5, truncate=False)
    return final_df

def fun_get_actual_reported_day(): return 16
def fun_get_opening_vision(): return 12
def fun_get_closing_vision(): return 12
def fun_get_reinvestment_vision(): return 12
def fun_annualized_inflation(live_forecast=True): return 1.01 if live_forecast else 1.05222
def fun_long_run_inflation(): return 1.025

def fun_sales_days_between(start_col, end_col):
    return size(
        F.filter(
            sequence(start_col, end_col),
            lambda x: dayofweek(x) != 1  
        )
    )

def fun_months_between_exact(start_col, end_col):
    return ((year(end_col) - year(start_col)) * 12 + (month(end_col) - month(start_col))).cast("int")
