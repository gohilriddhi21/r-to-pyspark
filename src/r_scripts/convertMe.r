convertMe <- function(
    dfSalesDaysFuture, 
    dfSalesMonthly, 
    dfReinvestmentProjects, 
    dfCannibalization, 
    dfReinvestFactors,
    strScope = "forecast") {
  
  dfDayDetails <- funFillDayDetails(dfSalesMonthly)

  strDateAsOf <- dfSalesDaysFuture$date_forecast
  strLocationNum <- dfSalesDaysFuture$loc_num
  nMonths = dfSalesDaysFuture$months_predict

  nMonthStart <- 0
  nMonthStart <- ifelse(day(ymd(strDateAsOf)) < funGetAcutalsReportedDay(),
    nMonthStart - 1,
    nMonthStart)

  # If they haven't opened, we'll need to filter on that
  if(any(is.na(dfSalesDaysFuture$open_date))) {
    print("You are missing some open dates")
    quit(save = "no", status = 1)
  }

  strOpenMonth <- ifelse(is.na(dfSalesDaysFuture$open_date),
    ymd("1900-01-01"),
    floor_date(dfSalesDaysFuture$open_date, "month")) %>% as.Date()

  # If we could've known or do know about their future closing, only return the correct number of months
  strCloseMonth <- ifelse(is.na(dfSalesDaysFuture$close_date),
    ymd("2200-01-01"),
    ifelse(ymd(strDateAsOf) + months(funGetClosingVision()) > dfSalesDaysFuture$close_date,
      strCloseMonth <- floor_date(dfSalesDaysFuture$close_date, "month"),
      ymd("2200-01-01"))) %>% as.Date()

  #lstFutureMonths <- lapply(strDateAsOf, function(x) floor_date(x, "month") + months(c(nMonthStart:(nMidTermMonths - 1))))
  lstAddMonths <- mapply(function(x, y) seq(from = x, to = (y - 1)),
    x = nMonthStart,
    y = nMonths,
    SIMPLIFY = FALSE)
  vecLengths <- lapply(lstAddMonths, length) %>% unlist()
  vecFutureMonths <- rep(floor_date(ymd(strDateAsOf), "month"), times = vecLengths) + months(unlist(lstAddMonths))

  # This should come from Location ABT, and inadvertently does based on
  # strLocationNum and dfSalesDaysFuture
  dfSalesDaysFuture <-
    data.frame(
      loc_num = rep(strLocationNum, times = vecLengths),
      month = vecFutureMonths,
      date_forecast = rep(strDateAsOf, times = vecLengths),
      open_date = rep(dfSalesDaysFuture$open_date, times = vecLengths),
      month_open = rep(strOpenMonth, times = vecLengths),
      close_date = rep(dfSalesDaysFuture$close_date, times = vecLengths),
      month_close = rep(strCloseMonth, times = vecLengths),
      jan1_day = weekdays(floor_date(vecFutureMonths, "year")),
      month_num = month(vecFutureMonths),
      leap_year = leap_year(year(vecFutureMonths)),
      concept_code = rep(dfSalesDaysFuture$location_type_code, times = vecLengths),
      location_type_code = rep(dfSalesDaysFuture$location_type_code,
        times = vecLengths),
      price_group =  rep(dfSalesDaysFuture$price_group, times = vecLengths),
      stringsAsFactors = FALSE
    ) %>%
    filter(month <= month_close, month >= month_open) %>%
    inner_join(dfDayDetails, by = c("jan1_day", "month_num", "leap_year")) %>%
    select(-leap_year,-jan1_day,-year_ref) %>%
    # Modify OCLs
    left_join(
      dfSalesMonthly %>%
        group_by(loc_num) %>%
        mutate(
          nMonthsHistory = n(),
          month_num = month(month)
        ) %>%
        filter(nMonthsHistory >= 12, # As least 12 months history
          # Drop everything but last 12 months
          row_number() >= nMonthsHistory - 12 + 1) %>%
        ungroup() %>%
        select(loc_num, days, month_num) %>%
        unique(),
      by = c("loc_num", "month_num")
    ) %>%
    mutate(days_max = ifelse(
      location_type_code == "OCL" &
        !is.na(location_type_code),
      days %>% coalesce(days_max) %>% pmin(days_max),
      days_max)) %>%
    select(-days)

  vecNaLocNums <- dfSalesDaysFuture %>%
    filter(is.na(location_type_code) | is.na(concept_code)) %>%
    .$loc_num %>%
    unique()
  if(length(vecNaLocNums) > 0) {
    print("There are some NA location type codes or concept codes")
    quit(save = "no", status = 1)
  }

  dfOpenCloseDaysLost <- dfSalesDaysFuture %>%
    group_by(loc_num) %>%
    filter(row_number() == 1) %>%
    ungroup() %>%
    mutate(days_before_open = funSalesDaysBetween(floor_date(open_date, "month"), open_date),
      days_after_close = funSalesDaysBetween(close_date, floor_date(close_date, "month") + months(1) - days(1))) %>%
    select(loc_num, days_before_open, days_after_close)

  dfSalesDaysFuture <- dfSalesDaysFuture %>%
    left_join(dfReinvestmentProjects %>%
        mutate(shutdown = ymd(shutdown),
          reopen = ymd(reopen)) %>%
        funReinvestmentProjectsToDaysLost(dfSalesMonthly = dfSalesMonthly),
      by = c("month", "loc_num")) %>%
    left_join(dfOpenCloseDaysLost,
      by = "loc_num") %>%
    mutate(days_lost = coalesce(days_lost, as.integer(0)),
      days_before_open = coalesce(days_before_open, as.integer(0)),
      days_after_close = coalesce(days_after_close, as.integer(0)),
      days_max = days_max - days_lost,
      days_max = ifelse(month == month_open,
        days_max - (days_before_open - 1), # -1 because we don't want to subtract the day it opens
        days_max),
      days_max = ifelse(month == month_close,
        days_max - days_after_close,
        days_max),
      days_max = pmax(0, days_max),
      age = funMonthsBetween(month_open, month)) %>%
    rename(days = days_max) %>%
    select(-days_lost, -open_date, -close_date,  -month_open, -month_close,
      -days_before_open, -days_after_close)

  # Finally, append inflation factors based on the latest available information at that time
  dfSalesDaysFuture <- dfSalesDaysFuture %>%
    mutate(reported_month = floor_date(ymd(date_forecast) - days(funGetAcutalsReportedDay() - 1), "month") - months(1)) %>%
    left_join(dfSalesMonthly %>%
        # We want ending inflation factor because that's where all future months will start
        select(loc_num, month, inflation_factor_ending) %>%
        rename(inflation_factor = inflation_factor_ending),
      by = c("loc_num", "reported_month" = "month"))

  # Create a data frame of inflation rates for each out month
  # Lower near-term inflation rate hard-coded for 2025, then transitions to long-term rate by 2027
  dfIncrementalInflation <- dfSalesDaysFuture %>%
    select(month, date_forecast) %>%
    unique() %>%
    group_by(date_forecast) %>%
    mutate(months_out = funMonthsBetween(date_forecast, month) + 1,
      begin_inflation_rate = if_else(month < ymd("2025-05-01"), 1.0,
                                     funAnnualizedInflation(strScope == "forecast")),
      begin_inflation_rate_monthly = begin_inflation_rate ^ (1/12),
      begin_inflation_rate_monthly = ifelse(months_out == 0, 1, begin_inflation_rate_monthly),
      end_inflation_rate = funLongRunInflation(),
      slope = (begin_inflation_rate - end_inflation_rate) / 12,
      intermediate_inflation_rate = pmin(
        pmax(
          begin_inflation_rate + slope *
            (funMonthsBetween(date_forecast, ymd("2026-01-01")) - months_out),
          begin_inflation_rate),
        end_inflation_rate),
      intermediate_inflation_rate_monthly = intermediate_inflation_rate ^ (1/12),
      intermediate_inflation_rate_monthly = ifelse(months_out == 0, 1, intermediate_inflation_rate_monthly),
      incremental_time_inflation = cumprod(intermediate_inflation_rate_monthly)) %>%
    select(month, date_forecast, incremental_time_inflation) %>%
    ungroup()

  # During this date range, we'll overwrite the above logic based on the November 2023 price change
  dtPriceChange <- ymd("2024-07-01")
  dtMonthPriceChange <- floor_date(dtPriceChange, "month")
  # if we having 6 month pricing windows, we need to start linearly increasing pricing before the
  # next window arrives
  nMonthsBeforeNextInc <- 5
  nRealizedPriceInc <- 2/3 # how much of this price increase should be realized?
  nMonthsShiftNextInc <- funMonthsBetween(min(dfSalesDaysFuture$month),
    dtPriceChange + months(nMonthsBeforeNextInc)) %>%
    max(0)
  # you know about it about 3 months in advance
  dtPriceChangeKnown <- dtPriceChange - days(3 * 30)
  # the price change is reflected in the sales data the following month
  dtPriceChangeRealized <- dtMonthPriceChange +
    months(1) +
    days(funGetAcutalsReportedDay() - 1)
  nMonthChangeSalesDays <- dtMonthPriceChange %>%
    funSalesDaysBetween(dtMonthPriceChange + months(1) - days(1))
  nMonthChangeAffectedSalesDays <-  dtPriceChange %>%
    funSalesDaysBetween(dtMonthPriceChange + months(1) - days(1))
  if (strDateAsOf[1] >= dtPriceChangeKnown & strDateAsOf[1] < dtPriceChangeRealized) {
    library(readr)

    dfSalesDaysFuture <- dfSalesDaysFuture %>%
      group_by(reported_month, concept_code) %>%
      # Fill in missing inflation factors with the average we would've known about at the time
      mutate(inflation_factor = ifelse(is.na(inflation_factor),
        mean(inflation_factor, na.rm = TRUE),
        inflation_factor)) %>%
      ungroup() %>%
      inner_join(dfIncrementalInflation,
        by = c("month", "date_forecast"))
      # Only part of April affected. Nothing before April affected
      mutate(inc = ifelse(is.na(inc), NA,
        ifelse(month > dtPriceChange, inc,
          ifelse(month == dtMonthPriceChange,
            inc * nMonthChangeAffectedSalesDays / nMonthChangeSalesDays,
            0))),
        # If you've already learned about the price increase, we don't need "inc" anymore
        # At that point, we just need to keep prices constant until November (happening below) wigh lag(incremental_time_inflation)
        inc = ifelse(date_forecast >= dtPriceChangeRealized, 0, inc)) %>% 
      # If they're receiving pricing in April, shift their further increases back to November
      # If they're not getting pricing in April, keep them on the same schedule
      group_by(loc_num) %>%
      mutate(incremental_time_inflation = ifelse(is.na(inc),
        lag(incremental_time_inflation,
          nMonthsShiftNextInc,
          default = 1),
        (inc + 1) * lag(incremental_time_inflation,
          nMonthsShiftNextInc,
          default = 1))) %>% 
      ungroup() %>%
      # Annualized inflation, turned monthly,
      # applied by the number of months between now and the month we're forecasting
      mutate(improved_rolling_inflation = inflation_factor * incremental_time_inflation,
        inflation_factor = improved_rolling_inflation) %>%
      select(loc_num, month, date_forecast, days, inflation_factor, age, concept_code, location_type_code)
  } else {
    dfSalesDaysFuture <- dfSalesDaysFuture %>%
      group_by(reported_month, concept_code) %>%
      # Fill in missing inflation factors with the average we would've known about at the time
      mutate(inflation_factor = ifelse(is.na(inflation_factor),
        mean(inflation_factor, na.rm = TRUE),
        inflation_factor)) %>%
      ungroup() %>%
      inner_join(dfIncrementalInflation,
        by = c("month", "date_forecast")) %>%
      # Annualized inflation, turned monthly, applied by the number of months between now and the month we're forecasting
      group_by(loc_num) %>%
      mutate(improved_rolling_inflation = inflation_factor *
        lag(incremental_time_inflation,
          nMonthsShiftNextInc,
          default = 1),
        inflation_factor = improved_rolling_inflation) %>%
      ungroup() %>%
      select(loc_num, month, date_forecast, days, inflation_factor, age, concept_code, location_type_code)
  }

  if(any(is.na(dfSalesDaysFuture$inflation_factor))){
    print("You are missing inflation factors for some date_forecast-location-month combinations")
    quit(save = "no", status = 1)
  }
  if(max(dfSalesDaysFuture$inflation_factor) > 10){
    print("At least one of your calculated inflation factors seems too high. Please investigate")
    quit(save = "no", status = 1)
  }
  dfGOCF <- dfCannibalization %>%
    mutate(loc_num = str_pad(loc_num, 5, pad = "0"),
      month = ymd(month))
  dfSalesDaysFuture <- dfSalesDaysFuture %>%
    left_join(dfGOCF, by = c("loc_num", "month")) %>%
    mutate(gocf = coalesce(gocf, 0),
      # If we're backtesting, some of these grand openings we couldn't have
      # known about at the time of the forecast
      # so we carry forward their last-known value
      gocf = ifelse(strScope == "forecast" |
          month < ymd(date_forecast) + months(funGetOpeningVision()),
        gocf, NA),
      gocf = as.double(gocf)) %>%
    group_by(loc_num, date_forecast) %>%
    fill(gocf) %>%
    ungroup()

  dfReinvestFactors <- dfReinvestFactors %>%
    mutate(loc_num = str_pad(loc_num, 5, pad = "0"),
      month = ymd(month))
  dfSalesDaysFuture <- dfSalesDaysFuture %>%
    left_join(dfReinvestFactors %>%
        rename(reinvestment_factor = factor), by = c("loc_num", "month")) %>%
    mutate(reinvestment_factor = coalesce(reinvestment_factor, 0),
      # If we're backtesting, some of these reinvestment projects we couldn't have
      # known about at the time of the forecast
      # so we carry forweard their last-known value
      reinvestment_factor = ifelse(strScope == "forecast" |
          month < ymd(date_forecast) + months(funGetReinvestmentVision()),
        reinvestment_factor, NA)) %>%
    group_by(loc_num, date_forecast) %>%
    fill(reinvestment_factor) %>%
    ungroup()

  # create age groupings (bins)
  lstBreaks <- c(0,1,5,10,15,20,30,50,100)
  lstLabels <- c('<1','1-5','5-10','10-15','15-20','20-30','30-50','50+')
  dfSalesDaysFuture$age_years <- round(dfSalesDaysFuture$age / 12, digits=1)
  dfSalesDaysFuture$age_group <- as.character(cut(dfSalesDaysFuture$age_years,
    breaks=lstBreaks,
    labels=lstLabels,
    right=FALSE))

  if(any(is.na(dfSalesDaysFuture$days))){
    print("You have NA day predictions. You shouldn't have any. These locations will get dropped in forecasting if not addressed.")
    quit(save = "no", status = 1)
  }

  print(head(dfSalesDaysFuture, 5))  # PRINT FIRST 5 ROWS
  return(dfSalesDaysFuture)
}