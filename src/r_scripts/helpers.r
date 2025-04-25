funFillDayDetails <- function(dfSalesMonthly){
    dfDayDetails <- dfSalesMonthly %>%
      mutate(leap_year = leap_year(year(month)),
             jan1_day = weekdays(floor_date(month, "year")),
             month_num = month(month)) %>%
      filter(month + months(1) + days(funGetAcutalsReportedDay() - 1) <= today())
    
    dfDayDetails <- dfDayDetails %>%
      group_by(jan1_day, month_num, leap_year) %>%
      summarise(month = max(month)) %>%
      ungroup() %>%
      inner_join(dfDayDetails, by = c("jan1_day", "month_num", "leap_year", "month")) %>%
      rename(year_ref = month) %>%
      group_by(jan1_day, month_num, leap_year, year_ref) %>%
      summarise(days_max = median(days, na.rm = TRUE)) %>%
      ungroup()

    dfManualAdd <- dfDayDetails %>%
      filter(month_num <= 2,
             leap_year == FALSE,
             jan1_day == "Wednesday") %>%
      mutate(leap_year = TRUE,
             days_max = days_max + ifelse(month_num == 2, 1, 0)) %>%
      rbind(dfDayDetails %>%
              filter(month_num > 2,
                     leap_year == FALSE,
                     jan1_day == "Thursday")%>%
              mutate(leap_year = TRUE,
                     jan1_day = "Wednesday")) %>%
      anti_join(dfDayDetails, by = c("jan1_day", "month_num", "leap_year"))

    # We need to manually append the situation of a leap-year where Jan 1 is a Wednesday
    dfDayDetails <- dfDayDetails %>%
      rbind(dfManualAdd) %>%
      mutate(days_max = as.integer(days_max))

    dfDayDetails <<- dfDayDetails
    blnSetSalesDaysFuture <<- TRUE
  return(dfDayDetails)
}

funReinvestmentProjectsToDaysLost <- function(dfReinvestment, dfSalesMonthly) {
  # Takes a data frame of loc_num, shutdown, reopen and turns it into a data frame of
  # loc_num, month, days_lost

  dfReinvestment <- dfReinvestment %>%
    mutate(month_shutdown = floor_date(shutdown, unit = "month"),
      month_reopen = floor_date(reopen, unit = "month"),
      month_between = funMonthsBetween(month_shutdown, month_reopen))

  dfReinvestment <- dfReinvestment[rep(seq_len(nrow(dfReinvestment)), dfReinvestment$month_between + 1),] %>%
    group_by(loc_num, shutdown) %>%
    mutate(month = month_shutdown + months(row_number() - 1)) %>%
    ungroup() %>%
    select(-month_between) %>%
    mutate(jan1_day = weekdays(floor_date(month, "year")),
      month_num = month(month),
      leap_year = leap_year(year(month))) %>%
    inner_join(funFillDayDetails(dfSalesMonthly),
      by = c("jan1_day", "month_num", "leap_year")) %>%
    select(-jan1_day, -month_num, -leap_year, -year_ref) %>%
    mutate(days_lost = ifelse(month != month_shutdown & month != month_reopen,
      days_max,
      ifelse(month == month_shutdown,
        funSalesDaysBetween(shutdown, pmin(month_shutdown + months(1) - days(1), reopen)),
        funSalesDaysBetween(month_reopen, reopen)))) %>%
    ungroup() %>%
    select(loc_num, month, days_lost)

  # Consolidate duplicate reinvestment project lost days in each month by taking the max lost
  dfReinvestment <- dfReinvestment %>%
    group_by(loc_num, month) %>%
    summarise(days_lost = max(days_lost)) %>%
    ungroup()

  return(dfReinvestment)
}

funSalesDaysBetween <- function(vecStart, vecEnd){
  # a function to calculate the number of non sundays between start and end date vectors of equal sizes

  vecNa <- which(is.na(vecStart) | is.na(vecEnd))
  # Fill these in with the same dates for simplicity
  vecStart[vecNa] <- ymd("2000-01-01")
  vecEnd[vecNa] <- ymd("2000-01-01")

  if(any(vecStart > vecEnd)) {
    print("Some of your end dates are before your start dates")
    quit(save = "no", status = 1)
  }
  vecStart <- vecStart %>% as.Date()
  vecEnd <- vecEnd %>% as.Date()

  lstDaysBetween <- mapply(seq.Date, from = vecStart, to = vecEnd, by = "days", SIMPLIFY = FALSE)
  return(lapply(lstDaysBetween, function(x) sum(!weekdays(x) == "Sunday")) %>% unlist())
}

funMonthsBetween <- function(strMonthStart, strMonthEnd){
  return(12 * (year(strMonthEnd) - year(strMonthStart)) +
           (month(strMonthEnd) - month(strMonthStart)))
}

funGetAcutalsReportedDay <- function(){
  # On what day of the month do we assume all actual monthly sales figures are known?
  return(16)
}

funGetOpeningVision <- function(){
  # How many months in advance will we know specifically which restaurants will be opening?
  return(12)
}

funGetClosingVision <- function(){
  # How many months in advance will we know specifically which restaurants will be closing?
  return(12)
}

funGetReinvestmentVision <- function(){
  # How many months in advance will we know specifically which restaurants will receiving a
  # reinvestment project
  return(12)
}

funAnnualizedInflation <- function(blnLiveForecast = TRUE){
  # What annualized inflation will we assume going forward?
  # This is expressed as a number > 1
  # This value was recommended in consultation with Brett Sorenson on the pricing team as the baseline
  # for annualized inflation during 2025
  if (blnLiveForecast) return(1.01)
  else return(1.05222)
}

funLongRunInflation <- function(){
  # What annualized inflation will we assume in the long run? This is based on market inflation expectations
  # This is expressed as a number > 1
  # This value was chosen in consultation with Brett Sorensen on the Pricing team based on inflation of
  # the "food away from home" category, which is subject to political direction around minimum wage
  return(1.025)
}
