# R to PySpark Migration for Sales Forecasting

## Project Overview
This project is about moving old R scripts to PySpark so they can handle much bigger datasets without running into memory or speed problems.

The goal is to predict future sales months for each location, 
- Monthly sales history,
- adjust for store open/close timing,
- Reinvestment projects (shutdowns/reopenings),
- Inflation effects (future price adjustments) and
- Cannibalisation from other stores (nearby store openings)

.. but in a way that's scalable and much faster.

I have kept all the original business logic the same, just made it much more efficient by using Spark.


#### Input Datasets:

`dfSalesDaysFuture`: Open/close dates.
`dfMonthlySales`: Historical monthly sales by location.
`dfReinvestmentProjects`: These are the times when a location was temporarily closed for upgrades.
`dfCannibalizationFactors`: How much do new store openings hurt existing stores?
`dfReinvestmentFactors`: Impact of reinvestments on sales.

---

## Approach 
R did a lot of sequential, in-memory processing, which I wanted to avoid and leverage Spark's distributed nature. 
Earlier, it was not scalable and might have taken longer-running jobs. 

Spark does distributed processing and works better on large volumes of datasets, which works best for this assignment. 

- Use SparkSQL functions
- braodcast small lookups - dayDetails, reinvestments
- Explode function


---


# Links
- [Google Docs Link](https://docs.google.com/document/d/1xHZeZfv7LnM9oVRomR-REU_uL2A_FPLHP9FWz8fXkBc/edit?tab=t.0)
- [Data Excel Sheet](https://docs.google.com/spreadsheets/d/1EMNsodrexqnVZyjBuxHtN6DqBm6QhP7IUQzaRei4u3c/edit?usp=sharing)  
