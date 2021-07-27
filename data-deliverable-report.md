# Data Deliverable Report - The Best Group™

**Team members**: John Wu, Kazen Gallman, Michael Kearney, Matthew Ji

## Step 1

1. Your data collection description goes here (at most around 200 words)

We had to acquire 5 pieces of data, turnstile data for the number of entries into a train station per week, covid data for caserate per week in new york for each zipcode, mapping training stations to zip code, and demographics for zipcodes

### Turnstile
We used web scraping with requests and Beautiful Soup to open every csv link on http://web.mta.info/developers/turnstile.html to get the weekly ridership data. 
Then we used Pandas to read the csv data and created a Dataframe. We appended the Week the data was from as a column in the new dataframe. Then we aggregated the data by Station using groupby() and found the change in ridership for that station in that week by taking the Max Cumulative ridership - Min Cumulative ridership per station per week using agg(). Finally we stored that dataframe as a SQL table using to_sql(). 

### Covid Case Rate Data
We imported a csv file from https://github.com/nychealth/coronavirus-data/blob/master/trends/caserate-by-modzcta.csv, into the python library pandas into a dataframe. Then transposed the data to be in terms of weeks, and after looped through the entries to get a new dataframe in the form [ZIP, DAY, MONTH, YEAR, CASERATE]. After this was exported using a pandas function to_sql(), to make a sqlite3 database table.

### Mapping training stations to zipcode
This was done manually by finding the corresponding station ID and searching for the zip code that it matched too.

### Demographics for zipcode
We imported a csv file from https://data.cccnewyork.org/data/table/66/median-incomes#66/107/62/a/a and used a python library called pandas to put it into a dataframe. Then the data was edited to only include the zip code and median income columns.

2. Your data commentary goes here (at most around 200 words)

### Turnstile
We grabbed the data from the MTA (the official transit system for NYC) which is a reputable source, they would have the official access to the information. However there are some problems. The count of entries and exits to the station are cumulative so in order to figure out how many people entered or exited from the station in a certain time frame we have to completely reformat the data.  

### Covid Case Rate Data
The data is from NYC Department of Health and Mental Hygiene which is a reliable source and the information is given public. One thing we noticed is that they give percentages for caserate, so we have to make sure we get the turnstile data into percentages too. Furthermore we had to frame this study for the second wave of the pandemic due unavailability of data for the beginning of the pandemic

### Mapping Training Stations to zip code
Since the zip codes were manually found for each station we believe the data to be very accurate as it relies on google's accuracy for zipcode 
Demographics for zip code

The data is from the citizens committee for children of new york which is a child advocacy organization. While not a part of the government this in some ways makes it less biased, as they can deliver data without any bias from politicians. The government is well known and has been running for 75 years. They are known for collecting data and statistics like this, as it relates to other data like parentual situations, and more.


## Step 2

Your data schema description goes here (at most around 300 words)


All fields are required, and missing fields will have their rows dropped, as the information will have been needed to compute the processed table. 

Subway Station Table, Covid Cases Table, and Station To Zip, are used to make the Processed Table, which will then be what is used for analysis.

### Subway station table

**STATION (PRIMARY KEY)**
STRING of station name from turnstile dataset
**DAY (PRIMARY KEY)**
INT that represents day from DD/MM that the week started at
**MONTH (PRIMARY KEY)**
INT that represents month from DD/MM that the week started at
**YEAR (PRIMARY KEY)**
INT that represents year that station ridership data was collected from
**RIDERSHIP_DIF**
difference between of ridership of turnstile for:
March 2019 - March 2020 vs March 2020 - March 2021

### Covid cases table
**ZIP CODE (Primary Key, String)**
STRING for zip code that covid caserate corresponds to
**DAY (PRIMARY KEY)**
INT that represents day from DD/MM that the week started at
**MONTH (PRIMARY KEY)**
INT that represents month from DD/MM that the week started at
**YEAR INT (PRIMARY KEY)**
INT that represents year that station ridership data was collected from
**CASERATE**
FLOAT for rate of confirmed cases per 100,000 people in the given zip code over the week starting at the day/month during 2020-2021
**Station To Zip**
STATION (Primary Key)
STRING of a Station name that matches stations from the turnstile dataset
**ZIP CODE**
STRING for zip code of the station as a String

### Demographics
**ZIP CODE (Primary Key)**
String of location that the following columns come from
**MEDIAN INCOME**
INT for Median income in dollars of residents in zip code

### Processed
**ZIP CODE**
STRING that represents the zipcode for the data collected
**DAY**
INT that represents day from DD/MM that the week started at
**MONTH**
INT that represents month from DD/MM that the week started at
**CASERATE**
FLOAT for rate of confirmed cases per 100,000 people in the given zip code over the week starting at the day/month during 2020-2021
**DIFFERENCE IN RIDERSHIP**
FLOAT for percent Difference in average ridership of all stations at that zipcode between weeks from 08/08/2020 to 07/03/2021 and weeks 08/08/2019 to 07/03/2020

## Step 3

1. Your observation of the data prior to cleaning the data goes here (limit: 250 words)

### Covid Case Rate
For the Covid Case Rate it was found by creating a box plot that out of the 8496 entries, 107 of them were outliers, as they were outside the bounds of the whiskers of the boxplot meaning they were 1.5x the interquartile lower than the the first quartile or 1.5x the interquartile range greater than the 3rd quartile. Other than this there were no duplicates or issues with the dataset

### Turnstile
The data of the entries and exits through each turnstile is cumulative so we have to subtract different entries at the same turnstile but at different times to figure out the amount of people who went through each turnstile during that time period. Looking at the distribution of ridership for each station and for each week, the station distribution seems exponential and the weekly distribution is pretty uniform which matches my intuition [See Turnstile_Station_dist and Turnstile_Weekly_dist]. 

### Station To Zip
Only used as a way to map stations to zipcodes, and so there were no issues here

### Demographics
By creating a boxplot out of the median incomes, it was found that out of the 722 entries for 722 zip codes, there were 73 outliers. This was done by creating a boxplot and checking for values outside the whiskers of the boxplot

### Processed
Due to the cleaning done in the previous steps this dataset is just a combination of the cleaned data so there were no issues here.

2. Your data cleaning process and outcome goes here (at most around 250 words)

### Covid Case Rate
So since there were outliers, which can be explained as error in recording the values, or variation that does not describe the dataset as a whole, these values were removed. This leaves the dataset with 8389 entries left of 8496 , meaning only about 1.2594% of the data from the original dataset was removed. Overall this showcases that the data should still be able to be representative even with the small amount of outliers removed, as the values left are representative. It is worth noting though that some of the outliers may have been useful, as it is possible that the outliers were in a weekly spike that could have given useful results when compared to the turnstile data. The distribution did not change change that much as the mean went from 134.524368 to 128.123898, and the standard deviation from 136.142373 to 123.484150 which shows that overall the outliers were not that useful other than lowering the max from 1473.790000 to 512.840000.

### Turnstile
The only real cleaning that was done was to group each station by week and take the max cumulative value and subtract the min cumulative value to get the ridership at that turnstile for that week. 

### Station To Zip
No cleaning was done here as the data only needed to include the zip code corresponding to the station

### Demographics
After taking 73 of the outliers out of the data set, there were 649 data points left. This was about 10% of the data. The distribution did have a meaningful change as its mean went from 77864 to 92842 and its standard deviation went from 92842 to 54688, which points toward the outliers having had a large effect on the data.

### Processed
The processed dataset only removes values where there is not a ridership difference percent and a caserate for that week, if only or none of them exist that row is deleted. This could happen as outliers are taken out in the covid caserate and the turnstile data, so certain weeks may be missing data.



## Step 4

Your train-test data descriptions goes here (at most around 250 words)

Because we are doing hypothesis testing for our project, analyzing data over a small dataset would not be reliable, therefore we are considering our “train data” in this response in the scope of all of what we collected and cleaned. In total, we have about 8000 rows estimated of differences. If we were to create a test set it would be done as a way to test our model. In other words we would take for example 20% of the data. And then after creating our model, we could use a model to check if those test rows matched in predicted ridership percent difference based on the covid caserate at the time.

In terms of hypothesis testing, we collected a table of covid caserate and train ridership for each region (zip code) in New York. This data will be paired together to use under a paired-t-test. The null hypothesis will be that there is not a significant difference in ridership difference percent and the case rate of covid at the time, while the alternative hypothesis with an alpha value of 0.05 will be that there is a significant difference in ridership difference percent and case rate of covid at the time.

As a second test we are interested in the topic of median income in a household vs the covid percentage for a specific region. This will be put under a continuous chi squared test (2-way ANOVA) to test if they are independent or not. The null hypothesis will be that covid case percentage does not affect median income of an area, while the alternative hypothesis is that covid case rate does affect the median income of an area. There will be an alpha value of 0.05.

## Step 5

Your socio-historical context & impact report goes here

The major stakeholders of the study would likely be people who have a vested interest in understanding how covid-19 affected transportation across different locations around NYC. Our dataset represents public transportation use across different zip codes, as such the people this project is most likely to affect would be communities in these specific zip codes. 

Based on the data that we are collecting (Number of people entering through the turnstiles, and covid rates based on zip code), there doesn’t seem to be any significant privacy issues currently. 

One underlying bias that was in our database was testing rate among the population of different zip codes. More cases could have been found in wealthier zip codes because they are more likely to have the spare resources to get early testing, leading to more cases. To mitigate this we looked at the case rate (percent) rather than the actual number of cases. 

The impacts of similar previous works like 
The impacts of COVID-19 pandemic on public transit demand in the United States https://journals.plos.org/plosone/article?id=10.1371/journal.pone.0242476) 
highlights through regression analysis how “communities with higher proportions of essential workers, vulnerable populations (African American, Hispanic, Female, and people over 45 years old), and more coronavirus Google searches tend to maintain higher levels of minimal demand during COVID-19”.  The anticipated impact of our project is similar, trying to show higher levels of demand during COVID-19 in poorer socioeconomic communities when compared to wealthier socioeconomic communities. 

## Step 6

Your team report goes here

1. What are the major technical challenges that you anticipate with your project? This can be on anything - from getting more/better data, to building your models.

We anticipate that the data collection that we did for this will have probably been the biggest technical challenge, however for the future we believe that it may prove very difficult to actually showcase the data through diagrams, as we may have to learn ways to overlay our data with maps of New York.

2. How have you been distributing the work between group members? What are your plans for work distribution in the future? You can write down the roles that each person has in the team. A quick note: Multiple members can and should certainly work on different parts of the project together!

Our current method for distribution work has been coming up with a list of tasks or portions of the project and then having each of us split and conquer and then work together on parts when needed while communicating over parts that will eventually have to be merged. For this deliverables we split it up as so

Kazen: Create Covid Cases Table, Create Processed Table
John: Web Scrape turnstile data and create an sql file
Michael: Find zipcode for stations, demographics table
Matthew: Write paired t-test for processed table, get yearly covid case rates to compare to demographics

In the future we plan to work together in finishing up any more hypothesis tests, creating diagrams and plots for the data, and getting more data if needed, so it will be more group work than distinct parts like this deliverable.

Note: the processed table is not complete, as the data still needs to be joined
