from sqlalchemy import create_engine, text
import pandas as pd
import os

# connection details
dsn_hostname = 'db'
dsn_user = 'postgres'
dsn_pwd = os.getenv('DB_PASSWORD')
dsn_port = '5432'
dsn_database = 'inside_airbnb_milan'

# create a connection string
dsn = f'postgresql://{dsn_user}:{dsn_pwd}@{dsn_hostname}:{dsn_port}/{dsn_database}'

# create an engine
engine = create_engine(dsn)

# connect to the database
connection = engine.connect()

## 1. for each neighbourhood, what is the weighted average number of listings per host? 

# construct the query
SQL_1 = """ 
SELECT *, 
       ROUND(avg_listing_per_host_per_neighbourhood * listing_per_neighbourhood / total_listings, 5) AS weighted_avg_listing_per_host_per_neighbourhood
    FROM (SELECT *, 
                 ROUND((listing_per_neighbourhood * 1.0 / host_per_neighbourhood), 5) AS avg_listing_per_host_per_neighbourhood,
                 SUM(listing_per_neighbourhood) OVER() AS total_listings
              FROM (SELECT neighbourhood_id,
                           COUNT(listing_id) AS listing_per_neighbourhood,
                           COUNT(DISTINCT pseudo_host_id) AS host_per_neighbourhood
                        FROM Listing
                        GROUP BY neighbourhood_id) AS subquery1) AS subquery2
;"""

# execute the query
result_1 = connection.execute(text(SQL_1))

# retrieve all the rows returned by the query
rows_1 = result_1.fetchall()

# get the column names
columns_1 = result_1.keys()

# display the results as a Pandas dataframe
df_result_1 = pd.DataFrame(rows_1, columns=columns_1)
df_result_1.to_csv('output/weighted_avg_listing_per_host_per_neighbourhood.csv', index=False)

## 2. for each neighbourhood, what is the weighted average review rating score? 

# construct the query
SQL_2 = """
SELECT *,
	   ROUND(avg_review_scores_rating_per_neighbourhood * listing_per_neighbourhood / total_listings, 5) AS weighted_avg_review_scores_rating_per_neighbourhood
	FROM (SELECT *,
	             ROUND((total_review_scores_rating_per_neighbourhood / listing_per_neighbourhood), 5) AS avg_review_scores_rating_per_neighbourhood,
				 SUM(listing_per_neighbourhood) OVER () AS total_listings
			  FROM(SELECT neighbourhood_id,
						  COUNT(listing_id) AS listing_per_neighbourhood,
						  SUM(review_scores_rating) AS total_review_scores_rating_per_neighbourhood
					   FROM Listing
					   GROUP BY neighbourhood_id) AS subquery1) AS subquery2
	ORDER BY avg_review_scores_rating_per_neighbourhood DESC
;"""

# execute the query
result_2 = connection.execute(text(SQL_2))

# retrieve all the rows returned by the query
rows_2 = result_2.fetchall()

# get the column names
columns_2 = result_2.keys()

# display the results as a Pandas dataframe
df_result_2 = pd.DataFrame(rows_2, columns=columns_2)
df_result_2.to_csv('output/weighted_avg_review_scores_rating_per_neighbourhood.csv', index=False)

## 3. what are the top 10 neighborhoods with the highest weighted average number of listings per host and the highest weighted average review rating score?

# construct the query
SQL_3 = """
WITH SQ1 AS (SELECT *,
					ROUND(avg_listing_per_host_per_neighbourhood * listing_per_neighbourhood / total_listings, 5) AS weighted_avg_listing_per_host_per_neighbourhood 
				 FROM (SELECT *,
							  ROUND((listing_per_neighbourhood * 1.0 / host_per_neighbourhood), 5) AS avg_listing_per_host_per_neighbourhood,
							  SUM(listing_per_neighbourhood) OVER () AS total_listings
						   FROM (SELECT neighbourhood_id,
										COUNT(listing_id) AS listing_per_neighbourhood,
										COUNT(DISTINCT pseudo_host_id) AS host_per_neighbourhood
									 FROM Listing
									 GROUP BY neighbourhood_id) AS subquery1) AS subquery2),
	 SQ2 AS (SELECT *,
					ROUND(avg_review_scores_rating_per_neighbourhood * listing_per_neighbourhood / total_listings, 5) AS weighted_avg_review_scores_rating_per_neighbourhood
				 FROM (SELECT *,
							  ROUND((total_review_scores_rating_per_neighbourhood / listing_per_neighbourhood), 5) AS avg_review_scores_rating_per_neighbourhood,
							  SUM(listing_per_neighbourhood) OVER () AS total_listings
						   FROM(SELECT neighbourhood_id,
									   COUNT(listing_id) AS listing_per_neighbourhood,
									   SUM(review_scores_rating) AS total_review_scores_rating_per_neighbourhood
									FROM Listing
									GROUP BY neighbourhood_id) AS subquery1) AS subquery2)
SELECT n.neighbourhood_name, SQ1.neighbourhood_id, SQ1.weighted_avg_listing_per_host_per_neighbourhood, SQ2.weighted_avg_review_scores_rating_per_neighbourhood
	FROM SQ1
	INNER JOIN SQ2
		ON SQ1.neighbourhood_id = SQ2.neighbourhood_id
	INNER JOIN Neighbourhood AS n
		ON SQ1.neighbourhood_id = n.neighbourhood_id
	ORDER BY SQ1.weighted_avg_listing_per_host_per_neighbourhood DESC, SQ2.weighted_avg_review_scores_rating_per_neighbourhood DESC
	LIMIT 10
;"""

# execute the query
result_3 = connection.execute(text(SQL_3))

# retrieve all the rows returned by the query
rows_3 = result_3.fetchall()

# get the column names
columns_3 = result_3.keys()

# display the results as a Pandas dataframe
df_result_3 = pd.DataFrame(rows_3, columns=columns_3)
df_result_3.to_csv('output/top_10_neighbourhoods.csv', index=False)

## 4. for each of the top 10 neighborhoods identified in question 3, what are the proportions of short-term rentals per room type?

# construct the query
SQL_4 = """
WITH SQ4 AS (SELECT *
				 FROM Listing
				 WHERE neighbourhood_id IN (61, 76, 59, 56, 38, 1, 5, 39, 48, 57))
SELECT b.neighbourhood_name,
	   a.*
	FROM (SELECT *,
				 ROUND((stay_period_count * 1.0 / stay_period_count_per_neighbourhood_and_room_type) * 100.0, 5) AS stay_period_proportion
			  FROM (SELECT *,
						   SUM(stay_period_count) OVER (PARTITION BY neighbourhood_id, room_type) AS stay_period_count_per_neighbourhood_and_room_type
						FROM (SELECT neighbourhood_id, room_type, stay_period,
									 COUNT(*) AS stay_period_count
								  FROM (SELECT *,
											   CASE WHEN minimum_nights < 30 THEN 'short-term'
													ELSE 'long-term' END AS stay_period
											FROM SQ4) AS subquery1
								  GROUP BY neighbourhood_id, room_type, stay_period) AS subquery2) AS subquery3) AS a
	LEFT JOIN Neighbourhood AS b
	    ON a.neighbourhood_id = b.neighbourhood_id
;"""

# execute the query
result_4 = connection.execute(text(SQL_4))

# retrieve all the rows returned by the query
rows_4 = result_4.fetchall()

# get the column names
columns_4 = result_4.keys()

# display the results as a Pandas dataframe
df_result_4 = pd.DataFrame(rows_4, columns=columns_4)
df_result_4.to_csv('output/proportion_of_short_term_rentals_per_room_type.csv', index=False)

# close the connection
connection.close()