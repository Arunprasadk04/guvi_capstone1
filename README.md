YouTube Data Harvesting

This Python application enables users to migrate YouTube data from MongoDB to MySQL for analysis and visualization. 
It fetches data such as channel information, video details, playlist information, and comments using the YouTube Data API. 
The fetched data is then stored in MongoDB and subsequently migrated to MySQL for further analysis.

Features

•	Data Migration: Fetches data from the YouTube Data API, stores it in MongoDB, and migrates it to MySQL.
•	Data Analysis: Performs various SQL queries on the migrated data for insights and analysis.
•	Streamlit Interface: Provides a user-friendly interface for input and display of data.

Prerequisites

•	Python 3.x
•	Google API key for YouTube Data API
•	MongoDB installed and running on localhost
•	MySQL server running on localhost
•	Required Python libraries (install via pip install):

          •	isodate
          •	mysql-connector-python
          •	pandas
          •	pymongo
          •	streamlit
          •	google-api-python-client
          

Usage

Enter the channel ID(s) separated by commas in the provided input field.
Click the "Migrate Data" button to fetch data from the YouTube API, store it in MongoDB, and migrate it to MySQL.
Select the desired table name from the dropdown menu to display the data.
Check the "Perform SQL Queries" checkbox to execute predefined SQL queries and view the results.

SQL Queries

The application supports various SQL queries for data analysis, including:
  1.	Names of all videos and their corresponding channels
  2.	Top 10 videos by views
  3.	Number of comments per video
  4.	Videos with the highest number of likes
  5.	Total likes and dislikes for each video
  6.	Total views for each channel
  7.	Videos published in the year 2022
  8.	Average duration of videos for each channel
  9.	

Contributing

Contributions are welcome! Please fork the repository, make your changes, and submit a pull request.# guvi_capstone1

YouTube Data Harvesting

This project consists of modules for extracting data from the YouTube API, storing it in MongoDB, migrating it to MySQL, and displaying it using Streamlit.

