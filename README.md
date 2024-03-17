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

Contributing
Contributions are welcome! Please fork the repository, make your changes, and submit a pull request.# guvi_capstone1
YouTube Data Extraction and Visualization
This project consists of modules for extracting data from the YouTube API, storing it in MongoDB, migrating it to MySQL, and displaying it using Streamlit.

Features
Data Extraction from YouTube API: Retrieve data such as video metadata, comments, and channel statistics from the YouTube API.
Data Storage in MongoDB: Store extracted YouTube data in a MongoDB database.
Data Migration to MySQL: Transfer data from MongoDB to MySQL for further analysis and visualization.
Interactive Dashboard with Streamlit: Display YouTube data stored in MySQL in an interactive web application using Streamlit.
Modules
Module 1: Data Extraction and Migration
This module involves the following steps:

MongoDB Setup: Create a MongoDB database and collection for storing YouTube data.
MySQL Setup: Set up a MySQL database and table for migrating data from MongoDB.
Data Extraction from YouTube API: Fetch data using API keys and channel IDs and store it in MongoDB.
Data Migration to MySQL: Transfer data from MongoDB to MySQL using Python scripts.
Module 2: Streamlit App for Data Visualization
This module utilizes Streamlit to create an interactive dashboard for visualizing YouTube data stored in MySQL. It includes:

Display Channel Data: Render channel data fetched from MySQL in tabular format.
Display Video Information: Present video information obtained from MySQL in a structured manner.
