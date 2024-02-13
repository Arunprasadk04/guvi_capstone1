# guvi_capstone1
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
