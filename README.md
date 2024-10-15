<h1 align='center'>BSc - A Software Solution for Analysis and Visualization of the Metropolitan Museum of Art Data </h1>

<div align='center'>
  <img src="https://img.shields.io/badge/MongoDB-%234ea94b.svg?style=for-the-badge&logo=mongodb&logoColor=white" />
  <img src="https://img.shields.io/badge/python-3670A0?style=for-the-badge&logo=python&logoColor=ffdd54" />
</div>

# Overview
This repository contains all the materials related to my Bachelor thesis project. It includes data insertions for both optimized and unoptimized database schemas, as well as queries tailored for each schema type. Additionally, it features queries designed to provide geospatial information. The dataset used throughout the project was enriched via web scraping techniques to ensure comprehensive data coverage. Metabase tool is used for data visualization.

# Content
  - `unoptimized-schema.py` - This code connects to a MongoDB database and processes data from a CSV file. It inserts artist information into the artistsV1 collection, ensuring there are no duplicate entries based on the artist's display name and birth date. It also processes object data, organizes fields with multiple values, and manages tags with associated URLs, finally inserting object data into the objectsV1 collection in batches of 10,000.
  - `optimized-schema.py` - This code connects to a MongoDB database and processes data from a CSV file. It inserts object information into the objects collection, including artist data, while ensuring that each artist's roles are aggregated without duplication based on their name and birth date. Additionally, it processes tags with associated URLs and other object metadata, inserting the data in batches of 10,000 documents into MongoDB for efficiency.
  - `optimized queries` directory - Contains all the queries tailored for the optimized schema.   
  - `unoptimized queries` directory - Contains all the queries tailored for the unoptimized schema.
  - `geospatial queries` directory - Contains all the queries that provide interesting geospatial information.
  - `ULANDataScraper.py` - This web scraping script is designed to retrieve artist data from the MongoDB collection, access URLs from the Artist ULAN URL field, and scrape additional details such as gender, birth place, and death place, including their geographical coordinates. If the artist data includes valid information, it updates the corresponding document in MongoDB.
  - `WikiDataScraper.py` - This web scraping script connects to a MongoDB database, retrieves artist documents, and for each artist with a valid Wikidata URL, scrapes additional information such as museums and lived places using the BeautifulSoup library. The results are added back to the MongoDB collection.

# Technologies
  - MongoDB: A NoSQL, document-oriented database that stores data in flexible, JSON-like documents rather than relying on a traditional relational table structure. It is designed for scalability, performance, and ease of use, making it ideal for handling large datasets and dynamic, unstructured data
  - Python: A high-level, versatile programming language known for its readability and ease of use, making it popular for beginners and experienced developers alike. It supports multiple programming paradigms, such as procedural, object-oriented, and functional programming, and is widely used in web development, data science, automation, and more.
  - Metabase: Metabase is an open-source business intelligence tool that allows users to visualize and analyze data through an intuitive interface without needing to write SQL queries. It connects to various databases and helps generate reports, dashboards, and insights for data-driven decision-making.

# Prerequisites
- <b>Git:</b> Ensure you have Git installed. You can download it from [here](https://git-scm.com/downloads).
- <b>MongoDB:</b> Ensure your machine has MongoDB installed. You can find it [here](https://www.mongodb.com/try/download/community).
- <b>Python:</b> Ensure you have installed Python before running the python scripts. You can download the latest Python version [here](https://www.python.org/downloads/).
- <b>Metabase:</b> For query result visualization it is prefered to use Metabase. You can download it from [here](https://www.metabase.com/docs/latest/installation-and-operation/installing-metabase), or you can run it as a docker container.
