# AMP Usage Tracking

This repository contains the project artifacts needed to support usage tracking of Applied Machine Learning Prototype (AMP) repositories using Github API data. Project artifacts include a dashboard built in Cloudera Data Visualization (CDV), a local SQLite database that feeds the dashboard, and data refresh utilities built around the Github API.

![dashboard_img](images/AMP_Usage_Tracking_Dashboard.png)



## Repository Structure

```
.
├── data               # Contains archived data extracts
├── db                 # SQLite database that supports the CDV dashboard
├── refresh_data.py    # Script that runs daily to update data in SQLite
└── utils.py           # Utility functions that support the data refresh script
```



## How it works

The project currently lives on FFLab-4 CDSW cluster and is fully automated to refresh data on a daily basis with the following workflow:

1. A recurring CML Job executes each morning at 8:00 AM EST to fetch the previous day's usage statistics for each repository defined in `refresh_data.py`. Usage stats include:
   - Daily views and clones for each repo
   - A prior 14 day aggregate count of where traffic was sourced from for each repo
2. These daily stats are saved to an archive and also appended to a cumulative data frame that collects running stats over time.
3. These archive data frames are used to overwrite the contents of a local SQLite DB each day. This ensures we have an identical backup of the database at all times.
4. The database connection is updated upon user access to the dashboard, so new data is continuously propogated

