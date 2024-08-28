# Twitter Topic Prediction Analysis

## Overview
This project involves a comprehensive data analysis of three samples of tweets (1k, 10k, 100k) scraped from Twitter, focusing on predicting topics based on tweet text and user descriptions.

## Data Preparation
### 1. Data Filtering and Cleaning
- Retained essential attributes from the raw tweet data.
- Identified and extracted the top 20 hashtags for each dataset.

### 2. Topic Assignment
- Assigned topics to tweets based on the identified top hashtags.
- Created a labeled dataset for each sample size, facilitating further analysis.

## Machine Learning Model
### 1. Model Building
- Implemented Logistic Regression and Random Forest algorithms.
- Trained models on the labeled datasets to predict tweet topics.

### 2. Model Evaluation
- Achieved a high precision score of **0.9675**.
- Attained a recall score of **0.98333**, indicating strong predictive performance.

## Temporal Analysis
### 1. Date Range Query
- Conducted temporal analysis on the tweets, enabling queries by specific date ranges and countries.

### 2. Export Results
- Exported the queried results for further analysis and insights.

## Conclusion
We successfully cleaned, labeled, modeled, and analyzed the tweet datasets, demonstrating robust topic prediction capabilities and effective temporal querying. The models developed in this project provide valuable insights into tweet content over time.

