# Amazon Vine Analysis
#### PySpark/Pandas/SQL/AWS-RDS Big Data

## Overview

The purpose of this analysis is to practice fundamentals of big data review, including the extraction, transforming, and loading of these data from a ***AWS bucket to an AWS SQL postgres server***. I use PySpark to retrieve the data and create data frames that were then cleaned and filtered for the purpose of drawing conclusion about the review data.  For this review, I looked at e-Book data in the amazon products list.

Example of PySpark loading for Amazon Vine Review Data
```
# Read in data from S3 Buckets
from pyspark import SparkFiles
url = "https://s3.amazonaws.com/amazon-reviews-pds/tsv/amazon_reviews_us_Digital_Ebook_Purchase_v1_01.tsv.gz"
spark.sparkContext.addFile(url)
df = spark.read.option("encoding", "UTF-8").csv(SparkFiles.get(""), sep="\t", header=True, inferSchema=True)

# Show DataFrame
# clean and reassign then add again
df = df.dropna()
df.show()

```

## Results

The basic overview of the data is as follows:

- There were a total of 5,101,476 reviews in the data set.
- Of those reviews, 65,145 were vine reviews, meaning that 5,036,331 were non-Vine reviews.
![image](Resources\total_vine_reviews.png)
- In the vine review program, none of those were paid reviews for the e-Books and every single review was unpaid.
- 37.9% of those unpaid reviews were 5-star reviews.  Comparatively, the total number of 5-star reviews for the entire cleaned data set was 57.9%.


## Summary

There were a total of 65,145 Vine reviews in the data set. Of those reviews, none of them were actually paid vine reviews.  So our analysis ended up being skewed to only look at the five star reviews for the Vine data that was not paid.
![image](Resources\NoPaidVines.png)
An additional analysis was run for the total reviews and the five star rating to compare to the percentage of the five star rating in the Vine program reviews specifically.  Based on the fact that the vine program reviews were 20% less likely to have five stars, it can be said that there is bias in the vine program reviews, maybe they employ modre critical stance when reviewing an e-book as compared to the general populace.

---


##### AWS Dataset
https://s3.amazonaws.com/amazon-reviews-pds/tsv/amazon_reviews_us_Digital_Ebook_Purchase_v1_01.tsv.gz