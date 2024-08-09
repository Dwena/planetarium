# platenary-project

App Spark, Hadoop, Kafka, Machine Learning (classification to know if a planet is habitable).

## Problems

1. Problem with spark container, impossible to use. So we used spark on our computer before using spark on a python/spark container.
2. Problem with kafka. Code is ok but producer.flush() takes a long time and no data is return after producer. No solution found for this.
3. Problem with docker containers. We had trouble to create python containers for our api. We tried multiple things but finally, things resolved a bit randomly.
4. We had trouble with using the decision tree model in our application and the preprocessing. Our preprocessing used all the csv but for our application, we use only 1 line. So the preprocessing had errors because we used means for fillna and we used a correlation matrix to remove some columns but with only one line of data it's not possible.
