# Project: STEDI Human Balance Analytics

## Project Details
The STEDI Team has been hard at work developing a hardware STEDI Step Trainer that:

* trains the user to do a STEDI balance exercise;
* and has sensors on the device that collect data to train a machine-learning algorithm to detect steps;
* has a companion mobile app that collects customer data and interacts with the device sensors.
STEDI has heard from millions of early adopters who are willing to purchase the STEDI Step Trainers and use them.

Several customers have already received their Step Trainers, installed the mobile application, and begun using them together to test their balance. The Step Trainer is just a motion sensor that records the distance of the object detected. The app uses a mobile phone accelerometer to detect motion in the X, Y, and Z directions.

The STEDI team wants to use the motion sensor data to train a machine learning model to detect steps accurately in real-time. Privacy will be a primary consideration in deciding what data can be used.

## Project Summary

The project will extract the data produced by the STEDI Step Trainer team into a data lakehouse solution on AWS so that Data Scientist and Data Analysts can use to visualize information or train the learning model that will help to improve the product being developed.

## Environment
The data pipeline will make use of the following:
* Python and Spark
* AWS Glue
* AWS Athena
* AWS S3

