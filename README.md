#Datix

Datix: A System for Scalable Network Analytics

#Overview

Datix is a Scalable Network Analytics System that utilizes MapReduce techniques and state-of-the-art distributed processing engines such as Hadoop and Spark. The dataset used is in the form of sFlow files coming from a large European IXP and the goal is to extract information about network routing, dimensioning, accountability and security by combining sFlows with external datasets that contain mappings of IPs to AS (Autonomous Systems), IPs to Countries or IPs to reverse DNS lookup.

Datix contributes the following:
* It introduces a smart way of pre-partitioning the dataset in files that contain records of a particular range of values, to facilitate data processing and query execution.
* Using this particular partitioning scheme, it is possible to efficiently execute filtering queries avoiding the need to process the entire dataset.
* The above functionality is integrated into an open-source, SQL compliant network data analysis system by implementing distributed join algorithms, such as map join in combination with custom-made user-defined functions (UDFs) that are aware of the underlying data format.

#Copyright Information
Datix was developed as part of my Diploma Thesis at the National Technical University of Athens.

Copyright (C) 2014 Dimitrios Sarlis

All code is Licensed under the Apache License 2.0.
