# traffic_electronic_count_ETL

This is a basic ETL for electronic traffic counting data that comes in text file format with .rsa extension.

## Summary Counts
Summary counts consist of different count types:
    - Type 20 : Speed summary description record
    - type 21 : Program 2.0 speed summary description record
    - Type 30 : Classification summary description record
    - Type 70 : Traffic flow summary description record
    - Type 60 : Length summary description record
    - Type 31 : Secondary class summary description record
    - Type 22 : Program 2.3 speed summary description record

## Individual Counts
Individual counts consist of 1 count type:
    - Type 10 - Individual vehicle data description record
    this had several classes of sub-data that is added on to each record