# gdd_usgs_example
An example GeoDeepDive application.

## Application Requirements
This GeoDeepDive application assumes the following:

1. A [subset](/credentials/usgs_geochron_pilot.csv) of the GeoDeepDive corpus has already been defined. 
2. The StanfordCoreNLP V3.52 output for this subset exists in a PostgreSQL table `public.nlp_sentences_352`.
3. That the credentials to connect to this PostgreSQL database are saved in a [Credentials.yml](/credentials/Credentials.yml) file.
4. R version 3.4 or later is installed, and that the operating system is Unix-compatible (Linux, OSX).
5. That system has internet access or that the R Packages `doParallel` and `RPostgreSQL` and their dependencies are already installed.
6. That there is an empty directory named output to hold the application results.

## Running the Applications
The application can be run with the following shell command. The default number of cores is 3 if not specified.

````bash
RScript ~/Path/usgs_geochron.R numcores
````

You could presumably try this directly from the git repo without cloning - if you could get around the credentials issues.
````bash
curl https://raw.githubusercontent.com/UW-Deepdive-Infrastructure/gdd_byod/master/application/usgs_geochron.R | RScript 4
````
