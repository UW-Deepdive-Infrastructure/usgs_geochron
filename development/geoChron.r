# Methods and Functions are camelCase. Variables and Data Structures are PascalCase
# Fields generally follow snake_case for better SQL compatibility
# Dependency functions are not embedded in master functions
# []-notation is used wherever possible, and $-notation is avoided.
# []-notation is marginally slower, but more explicit and works for atomic vectors

#############################################################################################################
############################################## CONFIGURATION, SCRIPT ########################################
#############################################################################################################    
# Increase the timeout time and change the fancyquote settings
options(timeout=600, "useFancyQuotes"=FALSE)

# Load or install RPostgreSQL package
if (suppressWarnings(require("RPostgreSQL"))==FALSE) {
        install.packages("RPostgreSQL",repos="http://cran.cnr.berkeley.edu/");
        library("RPostgreSQL");
        }

# Load or install RPostgreSQL package
if (suppressWarnings(require("RJSONIO"))==FALSE) {
        install.packages("RJSONIO",repos="http://cran.cnr.berkeley.edu/");
        library("RJSONIO");
        }  
        
# Link to the postgres database in teststrata (requires ssh)
# system("teststrata_tunnel")
Ludlamite<-dbConnect(PostgreSQL(), dbname="ludlamite",host="localhost",port="5432",user="zaffos")
Gill<-dbConnect(PostgreSQL(), dbname="gill",host="localhost",port="5432",user="zaffos")

#############################################################################################################
####################################### FIND GEOCHRON DOCS IN BURWELL FUNCTIONS #############################
#############################################################################################################
# No functions at this time


############################################# FIND GEOCHRON DOCS SCRIPT #####################################
# Load the data
GeochronMatches<-dbGetQuery(Ludlamite,"SELECT * FROM usgs_geochron.gdd_ts;",stringsAsFactors=FALSE)

# Subset to only documents that include geochronology (dict=38) terms
GeochronMatches<-subset(GeochronMatches,GeochronMatches[,"docid"]%in%GeochronMatches[which(GeochronMatches[,"dict"]==38),"docid"])
# Subset to only documents that match 1 or more hunted locations (dict=45)
GeochronMatches<-subset(GeochronMatches,GeochronMatches[,"docid"]%in%GeochronMatches[which(GeochronMatches[,"dict"]==45),"docid"])

# A much smaller set of locations within the bounding box 107.8W, 37N, 106W, 38N
Localities<-dbGetQuery(Gill,"SELECT name FROM places WHERE ST_Intersects(geom,ST_MakeEnvelope(-107.8, 37, -106, 38, 4326)) AND placetype='locality';")
County<-dbGetQuery(Gill,"SELECT name FROM places WHERE ST_Intersects(geom,ST_MakeEnvelope(-107.8, 37, -106, 38, 4326)) AND placetype='county';")
Smaller<-GeochronMatches[which(GeochronMatches[,"term"]%in%Localities[,"name"] & GeochronMatches[,"term"]%in%County[,"name"]),"docid"]
Smaller<-subset(GeochronMatches,GeochronMatches[,"docid"]%in%Smaller)

# Load the json file into postgres
# dbSendQuery(Ludlamite,"SET content `cat ~/Desktop/bibjson.json`;")
# dbSendQuery(Ludlamite,"CREATE TEMPORARY TABLE temp_json (bibjson jsonb);")
# dbSendQuery(Ludlamite,"INSERT INTO temp_json VALUES (:'content');")
# dbSendQuery(Ludlamite,"CREATE TABLE usgs_geochron.bibjson AS SELCT * FROM temp_json;")
# Although I store it in postgres for record-keeping, it is actually difficult to import it from
# the database as json into the R console, so I still use RJSONIO::fromJSON on the original doc
Bibjson<-RJSONIO::fromJSON("~/Desktop/bibjson.json")
# Find which bibjson items have documents
Output<-Bibjson[which(unlist(lapply(Bibjson,function(x) x$'_gddid'))%in%Smaller[,"docid"])]

# Convert the bibjson into a flat document
Flat<-setNames(as.data.frame(unlist(lapply(Output,function(x) x$'title'))),"title")
Flat[,"link"]<-unlist(lapply(Output,function(x) x$link[[1]][["url"]]))
rownames(Flat)<-unlist(lapply(Output,function(x) x$'_gddid'))
# Add in the location dictionaries and geochron terms
Flat[,"localities"]<-tapply(Smaller[which(Smaller[,"dict"]==38),"term"],Smaller[which(Smaller[,"dict"]==38),"docid"],function(x) paste(unique(x),collapse=" | "))
# Write it back out as csv
write.csv(Flat,"usgs_geochron_pilotsearch.csv")