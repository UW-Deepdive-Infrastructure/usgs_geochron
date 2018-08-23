# Methods and Functions are camelCase. Variables and Data Structures are PascalCase
# Fields generally follow snake_case for better SQL compatibility
# Dependency functions are not embedded in master functions
# []-notation is used wherever possible, and $-notation is avoided.
# []-notation is slower, but more explicit and works for atomic vectors

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
        
# Link to the postgres database in teststrata (requires ssh)
# system("teststrata_tunnel")
Connection<-dbConnect(PostgreSQL(), dbname="burwell",host="localhost",port="5439",user="john",password="postgres")

#############################################################################################################
####################################### FIND ADJACENT MAPS IN BURWELL FUNCTIONS #############################
#############################################################################################################
# No functions at this time


############################################# FIND ADJACENT MAPS SCRIPT #####################################
# Create a geoprocessing schema
dbSendQuery(Connection,"CREATE SCHEMA IF NOT EXISTS geoprocessing;")

# Create a table of all adjacent large polygons
dbSendQuery(Connection,"CREATE TABLE IF NOT EXISTS geoprocessing.adjacency_large AS
                        WITH touches_key AS (SELECT A.map_id AS first, B.map_id AS second 
                        FROM maps.large AS A JOIN maps.large AS B ON ST_Touches(A.geom,B.geom) 
                        WHERE A.source_id!=B.source_id),
                        first_join AS (SELECT first, second, C.source_id AS first_source, C.name AS first_name, c.strat_name AS first_strat, c.age AS first_age, c.lith AS first_lith, c.geom AS first_geom 
                        FROM maps.large AS C RIGHT JOIN touches_key AS D ON C.map_id=D.first)
                        SELECT F.*, E.source_id AS second_source, E.name AS second_name, E.strat_name AS second_strat, E.age AS second_age, E.lith AS second_lith, E.geom AS second_geom 
                        FROM maps.large AS E RIGHT JOIN first_join AS F ON E.map_id=F.second
                        ;")
# Add a primary key
dbSendQuery(Connection,"ALTER TABLE geoprocessing.adjacency_large ADD COLUMN IF NOT EXISTS adjacency_id serial PRIMARY KEY")

#############################################################################################################
############################################ MODEL PARAMETER FUNCTIONS ######################################
#############################################################################################################
# Create a 3d array of which adjacencies (see Adjacency table) mention which liths in the Macrostrat Dictionary
# A reasonable step would be to skip comparisons for maps with NA Liths
# I would only revisit this if the model does not have enough power
grepLiths<-function(Adjacency,Dictionary=Lithologies[,"name"]) {
        Matches<-array(data=0,dim=c(nrow(Adjacency),length(Dictionary),2))
        rownames(Matches)<-Adjacency[,"adjacency_id"]
        colnames(Matches)<-Dictionary
        for (i in seq_along(Dictionary)) {
                Matches[grep(Dictionary[i],Adjacency[,"first_lith"]),i,1]<-1
                Matches[grep(Dictionary[i],Adjacency[,"second_lith"]),i,2]<-1
                }
        return(Matches)
        }


# Dependency grepl function for convertAges
greplIntervals<-function(Intervals,Adjacency) {
        Boolean<-grepl(Intervals["name"],Adjacency,ignore.case=TRUE,perl=TRUE)
        return(Boolean)
        }

# Test for an exact match with an accepted term
convertAge<-function(Adjacency,Intervals,Age="first_age") {
        FinalMatrix<-matrix(NA,nrow=nrow(Adjacency),ncol=2)
        colnames(FinalMatrix)<-c("t_age","b_age")
        for (i in seq_along(Adjacency[,Age])) {
                # Check for an exact match 
                if (is.na(Count<-match(tolower(Adjacency[i,Age]),tolower(Intervals[,"name"])))!=TRUE) {
                        FinalMatrix[i,"t_age"]<-Intervals[Count,"t_age"];
                        FinalMatrix[i,"b_age"]<-Intervals[Count,"b_age"];
                        next;
                        }
                # Create a list of potential age matches
                GrepMatches<-apply(Intervals,1,greplIntervals,Adjacency[i,Age])
                if (any(GrepMatches)!=TRUE) {
                        FinalMatrix[i,"t_age"]<-NA
                        FinalMatrix[i,"b_age"]<-NA
                        next;
                        }
                else {
                        FinalMatrix[i,"t_age"]<-min(Intervals[which(GrepMatches),"t_age"])
                        FinalMatrix[i,"b_age"]<-max(Intervals[which(GrepMatches),"b_age"])
                        }
                }
        return(FinalMatrix)
        }

# Find the (1-x) percent overlap of two line segments
percentOverlap<-function(x1,x2,y1,y2) {
        Full<-pmax(x2,y2)-pmin(x1,y1)
        Overlap<-pmax(0,pmin(x2,y2)-pmax(x1,y1))
        return(1-Overlap/Full)
        }

# Find the range difference
intervalDist<-function(Adjacency) {
        Zeroes<-which(Adjacency[,"overlap"]==0)
        Maximums<-apply(Adjacency[Zeroes,c("first_t","second_t")],1,max)
        Minimums<-apply(Adjacency[Zeroes,c("first_b","second_b")],1,min)
        return(Minimums-Maximums)
        }

############################################# MODEL PARAMETER SCRIPTS ######################################
# Load the basics of the adjacency table into R
Adjacency<-dbGetQuery(Connection,"SELECT adjacency_id, first, second, first_lith, second_lith, first_age, second_age FROM geoprocessing.adjacency_large")
Adjacency<-na.omit(Adjacency)

# Load the lithologies dictionary into R
Lithologies<-read.csv("https://macrostrat.org/api/v2/defs/lithologies?all&format=csv")

# Create the grep array
AdjLiths<-grepLiths(Adjacency,Lithologies[,"name"])
# Find the table of which adjacent polyons share which liths
AdjLiths<-apply(AdjLiths,c(1,2),sum)

# Load the intervals dictionary into R
Intervals<-read.csv("https://macrostrat.org/api/v2/defs/intervals?all&format=csv")

# Find the first ages
FirstAges<-convertAge(Adjacency,Intervals,"first_age")
Adjacency[,"first_t"]<-FirstAges[,"t_age"]
Adjacency[,"first_b"]<-FirstAges[,"b_age"]

# Find the second ages
SecondAges<-convertAge(Adjacency,Intervals,"second_age")
Adjacency[,"second_t"]<-SecondAges[,"t_age"]
Adjacency[,"second_b"]<-SecondAges[,"b_age"]

# Find the percent overlap
Adjacency[,"overlap"]<-percentOverlap(Adjacency$first_t,Adjacency$first_b,Adjacency$second_t,Adjacency$second_b)
Adjacency[which(Adjacency[,"overlap"]==0),"overlap"]<-intervalDist(Adjacency)

#############################################################################################################
############################################ TRAINING SET FUNCTIONS #########################################
#############################################################################################################
# No functions at this time.

############################################### TRAINING SET SCRIPTS ########################################
# Download names
Names<-dbGetQuery(Connection,"SELECT adjacency_id, first_strat, second_strat FROM geoprocessing.adjacency_large")
# Limit to only those in 
Names<-na.omit(subset(Names,Names[,"adjacency_id"]%in%Adjacency[,"adjacency_id"]))
Names<-Names[sample(1:nrow(Names),100,replace=FALSE),]