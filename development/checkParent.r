# Custom functions are camelCase. Arrays and Arguments are PascalCase
# Dependency functions are not embedded in master functions
# []-notation is used wherever possible, and $-notation is avoided.

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
        
# Link to the postgres database if SSH to Homoplasy
Connection<-dbConnect(PostgreSQL(), dbname="burwell",host="localhost",port="5444",user="zaffos",password="postgres")

#############################################################################################################
############################################ CREATE DATABASE, MAP MATCH #####################################
#############################################################################################################
# No functions at this time.
#checkParent<-function(Intersections,Lookup) {
#        Intersections<-subset(Intersections,is.na(Intersections[,"child_strat"])!=TRUE)
#        Intersections<-subset(Intersections,is.na(Intersections[,"parent_strat"])!=TRUE)
#        Intersections<-subset(Intersections,Intersections[,"child_strat"]%in%Lookup[,"rank_name"])
#        Intersections<-subset(Intersections,Intersections[,"parent_strat"]%in%Lookup[,"rank_name"])
#        compatibility<-vector()
#        for (i in 1:nrow(Intersections)) {
#                Child<-subset(Lookup,Lookup[,"rank_name"]==Intersections[i,"child_strat"])
#                Parent<-subset(Lookup,Lookup[,"rank_name"]==Intersections[i,"parent_strat"])
#                String<-c(Child[,"mbr_name"],Child[,"fm_name"],Child[,"gp_name"],Child[,"sgp_name"])
#                Suffixes<-c(paste(Child[,"mbr_name"],"Member"),paste(Child[,"fm_name"],"Formation"),paste(Child[,"gp_name"],"Group"),paste(Child[,"sgp_name"],"Supergroup"))
#                compatibility[i]<-any(c(Intersections[i,"parent_strat"]%in%c(String,Suffixes),Child[,"concept_id"]%in%Parent[,"concept_id"]))
#                }
#        return(cbind(Intersections,compatibility))
#        }

# New version of checkParent, commented out the old version cause I didn't feel like git
checkParent<-function(Intersections,Lookup) {
        Compatibility<-vector(mode="logical",length=nrow(Intersections))
        Identical<-Intersections[,"child_id"]==Intersections[,"parent_id"]
        Explicit<-cbind(Intersections[,"parent_id"],Lookup[match(Intersections[,"child_id"],Lookup[,"strat_name_id"]),c("bed_id","mbr_id","fm_id","gp_id","sgp_id"))
        Explicit<-apply(Explicit,1,function(x) x[,"parent_id"]%in%x[,c("bed_id","mbr_id","fm_id","gp_id","sgp_id")]
        Concept<-
        }


############################################## DATA DOWNLOAD, BLM ###########################################
# Create a geoprocessing schema
dbSendQuery(Connection,"CREATE SCHEMA IF NOT EXISTS geoprocessing;")

# Create a table of the utah bbox large and medium polygon intersections
# This version, unlike the other version also removes water polys, but I've decided to keep those.
# dbSendQuery(Connection,"CREATE TABLE geoprocessing.utah_quality AS
#                        WITH A AS (SELECT * FROM maps.large WHERE "name"!='water' AND ST_Intersects(geom,ST_MakeEnvelope(-114.052998,36.997949,-109.041058,42.001618,4326))),
#                        B AS (SELECT * FROM maps.medium WHERE "name"!='water' AND ST_Intersects(geom,ST_MakeEnvelope(-114.052998,36.997949,-109.041058,42.001618,4326)))
#                        SELECT A."name" AS child_name, A."source_id" AS child_source, A."strat_name" AS child_strat, B."name" AS parent_name, B."source_id" AS parent_source, B."strat_name" AS parent_strat, ST_Intersection(A.geom,B.geom) AS geom FROM A JOIN B ON ST_Intersects(A.geom,B.geom)
#                        ;")
# dbSendQuery(Connection,"ALTER TABLE geoprocessing.utah_quality ADD COLUMN id serial PRIMARY KEY")

# Create a table of all large and medium polygon intersections
dbSendQuery(Connection,"CREATE TABLE geoprocessing.largemedium AS 
        SELECT A.map_id AS child_id, A."name" AS child_name, A."source_id" AS child_source, A."strat_name" AS child_strat, 
        B.map_id AS parent_id, B."name" AS parent_name, B."source_id" AS parent_source, B."strat_name" AS parent_strat, 
        ST_Intersection(A.geom,B.geom) AS geom 
        FROM maps.large AS A JOIN maps.medium AS B ON ST_Intersects(A.geom,B.geom)
        ;")
dbSendQuery(Connection,"ALTER TABLE geoprocessing.largemedium ADD COLUMN id serial PRIMARY KEY")

# This step should be unnecessary once the new concept_ids are implemented and can be done solely on the postgres side
# Download the lookup table into R
Lookup<-dbGetQuery(Connection,"SELECT * FROM macrostrat.lookup_strat_names;")
# Give everything a unique concept_id
Split<-split(Lookup,Lookup[,"concept_id"]==0)
Split[["TRUE"]][,"concept_id"]<-max(Lookup[,"concept_id"]):(max(Lookup[,"concept_id"])+nrow(Split[["TRUE"]])-1)
Split<-do.call(rbind,Split)
# Update all of the id's to their respective concept_id
Split[,"bed_id"]<-Split[match(Split[,"bed_id"],Split[,"strat_name_id"]),"concept_id"]
Split[,"mbr_id"]<-Split[match(Split[,"mbr_id"],Split[,"strat_name_id"]),"concept_id"]
Split[,"fm_id"]<-Split[match(Split[,"fm_id"],Split[,"strat_name_id"]),"concept_id"]
Split[,"gp_id"]<-Split[match(Split[,"gp_id"],Split[,"strat_name_id"]),"concept_id"]
Split[,"sgp_id"]<-Split[match(Split[,"sgp_id"],Split[,"strat_name_id"]),"concept_id"]
# Send this modified version to postgres
dbWriteTable(Connection,c("geoprocessing","lookup_strat_names"),value=Split,row.names=FALSE)

# Add a concept_id field, first convert map_id to strat_name_id, then from strat_name_id to concept id
dbSendQuery(Connection,"ALTER TABLE geoprocessing.largemedium ADD COLUMN child_concept integer")
dbSendQuery(Connection,"UPDATE geoprocessing.largemedium SET child_concept=strat_name_id FROM maps.map_strat_names WHERE child_id=map_id;")
dbSendQuery(Connection,"ALTER TABLE geoprocessing.largemedium ADD COLUMN parent_concept integer")
dbSendQuery(Connection,"UPDATE geoprocessing.largemedium SET parent_concept=strat_name_id FROM maps.map_strat_names WHERE parent_id=map_id;")
# Convert from strat_name_id to concept_id
dbSendQuery(Connection,"UPDATE geoprocessing.largemedium SET child_concept=concept_id FROM geoprocessing. WHERE child_id=map_id;")

# Check if the child_id and parent_id are identical
Identical<-Intersections[,"child_id"]==Intersections[,"parent_id"]
# Extract the strat_name_id hierarchy for each child_id
Hierarchy<-Split[match(Intersections[,"child_id"],Split[,"strat_name_id"]),c("bed_id","mbr_id","fm_id","gp_id","sgp_id"))
Explicit<-cbind(Intersections[,"parent_id"],Lookup[match(Intersections[,"child_id"],Lookup[,"strat_name_id"]),c("bed_id","mbr_id","fm_id","gp_id","sgp_id"))
Explicit<-apply(Explicit,1,function(x) x[,"parent_id"]%in%x[,c("bed_id","mbr_id","fm_id","gp_id","sgp_id")]

# Download the geoprocessed data into R
Intersections<-dbGetQuery(Connection,"SELECT child_strat,parent_strat,ST_AsText(geom) AS geom FROM geoprocessing.largemedium;")


# Check if child_strat and parent_strat are identical
Check<-checkParent(Intersections,Lookup)