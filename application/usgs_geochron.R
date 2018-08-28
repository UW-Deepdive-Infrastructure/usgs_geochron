# Methods and Functions are camelCase. Variables and Data Structures are PascalCase
# Fields generally follow snake_case for better SQL compatibility
# Dependency functions are not embedded in master functions
# []-notation is used wherever possible, and $-notation is avoided.
# []-notation is slower, but more explicit and works for atomic vectors

######################################### Load Required Libraries ###########################################
# Save and print the app start time
Start<-print(paste("Starting application",Sys.time()))

# If running from UW-Madison
# Load or install the doParallel package
if (suppressWarnings(require("doParallel"))==FALSE) {
        install.packages("doParallel",repos="http://cran.cnr.berkeley.edu/");
        library("doParallel");
        }

# Load or install the RPostgreSQL package
if (suppressWarnings(require("RPostgreSQL"))==FALSE) {
        install.packages("RPostgreSQL",repos="http://cran.cnr.berkeley.edu/");
        library("RPostgreSQL");
        }

# Load or install devtools. This is needed to get the geocarrot beta. This step can be removed once geocarrot is live.
if (suppressWarnings(require("devtools"))==FALSE) {
        install.packages("devtools",repos="http://cran.cnr.berkeley.edu/");
        library("devtools");
        }

# Install the geocarrot package from github
# This currently requires devtools... grrr
if (suppressWarnings(require("geocarrot"))==FALSE) {
        devtools::install_github("aazaff/geocarrot");
        library("geocarrot");
        }

# Start a cluster for multicore, 3 by default or higher if passed as command line argument
CommandArgument<-commandArgs(TRUE)
if (length(CommandArgument)==0) {
        Cluster<-makeCluster(3)
        } else {
        Cluster<-makeCluster(as.numeric(CommandArgument[1]))
        }

#############################################################################################################
####################################### LOAD DATA FUNCTIONS, GEODEEPDIVE ####################################
#############################################################################################################
# No functions at this time

######################################### LOAD DATA SCRIPT, GEODEEPDIVE #####################################
# print current status to terminal 
print(paste("Load NLP tables from postgres",Sys.time()))

# Download the config file
Credentials<-as.matrix(read.table("./credentials/Credentials.yml",row.names=1))
# Connect to PostgreSQL
Driver <- dbDriver("PostgreSQL") # Establish database driver
Connection <- dbConnect(Driver, dbname = Credentials["database:",], host = Credentials["host:",], port = Credentials["port:",], user = Credentials["user:",], password = Credentials["password:",])
# Query the sentences fro postgresql
DeepDiveData<-dbGetQuery(Connection,"SELECT docid, sentid, words, poses, dep_paths, dep_parents FROM nlp_sentences_352")

#############################################################################################################
###################################### NNP CLUSTER FUNCTIONS, GEODEEPDIVE ###################################
#############################################################################################################
# No functions at this time.

######################################## NNP CLUSTER SCRIPT, GEODEEPDIVE ####################################
# Print the current stats to terminal
print(paste("Distribute all functions to each core",Sys.time()))

# Pass the functions to the cluster
clusterExport(cl=Cluster,varlist=c("parseSentence","consecutivePoses"))

# print current status to terminal
print(paste("Extract all proper noun clusters",Sys.time()))

# Generate the proper noun strings
ProperList<-parApply(Cluster,DeepDiveData,1,consecutivePoses)
# Stop the cluster
stopCluster(Cluster)

# print current status to terminal
print(paste("Extraction complete. Writing output",Sys.time()))

# Collapse the list into a character matrix
ProperMatrix<-na.omit(do.call(rbind,ProperList))

# Set directory for output
OutputPath<-paste0(getwd(),"/output")

# Check if the output directory already exists
if (!dir.exists(OutputPath)) {
        dir.create(OutputPath)
        }
  
# Write csv output files
write.csv(ProperMatrix, paste(OutputPath,"ProperMatrix.csv",sep="/"),row.names=FALSE)

# COMPLETE
print(paste("Application Complete",Sys.time()))