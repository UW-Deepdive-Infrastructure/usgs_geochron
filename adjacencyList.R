# Custom functions are camelCase. Arrays, parameters, and arguments are PascalCase
# Dependency functions are not embedded in master functions, and are marked with the flag dependency in the documentation
# []-notation is used wherever possible, and $-notation is avoided.

######################################### Load Required Libraries ###########################################
# Save and print the app start time
Start<-print(Sys.time())

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
print(paste("Load postgres tables",Sys.time()))

# Download the config file
Credentials<-as.matrix(read.table("Credentials.yml",row.names=1))
# Connect to PostgreSQL
Driver <- dbDriver("PostgreSQL") # Establish database driver
Connection <- dbConnect(Driver, dbname = Credentials["database:",], host = Credentials["host:",], port = Credentials["port:",], user = Credentials["user:",])
# Query the sentences fro postgresql
DeepDiveData<-dbGetQuery(Connection,"SELECT docid, sentid, words, dep_paths, dep_parents FROM nlp_sentences_352") 

#############################################################################################################
####################################### ADJACENCY FUNCTIONS, GEODEEPDIVE ####################################
#############################################################################################################
# A function to parse a sentence and extract any grammatically linked termspaired terms
adjacencyPath<-function(Sentence,Path="amod") {
        ParsedSentence<-parseSentence(Sentence)
        PathMods<-as.matrix(ParsedSentence[,which(ParsedSentence["dep_paths",]==Path)])
        if (length(PathMods)<1) {return(setNames(rep(NA,4),c("docid","sentid","child","parent")))}
        FinalList<-vector("list")
        for (j in seq_len(ncol(PathMods))) {
                FinalList[[length(FinalList)+1]]<-c(Sentence[1:2],PathMods["words",j],ParsedSentence["words",as.numeric(PathMods["dep_parents",j])])
                }
        FinalTable<-do.call(rbind,FinalList)
        colnames(FinalTable)<-c("docid","sentid","child","parent")
        return(FinalTable)
        }

# Parse the NLP strings into a matrix format
parseSentence<-function(Sentence,Parameters=c("words","dep_paths","dep_parents")) {
        Sentence<-setNames(gsub("\"\"","SPACESUB",Sentence),names(Sentence))
        Sentence<-setNames(gsub("\",\"","COMMASUB",Sentence),names(Sentence))
        WordsMatrix<-sapply(Sentence[Parameters],function(x) strsplit(substring(x,2,nchar(x)-1),","))
        WordsMatrix<-do.call(rbind,WordsMatrix)
        WordsMatrix[which(WordsMatrix=="COMMASUB")]<-","
        WordsMatrix[which(WordsMatrix=="SPACESUB")]<-""
        colnames(WordsMatrix)<-1:ncol(WordsMatrix)
        return(WordsMatrix)
        }

######################################### ADJACENCY SCRIPT, GEODEEPDIVE #####################################
# print current status to terminal
print(paste("Generate the amod-noun pairs",Sys.time()))

# Pass the functions to the cluster
clusterExport(cl=Cluster,varlist=c("parseSentence","adjacencyPath"))

# Generate the adjacency-matrix
AdjacencyList<-parApply(Cluster,DeepDiveData,1,adjacencyPath)
# Stop the cluster
stopCluster(Cluster)

# Collapse the list into a character matrix
AdjacencyMatrix<-na.omit(do.call(rbind,AdjacencyList))

# Set directory for output
CurrentDirectory<-getwd()
setwd(paste(CurrentDirectory,"/output",sep=""))
    
# Clear any old output files
unlink("*")

# Write csv output files
write.csv(AdjacencyMatrix, "AdjacencyMatrix.csv")

# COMPLETE
print(paste("Complete",Sys.time()))