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
DeepDiveData<-dbGetQuery(Connection,"SELECT docid, sentid, words, poses, dep_paths, dep_parents FROM nlp_sentences_352")

#############################################################################################################
###################################### NNP CLUSTER FUNCTIONS, GEODEEPDIVE ###################################
#############################################################################################################
# A function to find proper noun clusters
findCluster<-function(Sentence,Parameters=c("words","poses")) {
        ParsedSentence<-parseSentence(Sentence,Parameters)
        FindConsecutive<-findConsecutive(which(ParsedSentence["poses",]=="NNP"))
        Proper<-sapply(FindConsecutive,function(x) paste(unname(ParsedSentence["words",x]),collapse=" "))
        Proper<-unname(cbind(Sentence["docid"],Sentence["sentid"],Proper))
        return(Proper)
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

# Sees if numbers are consecutive
findConsecutive<-function(NumericSequence) {
        Breaks<-c(0,which(diff(NumericSequence)!=1),length(NumericSequence))
        ConsecutiveList<-lapply(seq(length(Breaks)-1),function(x) NumericSequence[(Breaks[x]+1):Breaks[x+1]])
        return(ConsecutiveList)
        }

######################################## NNP CLUSTER SCRIPT, GEODEEPDIVE ####################################
# print current status to terminal
print(paste("Generate the nnp_clusters",Sys.time()))

# Pass the functions to the cluster
clusterExport(cl=Cluster,varlist=c("parseSentence","findConsecutive","findCluster"))

# Generate the proper noun strings
ProperList<-parApply(Cluster,DeepDiveData,1,findCluster)
# Stop the cluster
stopCluster(Cluster)

# Collapse the list into a character matrix
ProperMatrix<-na.omit(do.call(rbind,ProperList))
colnames(ProperMatrix)<-c("docid","sentid","Proper")

# Remove the "NA"'s
ProperMatrix<-subset(ProperMatrix,ProperMatrix[,"Proper"]!="NA")

# Set directory for output
CurrentDirectory<-getwd()
setwd(paste(CurrentDirectory,"/output",sep=""))
    
# Clear any old output files
unlink("*")

# Write csv output files
write.csv(ProperMatrix, "ProperMatrix.csv")

# COMPLETE
print(paste("Complete",Sys.time()))