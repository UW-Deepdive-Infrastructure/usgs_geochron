# Methods and Functions are camelCase. Variables and Data Structures are PascalCase
# Fields generally follow snake_case for better SQL compatibility
# Dependency functions are not embedded in master functions
# []-notation is used wherever possible, and $-notation is avoided.
# []-notation is slower, but more explicit and works for atomic vectors

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
print(paste("Load NLP tables from postgres",Sys.time()))

# Download the config file
Credentials<-as.matrix(read.table("/credentials/Credentials.yml",row.names=1))
# Connect to PostgreSQL
Driver <- dbDriver("PostgreSQL") # Establish database driver
Connection <- dbConnect(Driver, dbname = Credentials["database:",], host = Credentials["host:",], port = Credentials["port:",], user = Credentials["user:",])
# Query the sentences fro postgresql
DeepDiveData<-dbGetQuery(Connection,"SELECT docid, sentid, words, poses, dep_paths, dep_parents FROM nlp_sentences_352")

# print current status to terminal 
print(paste("Load postgres tables",Sys.time()))

#############################################################################################################
###################################### NNP CLUSTER FUNCTIONS, GEODEEPDIVE ###################################
#############################################################################################################
# A function to find proper noun clusters
findCluster<-function(Sentence,Parameters=c("words","poses"),Pose="NNP") {
        ParsedSentence<-parseSentence(Sentence,Parameters)
        if(all(is.na(ParsedSentence))) {return(setNames(c(Sentence["docid"], Sentence["sentid"],"parsing error"), c("docid", "sentid", "Proper")))}
        FindConsecutive<-findConsecutive(which(ParsedSentence["poses",]==Pose))
        Proper<-sapply(FindConsecutive,function(x) paste(unname(ParsedSentence["words",x]),collapse=" "))
        Proper<-unname(cbind(Sentence["docid"],Sentence["sentid"],Proper))
        colnames(Proper)<-c("docid","sentid","Proper") 
        return(Proper)
        }

# Sees if numbers are consecutive
findConsecutive<-function(NumericSequence) {
        Breaks<-c(0,which(diff(NumericSequence)!=1),length(NumericSequence))
        ConsecutiveList<-lapply(seq(length(Breaks)-1),function(x) NumericSequence[(Breaks[x]+1):Breaks[x+1]])
        return(ConsecutiveList)
        }

# Parse sentences
parseSentence<-function(Sentence,Parameters=c("words","dep_paths","dep_parents")) {
        Sentence<-setNames(cleanPunctuation(Sentence),names(Sentence))
        if ("words"%in%names(Sentence)) {Sentence["words"]<-trueCommas(Sentence["words"])}
        WordsMatrix<-sapply(Sentence[Parameters],function(x) strsplit(x,","))
        if (sum(diff(sapply(WordsMatrix,length)))!=0) {return(NA)}
        WordsMatrix<-do.call(rbind,WordsMatrix)
        WordsMatrix[which(WordsMatrix=="COMMASUB")]<-","
        WordsMatrix[which(WordsMatrix=="SPACESUB")]<-""
        colnames(WordsMatrix)<-1:ncol(WordsMatrix)
        return(WordsMatrix)
        }
                            
# R confuses 2,000,381 in a PostgreSQL array as 2 000 381, this function will convert those cases to 2000381.  
trueCommas<-function(Words) {
        InsideQuotes<-regmatches(Words, gregexpr('"[^"]*"',Words))[[1]]
        if (length(InsideQuotes)<1) {return(Words)}
        Replacements<-gsub(",","",InsideQuotes)
        for (i in 1:length(InsideQuotes)) {
                Words<-noquote(gsub(InsideQuotes[i],Replacements[i],Words))
                }
        return(Words)
        }

# Remove or replace problematic punctuation
# Even though this is redundnat with trueCommas it applies to more fields
cleanPunctuation<-function(Sentence) {
        Sentence<-gsub("\"\"","SPACESUB",Sentence)
        Sentence<-gsub("\",\"","COMMASUB",Sentence) 
        Sentence<-gsub("\\{|\\}","",Sentence)
        Sentence<-gsub("-LRB-","(",Sentence)
        Sentence<-gsub("-RRB-",")",Sentence)
        Sentence<-gsub("-LCB-","{",Sentence)
        Sentence<-gsub("-RCB-","}",Sentence)
        return(Sentence)
        }

######################################## NNP CLUSTER SCRIPT, GEODEEPDIVE ####################################
# Print the current stats to terminal
print(paste("Distribute all functions to each core",Sys.time()))

# Pass the functions to the cluster
clusterExport(cl=Cluster,varlist=c("parseSentence","findConsecutive","findCluster","cleanPunctuation","trueCommas"))

# print current status to terminal
print(paste("Clean up the punctuation",Sys.time()))

# Apply the clean punctuation to each sentence type
CleanedSentences<-parApply(Cluster,DeepDiveData,1,cleanPunctuation)

# print current status to terminal
print(paste("Extract all proper noun clusters",Sys.time()))

# Generate the proper noun strings
ProperList<-parApply(Cluster,CleanedSentences,1,findCluster)
# Stop the cluster
stopCluster(Cluster)

# Collapse the list into a character matrix
ProperMatrix<-na.omit(do.call(rbind,ProperList))
colnames(ProperMatrix)<-c("docid","sentid","Proper")

# Remove the "NA"'s
ProperMatrix<-subset(ProperMatrix,ProperMatrix[,"Proper"]!="NA")

# Set directory for output
OutputPath<-paste0(getwd(),"/output")

# Check if the output directory already exists
if (!dir.exists(OutputPath)) {
        dir.create(OutputPath)
        }
  
# Clear any old output files
unlink("*")

# Write csv output files
write.csv(ProperMatrix, paste(OutputPath,"ProperMatrix.csv",sep="/"))

# COMPLETE
print(paste("Complete",Sys.time()))