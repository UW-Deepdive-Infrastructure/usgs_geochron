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
# Query the sentences from postgresql
DeepDiveData<-dbGetQuery(Connection,"SELECT docid, sentid, words, poses, dep_paths, dep_parents FROM nlp_sentences_352")

# Load the dictionary
Dictionary<-dbGetQuery(Connection,"SELECT * FROM radiocarbon_dictionary")

#############################################################################################################
######################################## DICTIONARY FUNCTIONS, GEODEEPDIVE ##################################
#############################################################################################################
# Parse the NLP strings into a matrix format
parseSentence<-function(Sentence,Parameters=c("words","dep_paths","dep_parents")) {
        if ("words"%in%names(Sentence)) {Sentence["words"]<-trueCommas(Sentence["words"])}
        Sentence<-setNames(cleanPunctuation(Sentence),names(Sentence))
        WordsMatrix<-sapply(Sentence[Parameters],function(x) strsplit(x,","))
        WordsMatrix<-do.call(rbind,WordsMatrix)
        WordsMatrix[which(WordsMatrix=="\"COMMASUB\"")]<-","
        WordsMatrix[which(WordsMatrix=="SPACESUB")]<-""
        colnames(WordsMatrix)<-1:ncol(WordsMatrix)
        return(WordsMatrix)
        }

# Account for actual phrases or numbersets with commas - e.g., "19,560,238" -> 19560238
trueCommas<-function(Words) {
        InsideQuotes<-gregexpr('"[^"]*"',Words)
        Phrases<-regmatches(Words, InsideQuotes)[[1]]
        Phrases[which(Phrases=="\",\"")]<-"COMMASUB"
        if (length(Phrases)<1) {return(Words)}
        for (i in 1:length(Phrases)) {
                Top<-InsideQuotes[[1]][i]
                Bottom<-Top+attributes(InsideQuotes[[1]])[[1]][i]-1                
                Start<-substring(Words,1,Top)
                End<-substring(Words,Bottom,nchar(Words))
                Cleaned<-noquote(gsub(",","",Phrases[i]))
                Words<-paste0(Start,Cleaned,End)
                InsideQuotes<-gregexpr('"[^"]*"',Words)
                Phrases<-regmatches(Words, InsideQuotes)[[1]]
                Phrases[which(Phrases=="\",\"")]<-"COMMASUB"
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

######################################### LOAD DATA SCRIPT, GEODEEPDIVE #####################################
# Index the location of each dictionary symbol in the text
Index<-sapply(Dictionary[,"dict"],function(x) grep(x,DeepDiveData[,"words"],fixed=TRUE))

# Combine all terms within sentences
NewMatrix<-matrix(NA,nrow=nrow(DeepDiveData),ncol=nrow(Dictionary))
colnames(NewMatrix)<-Dictionary[,1]
for (i in 1:ncol(NewMatrix)) {
        NewMatrix[Index[[i]],i]<-colnames(NewMatrix)[i]
        }

# Extract the terms array
Terms<-apply(NewMatrix,1,function(x) paste(na.omit(x),collapse="|"))

# Bind the terms array to the original data and subset to only matches
Matches<-cbind(DeepDiveData,Terms)
# Extract only stenences with a match
Matches<-subset(Matches,Matches[,"Terms"]!="")