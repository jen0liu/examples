################################################################
### Shared across people, please always check code into after you successfully running this code below Rscript toPresto.R [brand].
################################################################

library(jsonlite)
library(gdata)
library(hash)
library(reshape)
source("util.R")
source("brandParser.R")
source("directory.R")
args<-commandArgs(TRUE) ## this allows command line parameters

######### extract brands and tables from directory ##############

print(args[2])

if(args[1]=="all"){
	brands=brands_all
	tables=tables_all
}else{
	brands=strsplit(args[1],",")[[1]]
	tables=tables_all[brands]
}

######### Determine which file to parse #######################
## By default, it's always parsing the newest file. Allows different brands to override

for(i in 1:length(brands)){
  
	file.lists=fromJSON(paste("http://example/list/file/",brands[i],sep=""))$files[[1]]
	uploadTime=fromJSON(paste("http://example/list/file/",brands[i],sep=""))$uploadTime[1]
	
	for(j in 1:length(values(tables[brands[i]]))){
		
		table.name=values(tables[brands[i]])[j]
		print(table.name)
		############## individual brand parsers ##############
		data = brand.parser(brands[i],file.lists)	

		sink("done.txt", append=FALSE, split=FALSE)
		file.lists
		sink()
		print(file.lists)
		############## common for all files ##################
		
		data=data[order(data$week),]
		data$week=as.Date(data$week)
		data$week=data$week-1 ## Presto week is inclusive
		
		########## Data QA ###################################
		## generate plots here and can view plots. http://example.com/financial/plots/
		
		output.dir=file.path("/var/www/html/financial", "plot")
		dir.create(output.dir, showWarnings = FALSE)
		setwd(output.dir)
		
#		data0=data[which(data$measure==unique(data$measure)[2]),]
		num_measures <- length(unique(data$measure));
		rnd <- sample(num_measures,1);
		data0=data[which(data$measure==unique(data$measure)[rnd]),]
		data0=aggregate(originalkpi~week,data0,sum)
		png(paste(brands[i],".png",sep=""))
		plot(data0$week,data0$originalkpi,type="l",main=paste(brands[i],unique(data$measure)[1],sep=" - "))
		dev.off()

		########## Split into weekly files ############## 
				
		if(!is.na(args[2]) & args[2] == "debug") output.dir="/ftp/db-testing"
		else output.dir="/ftp/db-raw"
		output.dir=file.path(output.dir, table.name)
		print(output.dir)
		dir.create(output.dir, showWarnings = FALSE)
		output.dir=file.path(output.dir,as.integer(as.POSIXct(Sys.time())))
		output.dir=paste(output.dir,"_000",sep="")
		dir.create(output.dir, showWarnings = FALSE)
		setwd(output.dir)
		weekly <- split(data, data$week)
		lapply(names(weekly), function(x){write.table(as.data.frame(weekly[[x]])[,!names(weekly[[x]]) %in% c("week")],col.names = FALSE,row.names = FALSE, quote = FALSE,sep=",", file = paste(uploadTime,"_Quantifind_data_", x, ".csv",sep = ""))})
		
		########## Generate schema for presto and write done.txt ###########
		
		maps=hash(c("character","numeric","Date"),c("string","double","string"))
		types=sapply(data, class)
		names=colnames(data)
		mapped.names=hash(names,types)
		mapped.names=del("week",mapped.names)
		schema=""
		for(i in 1:length(names)){
			schema=paste(schema,"[ ",sep="")
			schema=paste(schema,'"',names[i],'", "',maps[[types[i]]],'"',sep="")
			schema=paste(schema,"],")
		}
		schema=substr(schema,1,nchar(schema)-1)
		body='{
  "schemaName" : "%s",
  "skipHeader" : false,
  "overwrite" : true,
  "textFileFieldDelimiter" : ",",
  "columnMapping" : [ %s ],
  "dynamicPartitions" : [ ],
  "staticPartition" : {
  "partitionName" : "week",
  "partitionType" : "date"
  }
}'
		schema=sprintf(body,table.name,schema)
		sink("schema.json", append=FALSE, split=FALSE)
		cat(schema)
		sink()

		body='
{
  "uploadTime": "%s",
  "files": ["%s"]
}
'
		file.names=sprintf(body,uploadTime,file.lists)
		sink("done.txt", append=FALSE, split=FALSE)
		cat(file.names)
		sink()
	}
}

