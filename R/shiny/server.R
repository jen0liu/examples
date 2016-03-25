library(shiny)
library(jsonlite)
library(lubridate)
library(hash)
library(RPostgreSQL)
#library(reshape)
# library(ggplot2)

# Define server logic required to draw a histogram
shinyServer(function(input, output) {	
	getConfigDemos = reactive({
				if(input$selectionPath == "byTemplate"){
					if(grepl("_",input$selectionConfigId)){
						sessionId = system(sprintf("curl http://example.com/getSessionId?configId=%s",input$selectionConfigId), intern=TRUE)
						fromJSON(sprintf("http://example.com/getSegmentSets?sessionId=%s", sessionId))
					}
				}
			})
			
	getConfigFavs = reactive({
				if(input$selectionPath == "byTemplate"){	
					if(grepl("_",input$selectionConfigId)){
						sessionId = system(sprintf("curl http://example.com/getSessionId?configId=%s",input$selectionConfigId), intern=TRUE)
						fromJSON(sprintf("http://example.com/getFavs?sessionId=%s", sessionId))
					}
				}
			})
			
	getConfigTopics = reactive({
				if(input$selectionPath == "byTemplate"){	
					if(grepl("_",input$selectionConfigId)){
						sessionId = system(sprintf("curl http://example.com/getSessionId?configId=%s",input$selectionConfigId), intern=TRUE)
						fromJSON(sprintf("http://example.com/getTopics?sessionId=%s", sessionId))
					}
				}
			})
	
	output$docs = renderText({
				if(input$selectionPath == "byTemplate"){
					paste("Using Template Configs. cleanFilter and level3Filter are applied when necessary.", "For both Counts function and Timeseires function, counts are de-duped by KPI period.", "Currently, for Counts and Timeseries functions, only support filters by Demos, Topics or Favs. To get Tags, add them to configs. Sample function supports Tags.", "You can select mupltiple filters. It will return the counts for each demo/topic/fav combo separately. Caution: it's slow to return if too many filters are selected. ","Timeseries function is scaled by twitter random sample by default. But you can also uncheck the 'scale' box to get the raw counts", "You can also correlate with KPI if you select timeseries - group by week",sep="\n")
				}
				else if(input$selectionPath == "byCluster"){
					paste("Using Cluster Directly. cleanFilter and level3Filter are NOT applied.", "Counts function de-dupes by the entire period (startDate and endDate).", "Timeseries function de-dupes daily.", "You can select mupltiple demo groups. It extracts counts based on 'OR' expression. Only one number will be returned.","Timeseries function is scaled by twitter random sample by default. But you can also uncheck the 'scale' box to get the raw counts", sep="\n")					
				}				
			})
	
	output$compares = renderText({
				if(input$selectionPath == "byTemplate"){
					paste("Compare the counts through the data cleaning/modeling funnel.", sep="\n")
				}
			})
	
	output$selectionConfigId = renderUI({
				if(input$selectionPath == "byTemplate"){
					configs = system("curl http://example.com/getAllIds", intern=TRUE)
					configs = strsplit(configs, ',')[[1]]
					selectInput("selectionConfigId", "Config Id", choices=configs, selected=configs[2])
				}
				else if(input$selectionPath == "byCluster"){
					configs = fromJSON("http://example.com/status")
					configs = configs$verticals$twitter
					configs = sort(configs)
					selectInput("selectionConfigId", "Vertical", choices=configs, selected=configs[2])
				}				
			})
			
	output$ids <- renderUI({
				if(input$selectionPath == "byTemplate"){
					if(grepl("_",input$selectionConfigId)){
						sessionId = system(sprintf("curl http://example.com/getSessionId?configId=%s",input$selectionConfigId), intern=TRUE)
						ids <- fromJSON(sprintf("http://example.com/getIds?sessionId=%s", sessionId))
						selectizeInput("ids", "Brand", choices=ids, selected=ids[1],multiple = FALSE)
					}
				}
				else if(input$selectionPath == "byCluster"){
					ids = c("(All)",fromJSON(sprintf("http://example.com/ids?targetVertical=%s",input$selectionConfigId)))
					selectizeInput("ids", "Brand", choices=ids, selected=ids[1],multiple = TRUE)
				}
				
			})

	output$demos <- renderUI({
				if(input$selectionPath == "byTemplate"){
					demos <- c("None")
					if(!is.null(getConfigDemos())){
						name = getConfigDemos()
						for(i in 1:length(name)){
							sessionId = system(sprintf("curl http://example.com/getSessionId?configId=%s",input$selectionConfigId), intern=TRUE)
							segments <- fromJSON(sprintf("http://example.com/getSegments?sessionId=%s&segmentSet=%s", sessionId,URLencode(name[i])))
							segments = paste(name[i],segments,sep=";")
							demos=c(demos,segments)
						}						
					}
				}
				else if(input$selectionPath == "byCluster"){
					demos = c("None","gender:female","gender:male","age:0to17","age:18to24","age:25to29","age:30to34","age:35to39","age:40to49","age:50to99",
							"ethnicity:white","ethnicity:hispanic","ethnicity:asian","ethnicity:black",
							"geo:Eastern_North","geo:Central_North","geo:Central_East","geo:Eastern_West","geo:Pacific_Major","geo:America_Alaska","geo:America_Hawai",
							"geo:Central_West","geo:Central_South","geo:Eastern_South","geo:Eastern_Mid","geo:Mountain_Texas","geo:Mountain_Major","geo:Pacific_Other")
				}
				selectizeInput("demos", "Demos", choices=demos, selected=demos[1],multiple = TRUE)
			})
	
	output$favs = renderUI({
				if(input$selectionPath == "byTemplate"){
					favs = c("None",getConfigFavs())
					selectizeInput("favs", "Favs", choices=favs, selected=favs[1],multiple = TRUE)
				}
				else if(input$selectionPath == "byCluster"){
					textInput("favs", "Favs:", "")
				}				
			})
	
	output$topics = renderUI({
				if(input$selectionPath == "byTemplate"){
					topics = c("None",getConfigTopics())
					selectInput("topics", "Topics", choices=topics, selected=topics[1])
					selectizeInput("topics", "Topics", choices=topics, selected=topics[1],multiple = TRUE)
				}
				else if(input$selectionPath == "byCluster"){
					textInput("topics", "Terms:", "")
				}				
			})
	output$level3 = renderUI({
				if(input$selectionPath == "byTemplate" & input$functions != "timeseries" & input$functions != "counts"){
					checkboxInput("level3", "Apply Level3 Filter", value = FALSE)
				}				
			})
	output$tags = renderUI({
				if(!(input$selectionPath == "byTemplate" & (input$functions != "timeseries" | input$functions != "counts"))){
					tags = c("(All - Pre Any Filter)","consumer","[uk,english,!(dupe),!(retweet),!(url)]","consumer","[canada,english,!(dupe),!(retweet),!(url)]","url","photo","english","spanish","mention","hashtag","retweet","dupe")
					selectizeInput("tags", "Tags", choices=tags, selected=tags[2],multiple = TRUE)
				}				
			})
	
	output$functions = renderUI({
				functions = c("timeseries","counts","sample", "termsCooccurring")
				selectInput("functions", "Cluster Functions", choices=functions, selected=functions[2])
			})
		
	output$dateRange = renderUI({
				start.date="2015-01-01"
				end.date=getLatestDate()
				dateRangeInput("datePeriod", "Date Period", start=start.date, end=end.date)
			})
	
	output$rawData <- renderTable({
				if((input$functions == "counts" | input$functions == "sample" | input$functions == "termsCooccurring")){
					data=data.frame(getData())
				}
#				else if(input$functions == "timeseries"){
#					data=data.frame(getData())
#					data=data.frame(date=as.character(as.Date(data$date)),count=data$count)
#				}
			}, 
			include.rownames=FALSE)

	output$compareCounts <- renderTable({
				if(input$functions == "counts"){
					data=data.frame(getCompareData())
				}
			}, 
			include.rownames=FALSE)
	
	
	output$distPlot <- renderPlot({
				if(input$functions == "timeseries"){
					data=getData()
					if (!"end_date"%in%colnames(data)) return()
					if(input$selectionPath == "byCluster") data$label="all"
					data$end_date=as.Date(data$end_date)
#					data$date=as.Date(data$end_date)
					if(input$scale==TRUE){
						ylabs="Scaled Counts"
					}else{
						ylabs="Counts"
					}
					labels=unique(data$label)
					ylims=c(min(data$count),max(data$count))
					plot(data$end_date[which(data$label==labels[1])],data$count[which(data$label==labels[1])],type='l',col=1,lwd=2,ylim=ylims,ylab=ylabs,xlab="end_date")					
					for(i in 2:length(labels)){
						lines(data$end_date[which(data$label==labels[i])],data$count[which(data$label==labels[i])],col=i)
					}
					if(input$selectionPath == "byTemplate" & input$groupBy == "byWeek" & input$correlate){
						financial = getFinancials()
						financial$start_date=as.Date(financial$start_date)
						financial=financial[order(financial$start_date),]
						financial$end_date=financial$start_date+7
						financial = financial[which(financial$start_date>=input$datePeriod[1]),]
						data$year = year(data$start_date)
						data$week = week(data$start_date)
						financial$year = year(financial$start_date)
						financial$week = week(financial$start_date)
						merged=merge(data[which(data$label==labels[1]),],financial,by=c("year","week"))
						correlation = cor(merged$count,merged$original_kpi)
						print(correlation)
						par(new=TRUE)
						plot(financial$end_date, financial$original_kpi,,type="l",lty=2,col=1,xaxt="n",yaxt="n",xlab="",ylab="")
						legend("topright",legend=c(labels,"scaled KPI"),col=c(seq(1,length(labels)),1),lty=c(rep(1,length(labels)),2),bty ="n",border=NA,lwd=2)
						text(as.Date(input$datePeriod[1])+60,0.95*max(financial$original_kpi),paste("correlation = ",format(round(correlation, 2), nsmall = 2),sep=""),col = ifelse(correlation < 0,'red','forestgreen'))
					}else{
						legend("topright",legend=labels,col=seq(1,length(labels)),lty=rep(1,length(labels)),bty ="n",border=NA,lwd=2)
					}
					
					
				}
			})

	output$comparePlot <- renderPlot({
				if(input$selectionPath == "byTemplate" & input$functions == "timeseries"){
					data=getCompareData()
					if (!"date"%in%colnames(data)) return()
					data$end_date=as.Date(data$date)
					if(input$scale==TRUE){
						ylabs="Scaled Counts"
					}else{
						ylabs="Counts"
					}
					plot(data$end_date,data$raw,type='l',col=1,ylab=ylabs,xlab="end_date",ylim=c(min(data[,c(2:4),]),max(data[,c(2:4),])),main="raw counts")
					lines(data$end_date,data$consumer,col=2)
					lines(data$end_date,data$intent,col=3,lwd=2)
					legend("topright",c("Level1","Level2","Level3"),col=c(1,2,3),lty=c(1,1,1),bty ="n",border=NA,lwd=2)
				}
			})

	output$comparePlotScaled <- renderPlot({
				if(input$selectionPath == "byTemplate" & input$functions == "timeseries"){
					data=getCompareData()
					if (!"date"%in%colnames(data)) return()
					data$end_date=as.Date(data$date)
					if(input$scale==TRUE){
						ylabs="Scaled Counts"
					}else{
						ylabs="Counts"
					}
					plot(data$end_date,scale(data$raw),type='l',col=1,ylab=ylabs,xlab="end_date",ylim=c(-5,5),main="scaled")
					lines(data$end_date,scale(data$consumer),col=2)
					lines(data$end_date,scale(data$intent),col=3,lwd=2)
					legend("bottomright",c("Level1","Level2","Level3"),col=c(1,2,3),lty=c(1,1,1),bty ="n",border=NA,lwd=2)
				}
			})
	
	output$downloadData <- downloadHandler(
			filename = function() { paste(input$ids,input$demos,input$favs,input$topics,input$functions,'.csv', sep='_') },
			content = function(file) {
				write.csv(getData(), file,row.names=FALSE)
			}
	)

	output$compareData <- downloadHandler(
			filename = function() { paste(input$ids,input$demos,input$favs,input$topics,input$functions,'compare.csv', sep='_') },
			content = function(file) {
				write.csv(getCompareData(), file,row.names=FALSE)
			}
	)
	getData <- eventReactive(input$goButton,{
						if(input$functions == "sample" ){
							getSample()
						}else if(input$functions == "counts" ){
							data=getCounts()
						}else if(input$functions == "timeseries"){
							data=getTimeSeries()[[1]]
							data$date=as.Date(data$date)
							sample=data
							if(input$scale == TRUE){
								sample=getTimeSeries()[[2]]
								sample$date=as.Date(sample$date)
							}
							labels=unique(data$label)
							for(i in 1:length(labels)){
								data0=data[which(data$label==labels[i]),]
								data0=getScaled(input$selectionPath,input$groupBy,input$scale,data0,sample)
								if(input$selectionPath=="byTemplate") data0=data0[,c("label","start_date","end_date","count")]
								else data0=data0[,c("start_date","end_date","count")]								
								data0=data0[which(nchar(data0$start_date)>4),]
								if(i == 1){
									output=data0
								}else{
									output=rbind(output,data0)
								}
							}
							output
						}else if(input$functions == "termsCooccurring"){
							getTermsCoocuring()
						}
				})
				
	getTemplateCall = eventReactive(input$goButton,{
				configId = input$selectionConfigId
				ids = input$ids
				startDate = input$datePeriod[1]
				endDate = input$datePeriod[2]
				sessionId = system(sprintf("curl http://example.com/getSessionId?configId=%s",configId), intern=TRUE)
				demos = input$demos
				favs = input$favs
				tags=input$tags
				topics = input$topics
				level3 = tolower(input$level3)
				rtcFunction = input$functions
				scale = input$scale
				if(rtcFunction == "timeseries"){
					if(input$groupBy == "byDay") group.by="P1D"
					else if(input$groupBy == "byWeek") group.by="P1W"
					else if(input$groupBy == "byMonth") group.by="P1M"
					else if(input$groupBy == "byYear") group.by="P1Y"					
				}else if(rtcFunction == "counts"){
					group.by=paste("P",as.numeric(as.Date(endDate)-as.Date(startDate), units="days"),"D",sep="")
				}
				
				if(rtcFunction == "termsCooccurring"){
					rtcString = "http://example.com/%s?sessionId=%s&targetId=%s&targetStartDate=%s&targetEndDate=%s&targetLevel3Filter=%s&refId=%s&refStartDate=%s&refEndDate=%s&refLevel3Filter=%s"									
					json.call = sprintf(rtcString,
							"compare",
							sessionId,
							gsub('&','%26',URLencode(ids)),
							startDate,
							endDate,
							level3,
							gsub('&','%26',URLencode(ids)),
							startDate,
							endDate,
							level3)					
				}else if (rtcFunction == "timeseries" | rtcFunction == "counts"){
					rtcString = "http://example.com/brand/flexible?configId=%s&targetKeyOverride=%s&values=count&reportPeriodOverride=%s&reportStartDate=%s&reportEndDate=%s"					
					json.call = sprintf(rtcString,
							configId,
							gsub('&','%26',URLencode(ids)),
							group.by,
							startDate,
							endDate)
				}else{
				rtcString = "http://example.com/%s?sessionId=%s&targetId=%s&targetStartDate=%s&targetEndDate=%s&targetLevel3Filter=%s"									
				json.call = sprintf(rtcString,
						rtcFunction,
						sessionId,
						gsub('&','%26',URLencode(ids)),
						startDate,
						endDate,
						level3)
				}
				
				if(rtcFunction == "timeseries" | rtcFunction == "counts"){
					if( !is.null(demos[1]) & demos[1] != "None") {
						demoFilterStr = paste("selectors=demos:", substr(demos[1],1,(gregexpr(";",demos[1])[[1]]-1)), sep="")
						json.call <- paste(json.call, URLencode(demoFilterStr), sep="&")
					}
					if( !is.null(topics[1]) & topics[1] != "None") {
						topicFilterStr = "selectors=topic"
						json.call <- paste(json.call, URLencode(topicFilterStr), sep="&")
					}
					if( !is.null(favs[1]) & favs[1] != "None") {
						favFilterStr = "selectors=fav"
						json.call <- paste(json.call, URLencode(favFilterStr), sep="&")						
					}
				}else{
					if( !is.null(demos[1]) & demos[1] != "None" ) {
						demoFilterStr = paste(paste("targetSegmentSet=", demos, sep=""), sep="&")
						json.call <- paste(json.call, URLencode(demoFilterStr), sep="&")
					}
					if( !is.null(favs[1]) & favs[1] != "None") {
						favFilterStr = paste(paste("targetPersona=", favs, sep=""), collapse="&")      
						json.call <- paste(json.call, URLencode(favFilterStr), sep="&")
					}
					if( !is.null(topics[1]) & topics[1] != "None") {
						topicFilterStr = paste(paste("targetTopic=", topics, sep=""), collapse="&")      
						json.call <- paste(json.call, URLencode(topicFilterStr), sep="&")
					}
					if( !is.null(tags[1]) & tags[1] != "consumer") {
						if(tags == "(All - Pre Any Filter)") tags = "!()"
						tagFilterStr = paste(paste("targetTags=(", paste(tag,collapse=","), ")",sep=""), collapse="&")       
						json.call <- paste(json.call, URLencode(tagFilterStr), sep="&")
					}	
				}
				output=list(json.call)
				if(rtcFunction == "timeseries" & scale == TRUE){
					rtcString = "http://example.com/brand/flexible?configId=sample_config&targetKeyOverride=Sample&values=count&reportPeriodOverride=%s&reportStartDate=%s&reportEndDate=%s"					
					sample.call = sprintf(rtcString,
							group.by,
							startDate,
							endDate)
					output[[2]]=sample.call
				}
				print(output)
				output
			})
	
	getClusterCall= eventReactive(input$goButton,{
				configId = input$selectionConfigId
				ids=input$ids[1]
				scale=input$scale
#				if(ids=="(All)") ids="!()"
				if(length(input$ids)>1){
					for(i in 2:length(input$ids)){
						ids=paste(ids,input$ids[i],sep=",")
					}
				}
				ids=paste("(",ids,")",sep="")
				startDate = input$datePeriod[1]
				endDate = input$datePeriod[2]
				demos = input$demos
				favs = input$favs
				topics = paste("(",input$topics,")",sep="")
				level3 = tolower(input$level3)
				tag=input$tags
				rtcFunction = input$functions
				if(rtcFunction == "termsCooccurring"){
					if (ids == "()"){
						json.call = ""
					}else if(ids == "((All))"){
						rtcString = "http://example.com/%s?targetVertical=%s&targetStartDate=%s&targetEndDate=%s&refVertical=%s&refStartDate=%s&refEndDate=%s"									
						json.call = sprintf(rtcString,
								"compareTerms",
								configId,
								startDate,
								endDate,
								configId,
								startDate,
								endDate)
					}else{
						rtcString = "http://example.com/%s?targetVertical=%s&targetIds=%s&targetStartDate=%s&targetEndDate=%s&refVertical=%s&refIds=%s&refStartDate=%s&refEndDate=%s"									
						json.call = sprintf(rtcString,
								"compareTerms",
								configId,
								URLencode(ids),
								startDate,
								endDate,
								configId,
								URLencode(ids),
								startDate,
								endDate)
					}					
				}else{
					if (ids == "()"){
						json.call = ""
					}else if(ids == "((All))"){
						rtcString = "http://example.com/%s?targetVertical=%s&targetStartDate=%s&targetEndDate=%s"									
						json.call = sprintf(rtcString,
								rtcFunction,
								configId,
								startDate,
								endDate)
					}else{
						rtcString = "http://example.com/%s?targetVertical=%s&targetIds=%s&targetStartDate=%s&targetEndDate=%s"									
						json.call = sprintf(rtcString,
								rtcFunction,
								configId,
								URLencode(ids),
								startDate,
								endDate)
					}				
				}
				if( !is.null(demos[1]) & demos[1] != "None" ) {
					demoFilterStr="targetExpr="
					for(i in 1:length(demos)){
						demo.combos = paste(demos[i], sep="*")
					}
					demoFilterStr=paste(demoFilterStr,demo.combos,sep="")
					json.call <- paste(json.call, URLencode(demoFilterStr), sep="&")
				}
				if( !is.null(favs) & favs != "") {
					favFilterStr = paste(paste("targetFavs=", favs, sep=""), collapse="&")      
					json.call <- paste(json.call, URLencode(favFilterStr), sep="&")
				}
				if( !is.null(topics) & topics != "()") {
					topics = gsub('([\\"])','',topics)
					topicFilterStr = URLencode(paste(paste("targetTerms=", topics, sep=""), collapse="&"))
					topicFilterStr = gsub('#','%23',topicFilterStr)
					json.call <- paste(json.call, topicFilterStr, sep="&")
				}
				if( !is.null(tags) & tag != "consumer") {
					if(tag == "(All - Pre Any Filter)") tag = "!()"
					tagFilterStr = paste(paste("targetTags=(", paste(tag,collapse=","), ")",sep=""), collapse="&") 
					print(tagFilterStr)
					json.call <- paste(json.call, URLencode(tagFilterStr), sep="&")
				}
				output=list(json.call)
				if(scale == TRUE){
					rtcString = "http://example.com/timeseries?targetVertical=sample&targetStartDate=%s&targetEndDate=%s"									
					sample.call = sprintf(rtcString,
							startDate,
							endDate)
					output[[2]]=sample.call
				}
				output
				})


	getCounts = eventReactive(input$goButton,{
				selectionPath=input$selectionPath
				
				if(selectionPath == "byTemplate"){				
					demos=substr(input$demos,(gregexpr(";",input$demos[1])[[1]]+1),nchar(input$demos))
					topics=input$topics
					favs=input$favs
					cluster.call = getTemplateCall()[[1]]
					rtc.raw.data = fromJSON(cluster.call)
					cluster.data = as.data.frame(rtc.raw.data$results$data)
					
					print("from signum backend")
					print(cluster.call)
					cols="brand"
					if(demos[1]!="None"){
						cluster.data = cluster.data[which(cluster.data$segment%in%demos),]
						cols = c(cols,"segment")
					}
					if(topics[1]!="None"){
						cluster.data = cluster.data[which(cluster.data$topic%in%topics),]
						cols = c(cols,"topic")
					} 
					if(favs[1]!="None"){
						cluster.data = cluster.data[which(cluster.data$fav%in%favs),]
						cols = c(cols,"fav")
					} 
					data.frame(cluster.data[,c(cols,"Count")])
										
				}else if(selectionPath == "byCluster"){
					cluster.call=getClusterCall()[[1]]
					cluster.data = RJSONIO::fromJSON(cluster.call)
					data.frame(count=cluster.data[[1]]$groupCounts[[1]]$count)		
				}
			})

	getTermsCoocuring = eventReactive(input$goButton,{
				if(input$selectionPath == "byTemplate"){
					cluster.call = getTemplateCall()[[1]]
				}
				else if(input$selectionPath == "byCluster"){
					cluster.call = getClusterCall()[[1]]
				}
				cluster.data = fromJSON(cluster.call)
				word.list = cluster.data$entities[1][1:200,]
				data.frame(terms=word.list[!(grepl(input$topics,word.list))])
			})
	
	
	getTimeSeries  = eventReactive(input$goButton,{
				selectionPath=input$selectionPath
				demos=substr(input$demos,(gregexpr(";",input$demos[1])[[1]]+1),nchar(input$demos))
				topics=input$topics
				favs=input$favs
				
				if(selectionPath == "byTemplate"){
					cluster.call = getTemplateCall()[[1]]
					rtc.raw.data = fromJSON(cluster.call)
					cluster.data = as.data.frame(rtc.raw.data$results$data[[1]])
					cols="brand"
					if(demos[1]!="None"){
						cluster.data = cluster.data[which(cluster.data$segment%in%demos),]
						cols = c(cols,"segment")
					}
					if(topics[1]!="None"){
						cluster.data = cluster.data[which(cluster.data$topic%in%topics),]
						cols = c(cols,"topic")
					} 
					if(favs[1]!="None"){
						cluster.data = cluster.data[which(cluster.data$fav%in%favs),]
						cols = c(cols,"fav")
					}
					cluster.data=cluster.data[,c("date","Count",cols)]
					colnames(cluster.data)[2]="count"
					for(i in 1:nrow(cluster.data)){
						cluster.data[i,"label"]=paste(cluster.data[i,cols],collapse=":")
					}
					
					cluster.data=as.data.frame(cluster.data[,c("date","label","count")])
					output=list()
					output[[1]]=cluster.data
					if(input$scale==TRUE){
						sample.call = getTemplateCall()[[2]]
						sample.raw.data = fromJSON(sample.call)
						sample.data = as.data.frame(sample.raw.data$results$data[[1]])
						sample.data=sample.data[,c("date","Count")]
						colnames(sample.data)=c("date","count")
						sample.data$label="Sample"
						output[[2]]=as.data.frame(sample.data[,c("date","label","count")])
					}
					output
				}else if(selectionPath == "byCluster"){
					cluster.call=getClusterCall()[[1]]
					cluster.data = RJSONIO::fromJSON(cluster.call)
					cluster.data=cluster.data[[1]]$groupTs[[1]]$ts
					num=length(cluster.data)
					d0=data.frame(date=rep(as.Date("2011-01-01"),num),count=rep(NA,num))
					for(n in 1:num){
						d0$date[n]=cluster.data[[n]]$date
						d0$count[n]=cluster.data[[n]]$count
					}
					cluster.data=d0
					cluster.data$label = "all"
					output=list()
					output[[1]]=cluster.data
					if(input$scale==TRUE){
						sample.call=getClusterCall()[[2]]
						sample.data = RJSONIO::fromJSON(sample.call)
						sample.data=sample.data[[1]]$groupTs[[1]]$ts
						num=length(sample.data)
						d0=data.frame(date=rep(as.Date("2011-01-01"),num),count=rep(NA,num))
						for(n in 1:num){
							d0$date[n]=sample.data[[n]]$date
							d0$count[n]=sample.data[[n]]$count
						}
						sample.data=d0
						sample.data$label = "Sample"
						output[[2]]=sample.data
					}
					output
				}
			})
	getSample = eventReactive(input$goButton,{
				if(input$selectionPath == "byTemplate"){
					cluster.call = getTemplateCall()[[1]]
					cluster.data = fromJSON(cluster.call)
				}
				else if(input$selectionPath == "byCluster"){
					cluster.call = getClusterCall()[[1]]
					cluster.data = RJSONIO::fromJSON(cluster.call)
					num=length(cluster.data)
					d0=data.frame(txt=rep(NA,num))
					for(n in 1:num){
						d0$txt[n]=cluster.data[[n]]$txt
					}
					cluster.data=d0
				}
				data.frame(cluster.data$txt)
			})

	getFinancials = eventReactive(input$goButton,{
				if(input$selectionPath == "byTemplate" & input$correlate == TRUE){
					sessionId = system(sprintf("curl http://example.com/getSessionId?configId=%s",input$selectionConfigId), intern=TRUE)
					tableName = fromJSON(sprintf("http://example.com/getConfig?configId=%s", input$selectionConfigId))$tableName										
					con <- dbConnect(PostgreSQL(), host="example.com",user= "usr", password="pwd", dbname="table")
					query = paste("select * from", tableName)
					data=rs <- dbSendQuery(con,query)
					data=fetch(data,n=-1)
					configDef = fromJSON(sprintf("http://example.com/getConfig?configId=%s", input$selectionConfigId))$qpidEntityDefinitions
					nameTable = as.data.frame(matrix(nrow=length(configDef),ncol=2))
					for(i in 1:length(configDef)){
						nameTable[i,1]=names(configDef[i])
						nameTable[i,2]=as.character(as.data.frame(configDef[i])[,1])
					}
					colnames(nameTable)=c("name","id")
					data=data[which(data$brand == input$ids | data$brand == nameTable[which(nameTable$name==input$ids),"id"]),c("brand","start_date","original_kpi")]
					data$start_date=as.Date(data$start_date/86400000,origin="1970-01-01")
					data.frame(data)	
				}
			})

	getCompareData =eventReactive(input$goButton,{
					configId = input$selectionConfigId
					ids = input$ids
					startDate = input$datePeriod[1]
					endDate = input$datePeriod[2]
					sessionId = system(sprintf("curl http://example.com/getSessionId?configId=%s",configId), intern=TRUE)
					demos = input$demos
					favs = input$favs
					tag=input$tags
					topics = input$topics
					level3 = tolower(input$level3)
					rtcFunction = input$functions
					
					rtcString = "http://example.com/%s?sessionId=%s&targetId=%s&targetStartDate=%s&targetEndDate=%s"									
					consumer.call = sprintf(rtcString,
								rtcFunction,
								sessionId,
								gsub('&','%26',URLencode(ids)),
								startDate,
								endDate)
					
					sessionId = system(sprintf("curl http://example.com/getSessionId?configId=%s","sample_config"), intern=TRUE)
					rtcString = "http://example.com/%s?sessionId=%s&targetId=Sample&targetStartDate=%s&targetEndDate=%s"									
					sample.call = sprintf(rtcString,
								rtcFunction,
								sessionId,
								startDate,
								endDate)
						
					intent.call =paste(consumer.call, "targetLevel3Filter=TRUE&targetExtraFilter=TRUE", sep="&")
						
					rawFilterStr = paste(paste("targetTags=", "!()", sep=""), sep="&")
					raw.call = paste(consumer.call, URLencode(rawFilterStr), sep="&")
					
					print(intent.call)
					print(consumer.call)
					print(raw.call)
					raw.data = fromJSON(raw.call)
					consumer.data = fromJSON(consumer.call)
					intent.data = fromJSON(intent.call)
					if(rtcFunction == "timeseries"){
						raw.data=as.data.frame(raw.data[[2]][[1]]$ts[[1]])
						consumer.data=as.data.frame(consumer.data[[2]][[1]]$ts[[1]])
						intent.data=as.data.frame(intent.data[[2]][[1]]$ts[[1]])
						raw.data$date=as.Date(raw.data$date)
						consumer.data$date=as.Date(consumer.data$date)
						intent.data$date=as.Date(intent.data$date)
						raw.data$label="all"
						consumer.data$label="all"
						intent.data$label="all"
						sample = consumer.data
						if(input$scale == TRUE){
							sample = fromJSON(sample.call)
							sample=as.data.frame(sample[[2]][[1]]$ts[[1]])			
							sample$date=as.Date(sample$date)
							sample$label="all"
						}						
#						sample=sample[which(sample$date>=min(raw.data$date) & sample$date<=max(raw.data$date)),]
						raw.data=getScaled("byCluster",input$groupBy,input$scale,raw.data,sample)
						consumer.data=getScaled("byCluster",input$groupBy,input$scale,consumer.data,sample)
						intent.data=getScaled("byCluster",input$groupBy,input$scale,intent.data,sample)
						
						data.frame(date=raw.data$end_date,raw=raw.data$count,consumer=consumer.data$count,intent=intent.data$count)
					}else{
						data.frame(raw=raw.data$groupCounts[[1]]$count,consumer=consumer.data$groupCounts[[1]]$count,intent=intent.data$groupCounts[[1]]$count)
					}
			})
	
	getWeekly = function(data){
#		colnames(data)=c("date","count")
		data$year=year(data$date)
		data$week=week(data$date)
		data=aggregate(count~year+week,data,sum)
		data$date=as.Date(strptime(paste(data$year, (data$week*7-6),sep=" "),format="%Y %j"))+7
		data=data[1:(nrow(data)-1),]
		data=data[order(data$date),]
		data		
	}
	
	getMonthly = function(data){
		data$year=year(data$date)
		data$month=month(data$date)
		data=aggregate(count~year+month,data,sum)
		data$date=as.Date(paste(data$year,data$month,"01",sep="-")) %m+% months(1)
		data=data[1:(nrow(data)-1),]
		data=data[order(data$date),]
		data
	}

	getYearly = function(data){
		data$year=year(data$date)
		data=aggregate(count~year,data,sum)
		data$date=as.Date(paste((data$year+1),"01","01",sep="-"))								
		data=data[1:(nrow(data)-1),]
		data=data[order(data$date),]
		data
	}
	
	getMerged = function(data1,data2){
		if(is.null(data1$label)){
			data=merge(data1[,c("date","count")],data2[,c("date","count")],by=c("date"))
			data$count=data$count.x/data$count.y
			data=data[,c("date","count")]
		}else{
			data=merge(data1[,c("label","date","count")],data2[,c("date","count")],by=c("date"))
			data$count=data$count.x/data$count.y
			data=data[,c("label","date","count")]
		}		
		data
	}

	getScaled = function(selectionPath,groupBy,scale,data,sample){
		if(selectionPath == "byCluster"){
			if(groupBy == "byDay"){
				if(scale==TRUE){
					data=getMerged(data,sample)
				}
				data$end_date = data$date
				data$start_date = data$end_date
			}else if(groupBy == "byWeek"){
				data=getWeekly(data)				
				if(scale==TRUE){
					sample=getWeekly(sample)
					sample=sample[1:(nrow(sample)-1),]
					data=getMerged(data,sample)
				}
				data$end_date = data$date
				data$start_date = data$end_date - 7
			}else if(groupBy == "byMonth"){
				data=getMonthly(data)				
				if(scale==TRUE){
					sample=getMonthly(sample)
					sample=sample[1:(nrow(sample)-1),]
					data=getMerged(data,sample)
				}
				data$end_date = data$date
				data$start_date = data$end_date %m+% months(-1)
			}else if(groupBy == "byYear"){
				data=getYearly(data)				
				if(scale==TRUE){
					sample=getYearly(sample)
					sample=sample[1:(nrow(sample)-1),]
					data=getMerged(data,sample)
				}
				data$end_date = data$date
				data$start_date = data$end_date - 365
			}
		}else{
				if(scale==TRUE){
					data=getMerged(data,sample)
				}
				data$end_date = data$date
				data$start_date=data$end_date-(data$end_date[2]-data$end_date[1])
		}		
		data
	}
	getLatestDate = reactive({
				if(input$selectionPath == "byTemplate"){
					configId = input$selectionConfigId
					sessionId = system(sprintf("curl http://example.com/getSessionId?configId=%s",configId), intern=TRUE)
					if(nchar(sessionId[1]) == 36){
						cluster.call = sprintf("http://example.com/timeseries?sessionId=%s",sessionId)
						cluster.data = fromJSON(cluster.call)
						as.Date(cluster.data$groupTs[[1]]$ts[[1]]$date[length(cluster.data$groupTs[[1]]$ts[[1]]$date)])
					}
				}else{
					cluster.call = sprintf("http://example.com/timeseries?targetVertical=%s",input$selectionConfigId)
					cluster.data = fromJSON(cluster.call)
					as.Date(cluster.data$groupTs[[1]]$ts[[1]]$date[length(cluster.data$groupTs[[1]]$ts[[1]]$date)])
				}
												
			})	
	
})
	
