library(RPostgreSQL)
advertiser='client'
vertical='vertical_tweets'
intent='filtered_tweets_client'
con <- dbConnect(PostgreSQL(), host="example.com",user= "usr", password="usr", dbname="prototyping")

network=c('espn','hgtv','cbs','fox','abc','nbc','hst','usa','tnt','tbs')
networks=list()
for(i in 1:length(network)){
	query=paste("select t1.year,t1.month,t2.imp,t1.intent from (select extract(year from utc) as year, extract(quarter from utc) as month, count(distinct t.user_id) as intent from ",vertical," t inner join ",intent," i on t.id = i.id where t.utc > '2011-07-01' and t.utc < '2014-02-01' and not is_retweet and not has_url and is_english and dupe_count < 2 and array((select id from favs where title ~~* '",network[i],"%') union all (select f.id from favs f join network_to_fav nf on f.title=nf.fav where nf.network ~~* '",network[i],"')) && favs group by 1,2 order by 1,2) as t1 left outer join (select extract(year from utc) as year,extract(quarter from utc) as month, sum(avg_audience) as imp from adplacements ap join advertisers a on ap.advertiser_id = a.id where a.title ~* '",advertiser,"' and ap.utc > '2011-07-01' and ap.utc < '2014-02-01' and array[network_id] && array(select id from networks where title ~~* '",network[i],"') group by 1,2 order by 1,2) as t2 on t1.year = t2.year and t1.month = t2.month",sep="")	
	result <- dbSendQuery(con,query)
	df <- fetch(result, n = -1)
	networks[[i]]=df
}

segment=c('white','hispanic','black','asian','young','old','female','male')
segments=list()
for(i in 1:length(segment)){	
	query=paste("select t1.year,t1.month,t3.imp,t1.intent from (select extract(year from utc) as year, extract(month from utc) as month, count(distinct t.user_id) as intent from (select id,user_id,utc,eth_white as white,eth_hispanic as hispanic,eth_black as black,eth_asian as asian,eth_other as other,gen_female as female,gen_male as male,age_0to17+age_18to24 as young, age_25to29+age_30to34+age_35to39+age_40to49+age_50to99 as old from ",vertical," where not is_retweet and not has_url and is_english and dupe_count < 2) as t inner join ",intent," i on t.id = i.id where t.utc > '2011-07-01' and t.utc < '2014-02-01' and ",segment[i]," > 0.7 group by 1,2 order by 1,2) as t1 left outer join (select year,month,",segment[i]," as imp from (select year,month,sum(gen_male*impressions) as male,sum(gen_female*impressions) as female,sum(eth_white*impressions) as white,sum(eth_black*impressions) as black,sum(eth_hispanic*impressions) as hispanic,sum(eth_asian*impressions) as asian,sum(eth_other*impressions) as other,sum(age_18to24*impressions)+sum(age_25to34*impressions) as young,sum(age_35to44*impressions)+sum(age_45to54*impressions)+sum(age_55to64*impressions)+sum(age_65plus*impressions) as old from (select series_num,extract(year from utc) as year,extract(month from utc) as month,sum(avg_audience) as impressions from adplacements ap join advertisers a on ap.advertiser_id = a.id where a.title ~* '",advertiser,"' and utc > '2011-07-01' and utc < '2014-02-01' group by 1,2,3) as sum_audience inner join rentrak_series_demo using(series_num) group by 1,2) as t2) as t3 on t1.year = t3.year and t1.month = t3.month",sep="")	
	result <- dbSendQuery(con,query)
	df <- fetch(result, n = -1)
	segments[[i]]=df
}

imp_seg=rep(NA,length(segments))
intent_seg=rep(NA,length(segments))
for(i in 1:length(segments)){
	imp_seg[i]=sum(segments[[i]][,"imp"])
	intent_seg[i]=sum(segments[[i]][,"intent"])
}
#slices=list()
slices=c(networks,segments)
names_slices=c(network,segment)
names(slices)=names_slices

cost=rep(NA,length(slices))
for(i in 11:length(slices)){
	slice=slices[[i]]
#	slice=slices[[i]][slices[[i]][,"intent"]<max(slices[[i]][,"intent"]),]
#	slice=slices[[i]][slices[[i]][,"intent"]>min(slices[[i]][,"intent"]),]
	slice=slice[which(slice$imp>0),]
	model=lm(intent~imp+I(imp^2),data=slice)
	summary(model)
	plot(slice$imp,slice$intent)
	lines(slice$imp,predict(model),col="red")
	grid.max =  round(max(slice[,"imp"],na.rm=TRUE)*1.1)
	imp.grid = seq(0,grid.max, by=grid.max/1000)
	intent.grid=coef(model)[1]+coef(model)[2]*imp.grid + coef(model)[3]*imp.grid^2
	plot(imp.grid,intent.grid)
	intent.grid=intent.grid[1:(which(intent.grid==max(intent.grid))-1)]
	imp.grid=imp.grid[1:(which(intent.grid==max(intent.grid)))]
	plot(imp.grid,intent.grid)
	d.ipi = 1 / ( coef(model)[2] + 2*coef(model)[3]*imp.grid )
	d.ipi=d.ipi[1:max(which(d.ipi<4*min(d.ipi)))]
	intent.grid=intent.grid[1:length(d.ipi)]
	plot(intent.grid,d.ipi)
	avg_intent=mean(intent.grid,na.rm=TRUE)
	inc_intent=avg_intent+1
	low_intent=intent.grid[max(which(intent.grid<avg_intent),na.rm=TRUE)]
	high_intent=intent.grid[max(which(intent.grid<inc_intent),na.rm=TRUE)]
	low_ipi=d.ipi[max(which(intent.grid<avg_intent),na.rm=TRUE)]
	high_ipi=d.ipi[max(which(intent.grid<inc_intent),na.rm=TRUE)]
	cost[i]=(high_ipi - low_ipi)/(high_intent - low_intent)
	png(filename=paste(names_slices[i],".png",sep=""))
	plot(intent.grid,d.ipi,type="l",lty=1,lwd=3, main=paste("incremental impressions required for incremental intent - ",toupper(names_slices[i]),sep=""), xlab = "intent", ylab = "impressions")
	segments(min(intent.grid),low_ipi,low_intent,low_ipi,col="red")
	segments(low_intent,min(d.ipi),low_intent,low_ipi,col="red")
	segments(min(intent.grid),high_ipi,high_intent,high_ipi,col="blue")
	segments(high_intent,min(d.ipi),high_intent,high_ipi,col="blue")
	text(min(intent.grid),max(d.ipi),paste("incremental cost: ",round(cost[i])," impressions",sep=""),pos=4)
	dev.off()
}
names(cost)=names_slices