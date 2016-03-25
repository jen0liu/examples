rank.features <- function(brand,id,modeling.dir,seasonal.kpi = FALSE){
	

library(lubridate)
library(quantreg)
weekly=1
monthly=0
daily=0
seasonal.kpi=1


project = "luxury"
brand = "luxury"
id = "luxury:12345"

modeling.dir=paste("~/Documents/",project,"/",sep="")

kpi.file = paste(modeling.dir,"data/",gsub(":","_",id),"_decomposed.csv",sep="")
#text.file = paste(modeling.dir,id,'.text.mtx',sep="")
#text.file = paste(modeling.dir,"aggregated/",gsub(":","_",id),"/results",sep="")
text.file = paste(modeling.dir,"overall/",brand,"/results",sep="")
#text.file = paste(modeling.dir,"results3",sep="")

min.date = as.Date("2014-01-01")

source("~/example/R/util.R")
m = mm.load(text.file)

kpi=read.csv(kpi.file)
kpi$DateMonthYear=as.Date(kpi$end_date)
kpi$kpi=kpi$original_kpi
kpi.all = kpi
kpi=kpi[53:nrow(kpi.all),]
kpi.last.year=kpi.all[1:nrow(kpi),]
kpi.target=cbind(kpi,kpi.last.year[,"original_kpi"])
colnames(kpi.target)[ncol(kpi.target)]="last_year_kpi"

plot(kpi$DateMonthYear,kpi$trained_kpi,type="l")
tb.spl <- smooth.spline(kpi$DateMonthYear, kpi$trained_kp)
ines(tb.spl, col = "blue")

## adding forecast
forecast.date=as.Date("2014-01-01")
kpi$kpi=kpi$original_kpi
kpi_ts=ts(kpi$kpi[kpi$DateMonthYear<forecast.date],frequency=52,start=c(2011,50))
kpi.hw=HoltWinters(kpi_ts)

t=predict(kpi.hw,n.ahead=nrow(kpi[kpi$DateMonthYear>=forecast.date,]))
t=as.data.frame(t)
t$DateMonthYear=kpi$DateMonthYear[kpi$DateMonthYear>=forecast.date]
plot(kpi$DateMonthYear,kpi$kpi,type="l",ylim=c(0,250000000))
lines(t$DateMonthYear,t$fit,col=2)
legend("bottomright",'groups',c("actual","forecasted baseline"),lty=c(1,1),col=c("black","red"),bty ="n",border=NA)

kpi=merge(kpi,t,by="DateMonthYear")
kpi$kpi=kpi$fit-kpi$kpi


#kpi = read.table(
#		 kpi.file,
#		 sep = ",",
#		 header = TRUE,
#		 col.names = c("utc", "kpi")
#		 )
#kpi$DateMonthYear = utcToDate(kpi$utc)
# to modify the kpi itself, for example to be a residual relative to a seasonal model, do it here:

#kpi=kpi[which(kpi$DateMonthYear >= min.date),]
### aggregate monthly


if(seasonal.kpi){
	#compute a model over lagged kpi, then form the residual, and replace the kpi data.frame
	kpi=kpi[order(kpi$DateMonthYear),]	
#	kpi_ts=ts(kpi$kpi,frequency=7)
	kpi_ts=ts(kpi$kpi,frequency=52,start=c(2011,1))
	kpi_ts_dec=decompose(kpi_ts)		
	plot(kpi_ts_dec)
	kpi.target=kpi
	kpi.target$kpi=kpi_ts_dec$seasonal
	kpi.target$kpi=kpi_ts-kpi_ts_dec$seasonal
}else kpi.target = kpi

#kpi.target$kpi=1/kpi.target$kpi

lag = 0 # months
df = as.data.frame(m)
entities = names(df)
# old formt
#df$utc = as.numeric(rownames(df))
#rownames(df) = NULL
#df$utc = as.numeric(rownames(df))
#df$DateMonthYear = utcToDate(df$utc)
df$DateMonthYear = as.Date(rownames(df))
if(daily){
	dy=df
	dz = merge(dy[ c("DateMonthYear", entities)], kpi.target[, c("kpi", "DateMonthYear")], by = "DateMonthYear")
}

## aggregate weekly
if(weekly){
	d0 = head(kpi$DateMonthYear, 1)
	dT = 7
	df$DateMonthYear = d0 + dT * (as.numeric(df$DateMonthYear - d0 + dT - 1) %/% dT)
	dy = aggregate(. ~ DateMonthYear, df[ ,c("DateMonthYear", entities)], sum)
	dz = merge(dy[ c("DateMonthYear", entities)], kpi.target[, c("original_kpi", "DateMonthYear")], by = "DateMonthYear")
}

if(monthly){
	kpi$Year=year(kpi$DateMonthYear)
	kpi$Month=month(kpi$DateMonthYear)
	kpi=aggregate(kpi~Year+Month,data=kpi,sum)
	kpi$DateMonthYear=as.Date(paste(kpi$Year, kpi$Month,"15",sep="-"))
	kpi.target=kpi
	d0 = head(kpi$DateMonthYear, 1)
	dT = 30.5
	df$Month = month(df$DateMonthYear)
	df$Year = year(df$DateMonthYear)
	dy = aggregate(. ~ Year+Month, df[ ,c("Year","Month", entities)], sum)
	dy$DateMonthYear=as.Date(paste(dy$Year, dy$Month,"15",sep="-"))
	dz = merge(dy[ c("DateMonthYear", entities)], kpi.target[, c("kpi", "DateMonthYear")], by = c("DateMonthYear"))	
}

if(derivative=1){
	kpi.target_lagged=kpi.target$kpi[1:(nrow(kpi.target)-1)]
	kpi.target$kpi_lagged[1]=NA
	kpi.target$kpi_lagged[2:nrow(kpi.target)]=kpi_lagged
	kpi.target$kpi=kpi.target$kpi-kpi.target$kpi_lagged
	dy_lagged=dy[2:nrow(dy),2:ncol(dy)]
	dy_delta=dy[1:(nrow(dy)-1),2:ncol(dy)]-dy_lagged
	dy_delta$DateMonthYear=dy$DateMonthYear[2:nrow(dy)]
	dy=dy_delta
	dz = merge(dy[ c("DateMonthYear", entities)], kpi.target[, c("kpi", "DateMonthYear")], by = "DateMonthYear")
}

if(scale.entity=1){
	m_sample = mm.load("~/Documents/sampling/sample")
	lag = 0 # months
	df_sample = as.data.frame(m_sample)
	top=sort(colSums(df_sample[,1:(ncol(df_sample)-1)]),decreasing=T)
	df_sample = df_sample[ ,names(top)[1:10000]]
	entities_sample = names(df_sample)[2:ncol(df_sample)]
	df_sample$DateMonthYear = as.Date(rownames(df_sample))
	d0 = head(kpi$DateMonthYear, 1)
	dT = 7
	df_sample$DateMonthYear = d0 + dT * (as.numeric(df_sample$DateMonthYear - d0 + dT - 1) %/% dT)
	df_sample=aggregate(. ~ DateMonthYear, df_sample[ ,c("DateMonthYear", entities_sample)], sum)
	dy1=dy[1:nrow(df_sample),]
	dy0=dy1
	for(i in 1:length(entities)){
		if(length(which(entities_sample==entities[i]))){
			dy0[,entities[i]]=(dy1[,entities[i]])/(df_sample[,entities[i]])
		} else {
			dy0[,entities[i]]=NA
		}
	}
	dy0=dy0[,colSums(is.na(dy0))<nrow(dy0)]
	entities=names(dy0)[2:ncol(dy0)]
	dz = merge(dy0[ c("DateMonthYear", entities)], kpi.target[, c("kpi", "DateMonthYear")], by = "DateMonthYear")
	dz=dz[which(dz$DateMonthYear>min.date),]
}

if(scale.total=1){
	url = "http://example.com/timeseries?targetVertical=sample"
	sample_counts=fromJSON(url)$groupTs[[1]]$ts[[1]]
#	sample_counts=read.csv("/Users/jen/Documents/heineken_mar2015/scaling/scaling_daily.csv")
	df_sample_full=sample_counts
	colnames(df_sample_full)=c("DateMonthYear","TOTAL")
	df_sample_full$DateMonthYear = as.Date(df_sample_full$DateMonthYear)
	df_sample_full=df_sample_full[which(df_sample_full$DateMonthYear>=min.date),]
#	m_sample = mm.load("~/Documents/sampling/sample")
#	lag = 0 # months
#	df_sample_full = as.data.frame(m_sample)
#	df_sample_full$TOTAL=rowSums(df_sample_full)
#	df_sample_full$DateMonthYear = as.Date(rownames(df_sample_full))
	d0 = head(kpi$DateMonthYear, 1)
	dT = 7
	df_sample_full$DateMonthYear = d0 + dT * (as.numeric(df_sample_full$DateMonthYear - d0 + dT - 1) %/% dT)
	df_sample_full=aggregate(. ~ DateMonthYear, df_sample_full[ ,c("DateMonthYear", "TOTAL")], sum)
#	df_sample_full=df_sample_full[which(df_sample_full$DateMonthYear>=d0),]
	dy1=dy[1:nrow(df_sample_full),]
	dy0=dy1
	for(i in 1:length(entities)){
		dy0[,entities[i]]=dy1[,entities[i]]/df_sample_full$TOTAL
	}
	entities=names(dy0)[2:ncol(dy0)]
#	entities=names(dy0)[3:(ncol(dy0)-1)]
#	dz = merge(dy0[ c("DateMonthYear", entities)], kpi.target[, c("original_kpi", "last_year_kpi", "DateMonthYear")], by = "DateMonthYear")
	dz = merge(dy0[ c("DateMonthYear", entities)], kpi.target[, c("kpi", "DateMonthYear")], by = "DateMonthYear")
	dz=dz[which(dz$DateMonthYear>min.date),]
	#	dzy = merge(dy[ c("DateMonthYear", entities)], kpi.target[, c("kpi", "DateMonthYear")], by = "DateMonthYear")
}

#dz$kpi=dz$original_kpi-dz$last_year_kpi
#dz$kpi=dz$original_kpi
x = apply(dz[ ,entities], 2, function(col) cor(col, dz$kpi))
oc = apply(dz[ ,entities], 2, mean)
n = apply(dz[ ,entities], 2, sum)*mean(df_sample_full$TOTAL)
#n = apply(dz[ ,entities], 2, sum)
sd = apply(dz[ ,entities], 2, sd)

M = as.matrix(dz[,entities])
M = scale(M, center = colMeans(M), scale = FALSE)
z = (t(M)%*%M) / nrow(M)

build <- function(...){
	e1 = unlist(list(...))
	# allow list of entities on input
	ts = rowSums(dz[,e1])

	sxx = mean(ts^2)#z[e1,e1] # variance of input series
	syy = diag(z)[entities] # variances of all candidate additions
	sxy = colSums(z[e1,entities]) # covariance of input and all candidates

	cxz = cor(ts, dz$kpi)#x[e1] # correlation of input and kpi
	cyz = x[entities] # correlation of candidates and kpi
	sort((sqrt(sxx)*cxz+sqrt(syy)*cyz)/sqrt(sxx+syy + 2*sxy),decreasing=TRUE)
}

rank <- function(N = 20, count = 0, cor = 0){
#	oc_avg=mean(oc)
	options(scipen=999)
	ratio=mean(log(oc[which(oc>0)]),na.rm=T) / mean(x,na.rm=T)
	distance=rep(NA,length(oc))
#	distance=sign(x)*sqrt((scale(is.finite(log(oc))))^2+(scale(x))^2)
	for(i in 1:length(oc)){
		distance[i]=sign(x[i])*sqrt(log(oc[i])^2 + (ratio*x[i])^2)
	}
	names(distance)=names(oc)
	ranked=cbind(oc,sd,n,x)
	ranked=as.data.frame(ranked)
	colnames(ranked)=c("ratio","std","count","correlation")
	ranked=ranked[order(ranked$correlation,decreasing=T),]
	ranked$entities=rownames(ranked)
	ranked=ranked[,c("count","correlation")]	
	ranked[1:50,]
	count=2000
	ranked=ranked[which(ranked$oc>=count),]
	ranked[1:50,]
	ranked=ranked[which(ranked$oc>=count & ranked$x>=cor),]
	ranked[1:N,]
}
explore <- function(selected = NA){
	try(dev.off(), silent = TRUE)
	plot(oc, x, log = "x", cex = .2, col = "red")
	grid()
	if(is.na(selected)){
		idx = identify(oc,x,labels=entities,n=1)
		selected = entities[idx]
		rgx = gsub("\\.","|",selected)
	} else rgx = selected
		
	also = grep(selected, entities)

	points(oc[also], x[also], cex = .5, col = "blue",pch=19)
	dev.new()
	model(rgx)
#	print(paste("Selected",selected))
#	entities[also]
}

model <- function(rgx, min.date = as.Date("2010-01-01")){
#	feat = cbind(dz[,c("kpi","DateMonthYear")], data.frame("qpi"=rowSums(dz[,grep(rgx, colnames(dz))])))
	feat = cbind(dz[,c("kpi","DateMonthYear")], data.frame("qpi"=dz[,which(colnames(dz) == rgx)]))
	feat$DateMonthYear=as.Date(feat$DateMonthYear)
	feat=feat[order(feat$DateMonthYear),]
	#feat = cbind(dz[,c("kpi","DateMonthYear")], data.frame("qpi"=dz[,rgx]))
#	feat = feat[feat$DateMonthYear > min.date, ]

	mod = lm(kpi ~ qpi, data = feat)
	m25 = rq(kpi ~ qpi, data = feat, tau = .25)
	m75 = rq(kpi ~ qpi, data = feat, tau = .75)
	with(feat, plot(DateMonthYear, scale(kpi), type = "b", main = rgx,ylim=c(-3,4)))
	grid()
	#with(feat, points(DateMonthYear, predict(mod, feat), type = "l", col = "blue"))
	with(feat, points(DateMonthYear, scale(qpi), type = "l", col = "blue"))
	#with(feat, points(DateMonthYear, predict(m25, feat), type = "l", col = "red"))
	#with(feat, points(DateMonthYear, predict(m75, feat), type = "l", col = "red"))
	grid()
	summary(mod)
	cor(feat$kpi,feat$qpi)
}

#FIXME has weird closer issue with dT and T0, make this standalone with kpi.file input
credibility <- function(intent.csv){
	# postgres export such as:
	#\copy (select count(distinct(user <- id)),day from (select date <- trunc('day',utc) as day, user <- id from skincare <- tweets where array[514] && ids and is <- consumer <- tweet and txt ~* 'sweat|bought|actually|like lynx|(it|lynx) smell|good' and usstate = 'UK') FOOB group by day order by day) to '/home/dev/berglund/lynx_intent.csv' csv; 

	tmp = read.table(intent.csv, sep = ",")
	tmp$DateMonthYear = d0 + dT * (as.numeric(as.Date(tmp[[2]]) - d0 + dT - 1) %/% dT)

	cred = aggregate(. ~ DateMonthYear, tmp[,c(1,3)], sum)
	names(cred)[[2]] = "intent" 
	dx = merge(cred, kpi[kpi$DateMonthYear >= min.date,])
	mod = lm(kpi ~ intent, data = dx)
	print(summary(mod))

	plot(dz$DateMonthYear, scale(dz$kpi), type = "b", col = "red");grid()
	points(cred$DateMonthYear, scale(cred$intent), type = "b") 

	invisible(dx)
}

}

#### run a model with auto-regression term (same week last year) and top 9 features

# auto-regressive only
model0=lm(dz$original_kpi~dz$last_year_kpi)
summary(model0)
plot(dz$DateMonthYear,dz$original_kpi,type="l",lwd=2,ylim=c(10000000,14000000),main="auto-regressive only")
lines(dz$DateMonthYear,predict(model0),col="red",lwd=2)
legend("bottomright",'groups',c("original KPI","predicted KPI"),lty=c(1,1),col=c("black","red"),bty ="n",lwd=2,border=NA)

# auto-regressive, residuals, social
dz$residuals=dz$original_kpi-dz$last_year_kpi
dz$social=rowSums(dz[,c(rownames(ranked[1:9,]))])
model=lm(dz$residuals~dz$social)
summary(model)
plot(dz$DateMonthYear,dz$residuals,type="l",lwd=2,main="residuals only")
lines(dz$DateMonthYear,predict(model),col="red",lwd=2)
legend("bottomright",'groups',c("original KPI","predicted KPI"),lty=c(1,1),col=c("black","red"),bty ="n",lwd=2,border=NA)

model=lm(dz$original_kpi~dz$last_year_kpi+dz$social)
summary(model)
plot(dz$DateMonthYear,dz$original_kpi,type="l",lwd=2,ylim=c(10000000,14000000),main="auto-regressive + social")
lines(dz$DateMonthYear,predict(model),col="red",lwd=2)
legend("bottomright",'groups',c("original KPI","predicted KPI"),lty=c(1,1),col=c("black","red"),bty ="n",lwd=2,border=NA)

# social only
dz$social=rowSums(dz[,c(rownames(ranked[c(1:17,19:20),]))])
model=lm(dz$original_kpi~dz$social)
summary(model)
plot(dz$DateMonthYear,dz$original_kpi,type="l",lwd=2,ylim=c(10000000,14000000),main="social only")
lines(dz$DateMonthYear,predict(model),col="red",lwd=2)
legend("bottomright",'groups',c("original KPI","predicted KPI"),lty=c(1,1),col=c("black","red"),bty ="n",lwd=2,border=NA)

# autoregressive, price, social
dz=merge(dz,data[,c("end_date","heineken")],by.x="DateMonthYear",by.y="end_date")
dz$heineken.y=as.numeric(dz$heineken.y)
colnames(dz)[5005]="price"
model=lm(dz$original_kpi~dz$last_year_kpi+dz$price)
summary(model)
plot(dz$DateMonthYear,dz$original_kpi,type="l",lwd=2,main="auto-regressive + base price")
lines(dz$DateMonthYear,predict(model),col="red",lwd=2)
legend("bottomright",'groups',c("original KPI","predicted KPI"),lty=c(1,1),col=c("black","red"),bty ="n",lwd=2,border=NA)



### activation model forecast, with seasonal componets

data=read.csv("financial.csv")
data$start_date=as.Date(data$start_date/86400000,origin="1970-01-01")
data$end_date=as.Date(data$end_date/86400000,origin="1970-01-01")
data=data[which(data$start_date > as.Date('2013-01-01')),]
freq=52
reps=floor(nrow(data)/52)
data$season=c(rep(data$decomposed_kpi[1:52],reps),data$decomposed_kpi[1:(nrow(data)-reps*52)])
data$predicted_kpi=data$estimated_kpi+data$season
data=data[order(data$end_date),]

data$original_kpi[which(data$start_date>as.Date("2015-04-01"))]=0

plot(data$end_date,data$original_kpi,ylim=c(0,max(data$predicted_kpi,data$original_kpi,na.rm=TRUE)),main="Activations",ylab="Activations",xlab="Date",type="l")
lines(data$end_date,data$predicted_kpi,col="red")
legend("bottomright",'groups',c("original KPI","predicted KPI"),lty=c(1,1),col=c("black","red"),bty ="n",lwd=2,border=NA)

data$year=year(data$start_date)	
data$month=month(data$start_date)
data.original=aggregate(original_kpi~year+month,data,sum)
data.predicted=aggregate(predicted_kpi~year+month,data,sum)
data.original$start_date=as.Date(paste(data.original$year,data.original$month,"01",sep="-"))
data.predicted$start_date=as.Date(paste(data.predicted$year,data.predicted$month,"01",sep="-"))
data0=merge(data.original,data.predicted,by="start_date")
data0$end_date=data0$start_date%m+% months(1)
data0$original_kpi[which(data0$start_date>=as.Date("2015-04-01"))]=NA
plot(data0$end_date,data0$original_kpi,ylim=c(0,max(data0$predicted_kpi,data0$original_kpi,na.rm=TRUE)),main="Activations",ylab="Activations",xlab="Date",type="l")
lines(data0$end_date,data0$predicted_kpi,col="red")
legend("bottomright",'groups',c("original KPI","predicted KPI"),lty=c(1,1),col=c("black","red"),bty ="n",lwd=2,border=NA)
