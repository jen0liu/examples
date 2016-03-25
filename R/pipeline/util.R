yw.to.date <- function( x, fw = 0 ) {
	y <- as.numeric( substring( x, 1, 4 ) )
	w <- as.numeric( substring( x, 5 ) )
	foy <- ISOdate( y, 1, 1, tz = "" )  
	fy <- as.POSIXlt( foy )$wday
	dayno <- ( fw - fy ) %% 7 + 7 * ( w - 1 )
	as.Date(foy + dayno * 24 * 60 * 60)
}

read.excel.files <- function(match.string,file.lists){
	file.list=file.lists[grepl(match.string,file.lists)]
	for(m in 1:length(file.list)){
		df = as.data.frame(read.xls(file.list[m], sheet = 1, blank.lines.skip = TRUE, header = TRUE, perl = gdata:::findPerl("perl")))
		if( m == 1 )	data = df
		else	data = rbind(data, df)
	}
	data
}



