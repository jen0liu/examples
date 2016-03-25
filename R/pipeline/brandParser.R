
###############################################################################

brand.parser <- function(brand,file.lists){

if(brand=="example"){
    df = as.data.frame(read.xls(file.lists[1], sheet = 2, blank.lines.skip = TRUE, header = TRUE, as.is = T, perl = gdata:::findPerl("perl")))
    p = dim(df)[1]
    n = dim(df)[2]
    week = df[1,3:(n-1)]
    week = substring(week, 15)
    week = as.Date(parse_date_time(week, "%b %d, %Y"))
    data_line = which(df[,1]=="Dollar Sales"|df[,1] == "Unit Sales")
    df = df[c(data_line[1]:(data_line[3]-3), data_line[3]:(p-2)),-n]
    colnames(df)[1] = "measure"
    colnames(df)[2] = "brand"
    df$measure = rep(c("Dollar Sales", "Unit Sales", "Dollar Sales", "Unit Sales"), each = data_line[2] - data_line[1])
    data = reshape(df, idvar = c("measure", "brand"), varying = list(3:(n-1)), v.names = "originalkpi", direction = "long")
    data$originalkpi = as.numeric(gsub('\\$|,', '', data$originalkpi))
    data$week = rep(week, each = dim(df)[1])
    row.names(data) = NULL
    data = data[c("brand","measure", "originalkpi", "week")]
  }
  data
}



