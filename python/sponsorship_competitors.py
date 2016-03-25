from json import dumps, loads, JSONEncoder, JSONDecoder
from pprint import pprint
import json
import csv
import numpy as np
import urllib2, httplib
import os
from StringIO import StringIO
import gzip
import urllib as ul

################################################### Part 1: sponsorship impact

os.chdir('')
vertical=""
id=""
filter=""
favs = "[]"
exprs = "[]"
ctr="competitors"
cohort = False
id_comp=[""]
filter_comp=[""]

target_start_date = "2014-11-01"
target_end_date = "2015-02-08"

cohort = True
ctr="cohort"
cohort_start_date = "2015-02-01"
cohort_end_date = "2015-02-08"
target_start_date = "2015-02-08"
target_end_date = "2015-03-15"
control_cohort_start_date = "2014-09-15"
control_cohort_end_date = "2014-09-22"
control_target_start_date = "2014-09-22"
control_target_end_date = "2014-10-31"

####### constructing URLS

project=id[0:(len(id)-6)]

## filter = favs
if (len(favs)>2):
    if (len(exprs)>2):
        filter = "&targetFavs="+favs+"&targetExpr="+exprs
        file_name = project+"_"+ctr+"_fav_nonexpr.csv"
        file_name2 = "profile_"+project+"_"+ctr+"_fav_nonexpr.csv"
    else:
        filter = "&targetFavs="+favs
        file_name = project+"_"+ctr+"_nonfav.csv"
        file_name2 = "profile_"+project+"_"+ctr+"_nonfav.csv"
elif (len(exprs)>2):
    filter = "&targetExpr="+exprs
    file_name = project+"_competitors_expr_21to29.csv"
    file_name2 = "profile_"+project+"_"+ctr+"_expr_21to29.csv"
else:
    filter = "&targetFavs="+favs
    file_name = project+"_"+ctr+".csv"
    file_name2 = "profile_"+project+"_"+ctr+".csv"


if (cohort):
    urls = [{"name": "test_fav_filter",
         "url": "http://example.com/timeseries?targetVertical="+vertical+"&targetids="+id+"&targetFavs="+favs+"&targetEntities="+ul.quote(filter_id)+
                "&targetStartDate="+target_start_date+"&targetEndDate="+target_end_date+"&targetPrefilterVertical="+vertical+"&targetPrefilterids="+id+"&targetPrefilterStartDate="+cohort_start_date+"&targetPrefilterEndDate="+cohort_end_date},
        {"name": "test_filter",
         "url": "http://example.com/timeseries?targetVertical="+vertical+"&targetids="+id+"&targetEntities="+ul.quote(filter_id)+
                "&targetStartDate="+control_target_start_date+"&targetEndDate="+control_target_end_date+"&targetPrefilterVertical="+vertical+"&targetPrefilterids="+id+"&targetPrefilterStartDate="+control_cohort_start_date+"&targetPrefilterEndDate="+control_cohort_end_date}
        ]
    for i in range(0, len(id_comp)):
        urls.extend ([{"name": "control_fav_filter",
         "url": "http://example.com/timeseries?targetVertical="+vertical+"&targetids="+id_comp[i]+"&targetFavs="+favs+"&targetEntities="+ul.quote(filter_id_comp[i])+
                "&targetStartDate="+target_start_date+"&targetEndDate="+target_end_date+"&targetPrefilterVertical="+vertical+"&targetPrefilterids="+id_comp[i]+"&targetPrefilterStartDate="+cohort_start_date+"&targetPrefilterEndDate="+cohort_end_date},
        {"name": "control_filter",
         "url": "http://example.com/timeseries?targetVertical="+vertical+"&targetids="+id_comp[i]+"&targetEntities="+ul.quote(filter_id_comp[i])+
                "&targetStartDate="+control_target_start_date+"&targetEndDate="+control_target_end_date+"&targetPrefilterVertical="+vertical+"&targetPrefilterids="+id_comp[i]+"&targetPrefilterStartDate="+control_cohort_start_date+"&targetPrefilterEndDate="+control_cohort_end_date}
    ])
else:
    urls = [{"name": "test_fav_filter",
         "url": "http://example.com/timeseries?targetVertical="+vertical+"&targetids="+id+filter+"&targetEntities="+ul.quote(filter_id)+"&targetStartDate=2011-01-01&targetEndDate=2015-12-31"},
        {"name": "test_filter",
         "url": "http://example.com/timeseries?targetVertical="+vertical+"&targetids="+id+"&targetEntities="+ul.quote(filter_id)+"&targetStartDate=2011-01-01&targetEndDate=2015-12-31"},
        ]
    for i in range(0, len(id_comp)):
        urls.extend ([{"name": "control_fav_filter",
         "url": "http://example.com/timeseries?targetVertical="+vertical+"&targetids="+id_comp[i]+filter+"&targetEntities="+ul.quote(filter_id_comp[i])+"&targetStartDate=2011-01-01&targetEndDate=2015-12-31"},
        {"name": "control_filter",
         "url": "http://example.com/timeseries?targetVertical="+vertical+"&targetids="+id_comp[i]+"&targetEntities="+ul.quote(filter_id_comp[i])+"&targetStartDate=2011-01-01&targetEndDate=2015-12-31"}
    ])

################################################### Part 2: compare profile changes


id_comps='('
filter_id_comps='('
for i in range(0, len(id_comp)):
    if i < len(id_comp)-1 :
        id_comps= id_comps + id_comp[i] + ','
        filter_id_comps= filter_id_comps + filter_id_comp[i] + ','
    else:
        id_comps= id_comps + id_comp[i]
        filter_id_comps= filter_id_comps + filter_id_comp[i]

id_comps=id_comps+')'
filter_id_comps=filter_id_comps+')'

url = "http://example.com/compare?targetVertical="+vertical+"&targetids="+id+"&targetEntities="+ul.quote(filter_id)+"&targetStartDate="+target_start_date+"&targetEndDate="+target_end_date \
      +"&refVertical="+vertical+"&refids="+id_comps+"&refEntities="+ul.quote(filter_id_comps)+"&refStartDate="+target_start_date+"&refEndDate="+target_end_date

# all
counts = []

request = urllib2.Request(url)
request.add_header('Accept-encoding', 'gzip')
response = urllib2.urlopen(request)
if response.info().get('Content-Encoding') == 'gzip':
    buf = StringIO(response.read())
    f = gzip.GzipFile(fileobj=buf)
    data = f.read()
    data = json.loads(data)

data0 = data["favs"]
for i in range(0, len(data0)):
    try:
        entity = data0[i]['item']
        targetCount = data0[i]['count']
        targetRate = data0[i]['rawRate']
        refCount = data0[i]['refCount']
        refRate = float(data0[i]['rawRate']) - float(data0[i]['rawScore'])
        diff_ratio = targetRate / refRate
        diff_abs = targetRate - refRate
        threshold = (data0[i]['scoreConfInterval'][1] - data0[i]['scoreConfInterval'][0]) / 2
        counts.append(
            [filter,'favs',entity, targetCount, targetRate, refCount, refRate, diff_ratio, diff_abs,
             threshold])
    except:
        continue

data0 = data["demos"]
for i in range(0, len(data0)):
    try:
        entity = data0[i]['item']
        targetCount = data0[i]['count']
        targetRate = data0[i]['rawRate']
        refCount = data0[i]['refCount']
        refRate = float(data0[i]['rawRate']) - float(data0[i]['rawScore'])
        diff_ratio = targetRate / refRate
        diff_abs = targetRate - refRate
        threshold = (data0[i]['scoreConfInterval'][1] - data0[i]['scoreConfInterval'][0]) / 2
        counts.append(
            [filter,'demos',entity, targetCount, targetRate, refCount, refRate, diff_ratio, diff_abs,
             threshold])
    except:
        continue

data0 = data["geos"]['scores']
for i in range(0, len(data0)):
    try:
        entity = data0[i]['item']
        targetCount = data0[i]['count']
        targetRate = data0[i]['rawRate']
        refCount = data0[i]['refCount']
        refRate = float(data0[i]['rawRate']) - float(data0[i]['rawScore'])
        diff_ratio = targetRate / refRate
        diff_abs = targetRate - refRate
        threshold = (data0[i]['scoreConfInterval'][1] - data0[i]['scoreConfInterval'][0]) / 2
        counts.append(
            [filter,'geos',entity, targetCount, targetRate, refCount, refRate, diff_ratio, diff_abs,
             threshold])
    except:
        continue

header = ["filter","type","entities", "targetCount", "targetRate", "refCount", "refRate", "diff_ratio", "diff_abs", "threshold"]

with open(file_name2, "wb") as f:
    writer = csv.writer(f)
    writer.writerow(header)
    for m in range(0, len(counts)):
        try:
            writer.writerow(counts[m])
        except:
            continue

######## generate csv files
records = []
for i in range(0, len(urls),1):
    data = json.loads(urllib2.urlopen(urls[i]['url']).read())
    data = data[0]['groupTs'][0]['ts']
    for j in range(1, len(data)):
        try:
            date = data[j]['date']
            count = data[j]['count']
        except:
            continue
        records.append([urls[i]['name'], date, count])


with open(file_name, "wb") as f:
    writer = csv.writer(f)
    writer.writerow(["condition", "date", "count"])
    for i in range(0, len(records)):
        try:
            writer.writerow(records[i])
        except:
            continue

