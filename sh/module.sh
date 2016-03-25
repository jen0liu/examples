vertical=
project=
timeLags=(0 0 0 0 0 0)
idNames=(  )
ids=()
csvs=()
config=example_config

directory=overall
kpiColumn=2


#for i in `seq 1 ${#ids[@]}`
#for i in `seq 1 ${#csvs[@]}`
#for i in `seq 1 5`
for i in 4
do 
	sbt/sbt "project science" "run-main Module
	--host example.com
	--port 3000
	--qpid ${ids[$i-1]}
	--qpidName ${idNames[$i-1]}
	--vertical $vertical
	--startDate 2014-01-01
	--endDate 2015-11-01
	--maxFeatures 60                                                                                                                                                                                                                                                                                                                                                                                                                                 
	--bootstrapIterations 50
	--percentValidation 0.2
	--holdOutSamples 0
	--outputDir /Users/jen/Documents/$project/$directory	
	--scalingType consumer
	--dateEntityFromFile false
	--clusteringThreshold 0.95
	--aboveRandom 0.2
	--timeLag ${timeLags[$i-1]}
	--kpiFile /Users/jen/Documents/$project/data/${csvs[$i-1]}
	--kpiColumn $kpiColumn
	--delimiter ,
	--env dev
	--dateFormat yyyy-MM-dd
	--includeTotalConsumers false
	--preProcess scaling,8
	--corrType correlation
	--filterEmoticons true	
	--entityPenalty true
	--verbose 5

	"
done

#### shell script to process data

2) Make merge file
#run jen's script, check headings to get indices right

ls *zip > filelist
for filename in `cat filelist`
do
unzip $filename > temp
done

ls *tsv > filelist2
for filename in `cat filelist2`
do
sed 's/\t/,/g' $filename > ${filename:0:-3}"csv"
done
