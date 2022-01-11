#! /bin/bash 
. /home/akash/Desktop/DataGatherStock/DataInj.config
index=0;
echo "No. of companies in the list are:";
wc -l $FILE_SOURCE
 # just to know the no. of companies in the list
while IFS= read -r value; do
	index=$(( index +1))   # counter for keeping track of which company data is being gathered
	echo "gathering $index . $value"
	python zdata.py -i "NSE:$value" -f $F_DATE -t $T_DATE  -n $D_INTERVAL -o data/$value.csv 	# creates the data file in the pwd with the name of the company in  csv format
	sleep 4   # sleep command just to avoid blacklisting of my ip address.
done < $FILE_SOURCE  #file name of list of symbol companies.
