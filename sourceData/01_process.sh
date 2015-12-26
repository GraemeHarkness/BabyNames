for file in babies-first-names*.csv
do
	year=`echo $file | cut -d- -f4 | cut -d. -f1`
	cat $file | sed 's/\=//g' | grep --invert ",,,,,," | grep --invert "©" | grep -E "(^[1-9]|^,,,,)" > /tmp/tmpfile.csv
    IFS=,
	while read boyRank boyName boyNumBabies blank girlRank girlName girlNumBabies
	do
	    if [ "$boyRank" != "" ]
	    then
	        echo BOY,$year","$boyRank","$boyName","$boyNumBabies
	    fi
	    if [ "$girlRank" != "" ]
	    then
            echo GIRL,$year","$girlRank","$girlName","$girlNumBabies
	    fi
	done < /tmp/tmpfile.csv
done
