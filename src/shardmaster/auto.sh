for i in $(seq 1 1000)
do 
	go test -race  >> out
done


