 #!/bin/sh
for i in "compiled_ajoin" "compiled_join" "flink_join" "vulcano_ajoin" "vulcano_join"
do
	{ sh runBenchmark.sh $i 5 > ../output/$i.log; }
done

