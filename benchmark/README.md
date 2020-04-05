## Benchmarking Usage

### Run a benchmark

The two most important files when wanting to execute a benchmark are the
 `MainNetworkDataGenerator`, which starts the data generator and the
`MainNetworkEngine` which starts the engine and supplies it with queries.

All existing command-line parameters are explained in this readme.

The benchmark can be run locally, or over the network. When running the benchmark on one machine, 
you need to set the `--generatorHost` option to "127.0.0.1". Otherwise, use the IP of the computer which runs the data generator. 

The main part of the configuration is happening on the engine-side. The following steps have to be performed:
1. Decide whether you want to benchmark locally or over the network. Set the `--generatorHost` accordingly
2. Decide for a data type with the `--type` option. You can choose between "basic" and "nexmark". Set this option in both the engine and the data generator.
3. Decide for a time to run the benchmark. Set `--timeInSeconds` for both the engine and the generator
4. Select an operator you want to test. Set `--operator` in the engine
5. According to the operator you choose, determine whether you need one or two sources, set `amountOfSources` in the data generator accordingly
6. Now you are at the point of deciding how your benchmark should look like. You have several parameters to control this, such as `--newQueriesPerSecond`, `--removeQueriesPerSecond`, `--waitBetweenAddDel` and `--fixedQueries`
7. You are all set! You can now run the benchmark. Be sure to start the data generator before the engine!

If you want to execute more than one test, you can also use the Python Scripts in the `python`-package. These allow you to easily specify all the mentioned
parameters and run multiple types of benchmarks after one another. To gain insights into what benchmarks and combination of parameters make sense, please
refer to our Paper.

You can visualize your results by using the `makeGraphs.py` python file, although its use requires more in-depth use of the engine and its inner workings.

### Usage Example 
These example calls demonstrates how you can utilize the benchmark package to run a local single join query with the basic data:

1 - Build the project from the top level project directory

`mvn clean package`

2 - Run the data generator

`java -Xms10g -Xmx10g -jar benchmark/target/generator-jar-with-dependencies.jar -eps=1000000 -tis=305 -t=basic -ams=2`

3 - Run the engine

`java -Xms10g -Xmx10g -jar benchmark/target/engine-jar-with-dependencies.jar -gh=127.0.0.1 -tis=300 -t=basic -nqs=0 -rqs=0 -wbt=0 -op="join" -fq=1`

### Engine Parameters
The shortcuts for these parameters can be found in the `MainNetworkEngine` class.

| Parameter        | Explaination           |
| ------------- |:-------------:|
| --timeInSeconds      | Time in seconds the engine should run |
| --auctionSourcePort      | Nextmark-Benchmark port for the Auction-Stream      |
| --bidSourcePort      | Nextmark-Benchmark port for the Bid-Stream      |
| --basicPort1      | Basic-Benchmark port |
| --basicPort2      | Second Basic Benchmark port     |
| --serializer      | Serializer to use, possible values are "custom" (recomended) and "gson" |
| --generatorHost      | IP of the data generator      |
| --newQueriesPerSecond      | Number of queries to be added each waitBetweenAddDel interval   |
| --removeQueriesPerSecond      | Number of queries to be removed each waitBetweenAddDel interval      |
| --waitBetweenAddDel      | Number of seconds to wait between adding and removing queries      |
| --fixedQueries      | Number of fixed queries to run on their own or beside the added and removed queries      |
| --type      | Type of data you are using, possible values are "basic" and "nexmark"      |
| --operator      | Type of operator you want to test, basic possibilities: "map", "join", "ajoin", nexmark possibilities: "1" (Filter), "2" (Join), "3" (AJoin), "4" (HotCat), "5" (HPPA)      |

### Data Generator Parameters
The shortcuts for these parameters can be found in the `MainNetworkDataGenerator` class.

| Parameter        | Explaination           |
| ------------- |:-------------:|
| --eventsPerSecond      | Maximum Event per second the data generator will generate |
| --timeInSeconds      | Time in seconds the data generator should run |
| --auctionSourcePort      | Nextmark-Benchmark port for the Auction-Stream      |
| --bidSourcePort      | Nextmark-Benchmark port for the Bid-Stream      |
| --basicPort1      | Basic-Benchmark port |
| --basicPort2      | Second Basic Benchmark port     |
| --serializer      | Serializer to use, possible values are "custom" (recomended) and "gson" |
| --type      | Type of data you are using, possible values are "basic" and "nexmark"      |
| --amountOfSources      | Amount of sources you use, use 1 for running map and filter, and 2 for all join-based queries     |

