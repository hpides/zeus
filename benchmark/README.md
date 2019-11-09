## Benchmarking Tips

### EpsilonGC 
`-XX:+UnlockExperimentalVMOptions -XX:+UseEpsilonGC`

An experimental garbage collector setting that does no garbage collection. Good to increase the reliabiliy of benchmarks.
However, if max. heap space is reached your program crashes.
Therefore inc. the max. memory as much as possible with: `-Xmx8G`


### JIT
`-Xcomp`

Every class is compiled by the JIT. This should stop sudden performance changes because of the JIT
