# Lab work corresponding to study of MIT 6.824 Grad Course -  Distributed Systems

 ## Lab 1 - Map Reduce Implementation

Directory: scr/main

1. Remove previous output.
 ```$ rm mr-out*```

2. Build word count plugin
 ```$ go build -race -buildmode=plugin ../mrapps/wc.go```

3. Run the coordinator / master with pg-* files as input
```$ go run -race mrcoordinator.go pg-*.txt```

4. Run several workers in seperate terminals
```$ go run -race mrworker.go wc.so```

When the workers and coordinator have finished, look at the output in mr-out-*. When you've completed the lab, the sorted union of the output files should match the sequential output, like this:
 ```$ cat mr-out-* | sort | more```
 
 ```
 A 509
 ABOUT 2
 ACT 8
 ...
```

5. Test 20 times
```$ bash test-mr-many.sh 20```
