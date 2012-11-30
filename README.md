BiocParallel
============

Bioconductor facilities for parallel evaluation (experimental)

Possible TODO
-------------

+ encapsulate arguments as ParallelParam()

+ Standardize signatures
+ Make functions generics
+ parLapply-like function
+ map/reduce-like function
 
+ Abstract scheduler

TOOD
----

+ MulticoreParam on Windows -- at least fall back gracefully to single-core
+ SnowParam support for setSeed, recursive, cleanup
+ subset SnowParam
+ elaborate SnowParam for SnowSocketParam, SnowForkParam, SnowMpiParam, ...
