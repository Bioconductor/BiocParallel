BiocParallel
============

Bioconductor facilities for parallel evaluation (experimental)

Possible TODO
-------------

+ map/reduce-like function
+ bpforeach?
+ Abstract scheduler
+ lazy DoparParam
+ SnowParam support for setSeed, recursive, cleanup
+ subset SnowParam
+ elaborate SnowParam for SnowSocketParam, SnowForkParam, SnowMpiParam, ...
+ MulticoreParam on Windows
+ Short vignette

DONE
----

+ encapsulate arguments as ParallelParam()
+ Standardize signatures
+ Make functions generics
+ parLapply-like function
