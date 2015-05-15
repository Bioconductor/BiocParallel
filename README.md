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

DONE
----

+ encapsulate arguments as ParallelParam()
+ Standardize signatures
+ Make functions generics
+ parLapply-like function
+ Short vignette
+ elaborate SnowParam for SnowSocketParam, SnowForkParam, SnowMpiParam, ...
+ MulticoreParam on Windows

github notes
------------

+ commit one-liners with names

    git log --pretty=format:"- %h %an: %s"

TO FIX
-------------

+ DoparParam does not pass foreach args 
  (specifically access to .options.nws for chunking)
