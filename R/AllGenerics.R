setGeneric("bplapply", signature=c("X", "BPPARAM"),
    function(X, FUN, ..., BPRESUME=getOption("BiocParallel.BPRESUME", FALSE),
             BPPARAM)
      standardGeneric("bplapply"))

setGeneric("bpmapply", signature=c("FUN", "BPPARAM"),
    function(FUN, ..., MoreArgs=NULL, SIMPLIFY=TRUE, USE.NAMES=TRUE,
        BPRESUME=getOption("BiocParallel.BPRESUME", FALSE), BPPARAM)
    standardGeneric("bpmapply"))

setGeneric("bpvec", signature=c("X", "BPPARAM"),
    function(X, FUN, ..., AGGREGATE=c, BPPARAM) standardGeneric("bpvec"))

setGeneric("bpvectorize",
    function(FUN, ..., BPPARAM) standardGeneric("bpvectorize"))

setGeneric("bpaggregate",
    function(x, ..., BPPARAM)
    standardGeneric("bpaggregate"))

setGeneric("bpworkers",
    function(x, ...) standardGeneric("bpworkers"))

setGeneric("bpstart",
    function(x, ...) standardGeneric("bpstart"))

setGeneric("bpstop",
    function(x, ...) standardGeneric("bpstop"))

setGeneric("bpisup",
    function(x, ...) standardGeneric("bpisup"))

setGeneric("bpbackend",
    function(x, ...) standardGeneric("bpbackend"))

setGeneric("bpbackend<-",
    function(x, ..., value) standardGeneric("bpbackend<-"))

## scheduling

setGeneric("bpschedule",
    function(x, ...) standardGeneric("bpschedule"))
