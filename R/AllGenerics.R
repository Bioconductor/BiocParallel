setGeneric("bplapply", signature=c("X", "BPPARAM"),
    function(X, FUN, ..., BPRESUME=getOption("BiocParallel.BPRESUME", FALSE),
        BPTRACE=TRUE, BPPARAM=bpparam())
    standardGeneric("bplapply"))

setGeneric("bpmapply", signature=c("FUN", "BPPARAM"),
    function(FUN, ..., MoreArgs=NULL, SIMPLIFY=TRUE, USE.NAMES=TRUE,
        BPRESUME=getOption("BiocParallel.BPRESUME", FALSE),
        BPTRACE=TRUE, BPPARAM=bpparam())
    standardGeneric("bpmapply"))

setGeneric("bpiterate", signature=c("ITER", "FUN", "BPPARAM"),
    function(ITER, FUN, ..., BPPARAM=bpparam())
    standardGeneric("bpiterate"))

setGeneric("bpvec", signature=c("X", "BPPARAM"),
    function(X, FUN, ..., AGGREGATE=c, BPPARAM=bpparam())
    standardGeneric("bpvec"))

setGeneric("bpvectorize",
    function(FUN, ..., BPPARAM=bpparam())
    standardGeneric("bpvectorize"))

setGeneric("bpaggregate",
    function(x, ..., BPPARAM=bpparam())
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
