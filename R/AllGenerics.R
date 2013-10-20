setGeneric("bplapply", signature=c("X", "BPPARAM"),
    function(X, FUN, ..., resume=getOption("BiocParallel.resume", FALSE),
             BPPARAM)
{
      standardGeneric("bplapply")
})

setGeneric("bpmapply", signature=c("FUN", "BPPARAM"),
    function(FUN, ..., MoreArgs=NULL, SIMPLIFY=TRUE, USE.NAMES=TRUE,
        resume=getOption("BiocParallel.resume", FALSE), BPPARAM)
{
    standardGeneric("bpmapply")
})

setGeneric("bpvec", signature=c("X", "BPPARAM"),
    function(X, FUN, ..., AGGREGATE=c, BPPARAM) standardGeneric("bpvec"))

setGeneric("bpvectorize",
    function(FUN, ..., BPPARAM) standardGeneric("bpvectorize"))

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
