setGeneric("bplapply", signature=c("X", "BPPARAM"),
    function(X, FUN, ..., BPREDO=list(), BPPARAM=bpparam())
    standardGeneric("bplapply"))

setGeneric("bpmapply", signature=c("FUN", "BPPARAM"),
    function(FUN, ..., MoreArgs=NULL, SIMPLIFY=TRUE, USE.NAMES=TRUE,
             BPREDO=list(), BPPARAM=bpparam())
    standardGeneric("bpmapply"))

setGeneric("bpiterate", signature=c("ITER", "FUN", "BPPARAM"),
    function(ITER, FUN, ..., BPPARAM=bpparam())
    standardGeneric("bpiterate"))

setGeneric("bpvec", signature=c("X", "BPPARAM"),
    function(X, FUN, ..., AGGREGATE=c, BPREDO=list(), BPPARAM=bpparam())
    standardGeneric("bpvec"))

setGeneric("bpvectorize",
    function(FUN, ..., BPREDO=list(), BPPARAM=bpparam())
    standardGeneric("bpvectorize"))

setGeneric("bpaggregate", signature=c("x", "BPPARAM"),
    function(x, ..., BPREDO=list(), BPPARAM=bpparam())
    standardGeneric("bpaggregate"))

setGeneric("bpworkers",
    function(x, ...) standardGeneric("bpworkers"))

setGeneric("bpworkers<-",
    function(x, ..., value) standardGeneric("bpworkers<-"))

setGeneric("bptasks",
    function(x, ...) standardGeneric("bptasks"))

setGeneric("bptasks<-",
    function(x, ..., value) standardGeneric("bptasks<-"))

## errors
setGeneric("bpcatchErrors",
    function(x, ...) standardGeneric("bpcatchErrors"))

setGeneric("bpcatchErrors<-",
    function(x, ..., value) standardGeneric("bpcatchErrors<-"))

setGeneric("bpstopOnError",
    function(x, ...) standardGeneric("bpstopOnError"))

setGeneric("bpstopOnError<-",
    function(x, ..., value) standardGeneric("bpstopOnError<-"))

setGeneric("bpprogressbar",
    function(x, ...) standardGeneric("bpprogressbar"))

setGeneric("bpprogressbar<-",
    function(x, ..., value) standardGeneric("bpprogressbar<-"))

setGeneric("bpRNGseed",
    function(x, ...) standardGeneric("bpRNGseed"))

setGeneric("bpRNGseed<-",
    function(x, ..., value) standardGeneric("bpRNGseed<-"))

setGeneric("bplog",
    function(x, ...) standardGeneric("bplog"))

setGeneric("bplog<-",
    function(x, ..., value) standardGeneric("bplog<-"))

setGeneric("bplogdir",
    function(x, ...) standardGeneric("bplogdir"))

setGeneric("bplogdir<-",
    function(x, ..., value) standardGeneric("bplogdir<-"))

setGeneric("bpthreshold",
    function(x, ...) standardGeneric("bpthreshold"))

setGeneric("bpthreshold<-",
    function(x, ..., value) standardGeneric("bpthreshold<-"))

## results
setGeneric("bpresultdir",
    function(x, ...) standardGeneric("bpresultdir"))

setGeneric("bpresultdir<-",
    function(x, ..., value) standardGeneric("bpresultdir<-"))

## control
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
