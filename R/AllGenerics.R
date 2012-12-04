setGeneric("bplapply",
    function(X, FUN, ..., param) standardGeneric("bplapply"))

setGeneric("bpvec",
    function(X, FUN, ..., param) standardGeneric("bpvec"))

setGeneric("bpvectorize",
    function(FUN, ..., param) standardGeneric("bpvectorize"))

setGeneric("bpworkers",
    function(param, ...) standardGeneric("bpworkers"))

setGeneric("bpstart",
    function(param, ...) standardGeneric("bpstart"))

setGeneric("bpstop",
    function(param, ...) standardGeneric("bpstop"))

setGeneric("bpisup",
    function(param, ...) standardGeneric("bpisup"))

setGeneric("bpbackend",
    function(param, ...) standardGeneric("bpbackend"))

setGeneric("bpbackend<-",
    function(param, ..., value) standardGeneric("bpbackend<-"))

## scheduling

setGeneric("bpschedule",
    function(param, ...) standardGeneric("bpschedule"))
