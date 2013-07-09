setOldClass(c("SOCKcluster", "cluster"))

.SnowParam <-
    setRefClass("SnowParam",
    contains="BiocParallelParam",
    fields=list(
      .clusterargs="list",
      cluster="SOCKcluster",
      catch.errors = "logical"),
    methods=list(
      show = function() {
          callSuper()
          if (bpisup(.self))
              print(bpbackend(.self))
          else {
              cat("cluster 'spec': ", .clusterargs$spec,
                  "; 'type': ", .clusterargs$type, "\n", sep="")
          }
      }))

.nullCluster <- function(type) {
    if (type == "FORK" || type == "SOCK")
        type <- "PSOCK"
    makeCluster(0L, type)
}

SnowParam <-
    function(workers = 0L, type, catch.errors = FALSE, ...)
{
    if (missing(type))
        type <- parallel:::getClusterOption("type")
    args <- c(list(spec=workers, type=type), list(...))
    .clusterargs <- lapply(args, force)
    cluster <- .nullCluster(type)
    .SnowParam(.clusterargs=.clusterargs, cluster=cluster,
               .controlled=TRUE, workers=workers,
               catch.errors = catch.errors, ...)
}

setAs("SOCKcluster", "SnowParam", function(from) {
    .clusterargs <- list(spec=length(from),
                         type=sub("cluster$", "", class(from)[1]))
    .SnowParam(.clusterargs=.clusterargs, cluster=from,
               .controlled=FALSE, workers=length(from))
})

## control

setMethod(bpworkers, "SnowParam", function(x, ...) {
    if (bpisup(x))
        length(bpbackend(x))
    else
        x$.clusterargs$spec # TODO: This can be a non-integer, I think.
})

setMethod(bpstart, "SnowParam", function(x, ...) {
    if (!.controlled(x))
        stop("'bpstart' not available; instance from outside BiocParallel?")
    if (bpisup(x))
        stop("cluster already started")
    bpbackend(x) <- do.call(makeCluster, x$.clusterargs)
    invisible(x)
})

setMethod(bpstop, "SnowParam", function(x, ...) {
    if (!.controlled(x))
        stop("'bpstop' not available; instance from outside BiocParallel?")
    if (!bpisup(x))
        stop("cluster already stopped")
    stopCluster(bpbackend(x))
    bpbackend(x) <- .nullCluster(x$.clusterargs$type)
    invisible(x)
})

setMethod(bpisup, "SnowParam", function(x, ...) {
    length(bpbackend(x)) != 0
})

setMethod(bpbackend, "SnowParam", function(x, ...) {
    x$cluster
})

setReplaceMethod("bpbackend", c("SnowParam", "SOCKcluster"),
    function(x, ..., value) {
    x$cluster <- value
    x
})

## evaluation

setMethod(bplapply, c("ANY", "SnowParam"), function(X, FUN, ..., BPPARAM) {
    FUN <- match.fun(FUN)
    if (!bpisup(BPPARAM)) {
        BPPARAM <- bpstart(BPPARAM)
        on.exit(bpstop(BPPARAM))
    }
    if (isTRUE(BPPARAM$catch.errors)) {
      wrap = function(.FUN, ...) {
        try(do.call(.FUN, list(...)))
      }
      results = parLapply(bpbackend(BPPARAM), X, wrap, .FUN = FUN, ...)
      is.error = vapply(results, inherits, logical(1L), what = "try-error")
      LastError$store(obj = X, results = results, is.error = is.error, throw.error = TRUE)
    } else {
      results = parLapply(bpbackend(BPPARAM), X, FUN, ...)
    }

    return(results)
})
