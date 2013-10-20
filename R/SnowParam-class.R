setOldClass(c("SOCKcluster", "cluster"))

.SnowParam <-
    setRefClass("SnowParam",
    contains="BiocParallelParam",
    fields=list(
      .clusterargs="list",
      cluster="SOCKcluster"),
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

.nullCluster <-
    function(type)
{
    if (type == "FORK" || type == "SOCK")
        type <- "PSOCK"
    makeCluster(0L, type)
}

SnowParam <-
    function(workers=0L, type, catch.errors=TRUE, ...)
{
    if (missing(type))
        type <- parallel:::getClusterOption("type")
    args <- c(list(spec=workers, type=type), list(...))
    # FIXME I don't think this is required, lists always inflict a copy
    .clusterargs <- lapply(args, force)
    cluster <- .nullCluster(type)
    .SnowParam(.clusterargs=.clusterargs, cluster=cluster,
        .controlled=TRUE, workers=workers,
        catch.errors=catch.errors, ...)
}

setAs("SOCKcluster", "SnowParam",
    function(from)
{
    .clusterargs <- list(spec=length(from),
        type=sub("cluster$", "", class(from)[1L]))
    .SnowParam(.clusterargs=.clusterargs, cluster=from, .controlled=FALSE,
        workers=length(from), catch.errors=TRUE)
})

## control

setMethod(bpworkers, "SnowParam",
    function(x, ...)
{
    if (bpisup(x))
        length(bpbackend(x))
    else
        x$.clusterargs$spec # TODO: This can be a non-integer, I think.
})

setMethod(bpstart, "SnowParam",
    function(x, ...)
{
    if (!.controlled(x))
        stop("'bpstart' not available; instance from outside BiocParallel?")
    if (bpisup(x))
        stop("cluster already started")
    bpbackend(x) <- do.call(makeCluster, x$.clusterargs)
    invisible(x)
})

setMethod(bpstop, "SnowParam",
    function(x, ...)
{
    if (!.controlled(x))
        stop("'bpstop' not available; instance from outside BiocParallel?")
    if (!bpisup(x))
        stop("cluster already stopped")
    stopCluster(bpbackend(x))
    bpbackend(x) <- .nullCluster(x$.clusterargs$type)
    invisible(x)
})

setMethod(bpisup, "SnowParam",
    function(x, ...)
{
    length(bpbackend(x)) != 0L
})

setMethod(bpbackend, "SnowParam",
    function(x, ...)
{
    x$cluster
})

setReplaceMethod("bpbackend", c("SnowParam", "SOCKcluster"),
    function(x, ..., value)
{
    x$cluster <- value
    x
})

## evaluation

setMethod(bpmapply, c("ANY", "SnowParam"),
    function(FUN, ..., MoreArgs=NULL, SIMPLIFY=TRUE, USE.NAMES=TRUE,
        resume=getOption("BiocParallel.resume", FALSE), BPPARAM)
{
    FUN <- match.fun(FUN)
    # recall on subset
    if (resume) {
        results <- .resume(FUN=FUN, ..., MoreArgs=MoreArgs, SIMPLIFY=SIMPLIFY,
            USE.NAMES=USE.NAMES, BPPARAM=BPPARAM)
        return(results)
    }

    if (!bpisup(BPPARAM)) {
        # FIXME we should simply fall back to serial
        BPPARAM <- bpstart(BPPARAM)
        on.exit(bpstop(BPPARAM))
    }

    # FIXME we should maybe always wrap in a try?
    if (BPPARAM$catch.errors)
        FUN <- .composeTry(FUN)
    results <- clusterMap(cl=bpbackend(BPPARAM), fun=FUN, ...,
        MoreArgs=MoreArgs, SIMPLIFY=FALSE, USE.NAMES=USE.NAMES, RECYCLE=TRUE)
    is.error <- vapply(results, inherits, logical(1L), what="remote-error")
    if (any(is.error))
        LastError$store(results=results, is.error=is.error, throw.error=TRUE)

    .simplify(results, SIMPLIFY=SIMPLIFY)
})
