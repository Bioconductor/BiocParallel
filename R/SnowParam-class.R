setOldClass(c("SOCKcluster", "cluster"))
setOldClass(c("spawnedMPIcluster", "cluster"))

.SnowParam <-
    setRefClass("SnowParam",
    contains="BiocParallelParam",
    fields=list(
      .clusterargs="list",
      cluster="cluster"),
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
    function()
{
    makeCluster(0L, "PSOCK")
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

setAs("cluster", "SnowParam",
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

setReplaceMethod("bpbackend", c("SnowParam", "cluster"),
    function(x, ..., value)
{
    x$cluster <- value
    x
})



## evaluation

setMethod(bpmapply, c("ANY", "SnowParam"),
    function(FUN, ..., MoreArgs=NULL, SIMPLIFY=TRUE, USE.NAMES=TRUE,
        BPRESUME=getOption("BiocParallel.BPRESUME", FALSE), BPPARAM)
{
    FUN <- match.fun(FUN)
    # recall on subset
    if (BPRESUME) {
        return(.bpresume_mapply(FUN=FUN, ..., MoreArgs=MoreArgs,
            SIMPLIFY=SIMPLIFY, USE.NAMES=USE.NAMES, BPPARAM=BPPARAM))
    }

    if (!bpisup(BPPARAM)) {
        BPPARAM <- bpstart(BPPARAM)
        on.exit(bpstop(BPPARAM))
    }

    if (!bpschedule(BPPARAM))
        return(bpmapply(FUN=FUN, ..., MoreArgs=MoreArgs, SIMPLIFY=SIMPLIFY,
            USE.NAMES=USE.NAMES, BPRESUME=BPRESUME,
            BPPARAM=SerialParam(catch.errors=BPPARAM$catch.errors)))

    # clusterMap not consistent with mapply w.r.t. zero-length input
    if (missing(...) || length(..1) == 0L)
      return(list())

    if (BPPARAM$catch.errors)
        FUN <- .composeTry(FUN)
    results <- clusterMap(cl=bpbackend(BPPARAM), fun=FUN, ...,
        MoreArgs=MoreArgs, SIMPLIFY=FALSE, USE.NAMES=USE.NAMES, RECYCLE=TRUE)
    is.error <- vapply(results, inherits, logical(1L), what="remote-error")
    if (any(is.error))
        LastError$store(results=results, is.error=is.error, throw.error=TRUE)

    .simplify(results, SIMPLIFY=SIMPLIFY)
})
