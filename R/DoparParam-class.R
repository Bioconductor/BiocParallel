### =========================================================================
### DoparParam objects
### -------------------------------------------------------------------------


### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Constructor
###

.DoparParam_prototype <- .BiocParallelParam_prototype

.DoparParam <- setRefClass("DoparParam",
    contains="BiocParallelParam",
    fields=list(),
    methods=list()
)

DoparParam <-
    function(stop.on.error=TRUE, RNGseed = NULL)
{
    if (!requireNamespace("foreach", quietly = TRUE))
        stop("DoparParam() requires the 'foreach' package", call. = FALSE)

    prototype <- .prototype_update(
        .DoparParam_prototype,
        stop.on.error=stop.on.error,
        RNGseed=RNGseed
    )

    x <- do.call(.DoparParam, prototype)

    ## DoparParam is always up, so we need to initialize
    ## the seed stream here
    .bpstart_set_rng_stream(x)

    validObject(x)
    x
}

setMethod("bpworkers", "DoparParam",
    function(x)
{
    if (bpisup(x))
        foreach::getDoParWorkers()
    else 0L
})

setMethod("bpisup", "DoparParam",
    function(x)
{
    isNamespaceLoaded("foreach") &&
        foreach::getDoParRegistered() &&
        (foreach::getDoParName() != "doSEQ")
})

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Manager
###
.DoparParamManager <- setClass("DoparParamManager",
    contains="TaskManager"
)

## constructor
setMethod(
    ".manager", "DoparParam",
    function(BPPARAM)
{
    .DoparParamManager(
        BPPARAM = BPPARAM,
        tasks = new.env(parent = emptyenv())
    )
})

setMethod(
    ".manager_send", "DoparParamManager",
    function(manager, value, ...)
{
    taskId <- length(manager$tasks) + 1L
    if (taskId == 1L)
        manager$const.value <- .task_const(value)
    manager$tasks[[as.character(taskId)]] <- .task_dynamic(value)
})

setMethod(
    ".manager_recv", "DoparParamManager",
    function(manager)
{
    stopifnot(length(manager$tasks) > 0L)
    tasks <- as.list(manager$tasks)
    tasks <- tasks[order(names(tasks))]
    const.value <- manager$const.value
    `%dopar%` <- foreach::`%dopar%`
    foreach <- foreach::foreach
    tryCatch({
        results <-
            foreach(task = tasks)%dopar%{
                task <- .task_remake(task, const.value)
                if (task$type == "EXEC")
                    value <- .bpworker_EXEC(task)
                else
                    value <- NULL
                list(value = value)
            }
    }, error=function(e) {
        stop(
            "'DoparParam()' foreach() error occurred: ",
            conditionMessage(e)
        )
    })
    ## cleanup the tasks
    remove(list = ls(manager$tasks), envir = manager$tasks)
    manager$const.value <- NULL

    results
})

setMethod(
    ".manager_send_all", "DoparParamManager",
    function(manager, value)
{
    nworkers <- bpworkers(manager$BPPARAM)
    for (i in seq_len(nworkers)) {
        .manager_send(manager, value)
    }
})

setMethod(
    ".manager_recv_all", "DoparParamManager",
    function(manager) .manager_recv(manager)
)

setMethod(
    ".manager_capacity", "DoparParamManager",
    function(manager)
{
    .Machine$integer.max
})


### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Methods - evaluation
###

setMethod("bpiterate", c("ANY", "ANY", "DoparParam"),
    function(ITER, FUN, ..., BPREDO = list(),
             BPPARAM=bpparam(), BPOPTIONS=bpoptions())
{
    stop("'bpiterate' not supported for DoparParam")
})
