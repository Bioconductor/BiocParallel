.optionRegistry <- setRefClass(".BiocParallelOptionsRegistry",
    fields=list(
        options = "list"),
    methods=list(
        register = function(optionName, genericName) {
            if (!is.null(.self$options[[optionName]]))
                message("Replacing the function `",
                        optionName,
                        "` from the option registry")
            .self$options[[optionName]] <- genericName
            invisible(registered())
        },
        registered = function() {
            .self$options
        })
)$new()  # Singleton

## Functions to register the S4generic for BPPARAM
.registeredOptions <-
    function()
{
    .optionRegistry$registered()
}

.registerOption <-
    function(optionName, genericName)
{
    getter <- getGeneric(genericName)
    setter <- getGeneric(paste0(genericName, "<-"))
    if (is.null(getter))
        stop("The S4 function '", genericName, "' is not found")
    if (is.null(setter))
        stop("The S4 replacement function '", genericName, "' is not found")
    .optionRegistry$register(optionName, genericName)
}

.registerOption("workers", "bpworkers")
.registerOption("tasks", "bptasks")
.registerOption("jobname", "bpjobname")
.registerOption("log", "bplog")
.registerOption("logdir", "bplogdir")
.registerOption("threshold", "bpthreshold")
.registerOption("resultdir", "bpresultdir")
.registerOption("stop.on.error", "bpstopOnError")
.registerOption("timeout", "bptimeout")
.registerOption("exportglobals", "bpexportglobals")
.registerOption("exportvariables", "bpexportvariables")
.registerOption("progressbar", "bpprogressbar")
.registerOption("RNGseed", "bpRNGseed")
.registerOption("force.GC", "bpforceGC")
.registerOption("fallback", "bpfallback")

## functions for changing the paramters in BPPARAM
.bpparamOptions <-
    function(BPPARAM, optionNames)
{
    registeredOptions <- .registeredOptions()
    ## find the common parameters both BPPARAM and BPOPTIONS
    paramOptions <- intersect(names(registeredOptions), optionNames)
    getterNames <- unlist(registeredOptions[paramOptions])
    setNames(lapply(
        getterNames,
        do.call,
        args = list(BPPARAM)
    ), paramOptions)
}

## value: BPOPTIONS
`.bpparamOptions<-` <-
    function(BPPARAM, value)
{
    BPOPTIONS <- value
    registeredOptions <- .registeredOptions()
    optionNames <- names(BPOPTIONS)
    paramOptions <- intersect(names(registeredOptions), optionNames)
    setterNames <- paste0(unlist(registeredOptions[paramOptions]), "<-")
    for (i in seq_along(paramOptions)) {
        paramOption <- paramOptions[i]
        setterName <- setterNames[i]
        do.call(
            setterName,
            args = list(BPPARAM, BPOPTIONS[[paramOption]])
        )
    }
    BPPARAM
}

## Check any possible issues in bpoptions
.validateBpoptions <-
    function(BPOPTIONS)
{
    bpoptionsArgs <- names(formals(bpoptions))
    registeredOptions <- names(.registeredOptions())
    allOptions <- c(bpoptionsArgs, registeredOptions)
    idx <- which(!names(BPOPTIONS) %in% allOptions)
    if (length(idx))
        message(
            "unregistered options found in bpoptions:\n",
            "  ", paste0(names(BPOPTIONS)[idx], collapse = ", ")
        )
}

## The function simply return a list of its arguments
bpoptions <-
    function(
        workers, tasks, jobname,
        log, logdir, threshold,
        resultdir, stop.on.error,
        timeout, exportglobals, exportvariables,
        progressbar,
        RNGseed, force.GC,
        fallback,
        exports, packages,
        ...)
{
        dotsArgs <- list(...)
        passed <- names(as.list(match.call())[-1])
        passed <- setdiff(passed, names(dotsArgs))
        if (length(passed))
            passedArgs <- setNames(mget(passed), passed)
        else
            passedArgs <- NULL
        opts <- c(passedArgs, dotsArgs)
        .validateBpoptions(opts)
        opts
}


