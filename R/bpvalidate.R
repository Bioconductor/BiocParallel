.BPValidate <- setClass(
    "BPValidate",
    slots = c(
        symbol = "character",
        environment = "character",
        unknown = "character"
    )
)

BPValidate <-
    function(symbol = character(), environment = character(),
        unknown = character())
{
    if (is.null(symbol))
        symbol <- character()
    if (is.null(environment))
        environment <- character()
    .BPValidate(symbol = symbol, environment = environment, unknown = unknown)
}

.bpvalidateSymbol <- function(x) x@symbol

.bpvalidateEnvironment <- function(x) x@environment

.bpvalidateUnknown <- function(x) x@unknown

.show_bpvalidateSearch <- function(x)
{
    search <- data.frame(
        symbol = .bpvalidateSymbol(x),
        environment = .bpvalidateEnvironment(x),
        row.names = NULL
    )
    output <- capture.output(search)
    text <- ifelse(NROW(search), paste(output, collapse = "\n  "), "none")
    c("symbol(s) in search() path:\n  ", text)
}

.show_bpvalidateUnknown <- function(x)
{
    unknown <- .bpvalidateUnknown(x)
    text <- ifelse(length(unknown), paste(unknown, collapse = "\n  "), "none")
    c("unknown symbol(s):\n  ", text)
}

setMethod("show", "BPValidate", function(object) {
    cat(
        "class: ", class(object), "\n",
        .show_bpvalidateSearch(object), "\n\n",
        .show_bpvalidateUnknown(object), "\n\n",
        sep = ""
    )
})

#########################
## Utils
#########################
.filterDefaultPackages <-
    function(symbols)
{
    pkgs <- c(
        "stats", "graphics", "grDevices", "utils", "datasets",
        "methods", "Autoloads", "base"
    )
    drop <- unlist(symbols, use.names = FALSE) %in% paste0("package:", pkgs)
    symbols[!drop]
}

## Filter the variables that will be available after `fun` loads
## packages
.filterLibraries <-
    function(codes, symbols, ERROR_FUN)
{
    warn <- err <- NULL
    ## 'fun' body loads libraries
    pkgLoadFunc <- c("require", "library")
    i <- grepl(
        paste0("(", paste0(pkgLoadFunc, collapse = "|"), ")"),
        codes
    )
    xx <- lapply(codes[i], function(code) {
        withCallingHandlers(tryCatch({
            ## convert character code to expression
            expr <- parse(text = code)[[1]]
            ## match the library/require function arguments
            expr <- match.call(eval(expr[[1]]), expr)
            ## get the package name from the function arguments
            pkg <- as.character(expr[[which(names(expr) == "package")]])
            which(symbols %in% getNamespaceExports(pkg))
        }, error=function(e) {
            err <<- append(err, conditionMessage(e))
            NULL
        }), warning=function(w) {
            warn <<- append(warn, conditionMessage(w))
            invokeRestart("muffleWarning")
        })
    })
    if (!is.null(warn) || !is.null(err))
        ERROR_FUN("attempt to load library failed:\n    ",
                  paste(c(warn, err), collapse="\n    "))
    xx <- unlist(xx)
    if (length(xx))
        symbols <- symbols[-xx]
    symbols
}

## find the variables that needed to be exported
.findVariables <-
    function(fun, ERROR_FUN = capture.output)
{
    unknown <- findGlobals(fun)
    env <- environment(fun)
    codes <- deparse(fun)
    ## TODO: The location where the pkg is loaded is not considered here
    ## (should we consider it??)
    ## remove the symbols that will be loaded inside the function
    unknown <- .filterLibraries(codes, unknown, ERROR_FUN)

    ## Find the objects that will ship with the function
    while (length(unknown) &&
           !identical(env, emptyenv()) &&
           !identical(.GlobalEnv, env))
    {
        i <- vapply(unknown, function(x) {
            !exists(x, envir = env, inherits = FALSE)
        }, logical(1))
        ## Force evaluation of the known arguments to
        ## make sure they will be exported correctly
        known <- unknown[-i]
        for (nm in known)
            force(env[[nm]])
        unknown <- unknown[i]
        env <- parent.env(env)
    }

    ## Find the objects that are defined in the search path
    ## (only if the function/expr depends on the global)
    inpath <- list()
    if (length(unknown) && identical(.GlobalEnv, env)) {
        inpath <- lapply(unknown, function(x) {
            where <- find(x)
            ## Includes only packages and variables in the global
            ## environment
            keep <-  startsWith(where, "package:") | where == ".GlobalEnv"
            head(where[keep], 1L)
        })
        names(inpath) <- unknown
        i <- as.logical(lengths(inpath))
        unknown <- unknown[!i]
        inpath <- inpath[i]
        inpath <- .filterDefaultPackages(inpath)
    }

    ## The package required by the worker
    pkgs <- unique(unlist(inpath, use.names = FALSE))

    ## variables defined in the global environment
    globalvars <- names(inpath)[pkgs == ".GlobalEnv"]

    pkgs <- pkgs[pkgs != ".GlobalEnv"]
    pkgs <- gsub("package:", "", pkgs, fixed = TRUE)

    list(
        unknown = unknown,
        pkgs = pkgs,
        globalvars = globalvars,
        inpath = inpath
    )
}

#########################
## validate funtions and vairables that need to be exported
#########################
bpvalidate <- function(fun, signal = c("warning", "error", "silent"))
{
    typeof <- suppressWarnings(typeof(fun))
    if (!typeof %in% c("closure", "builtin"))
        stop("'fun' must be a closure or builtin")

    if (is.function(signal)) {
        ERROR_FUN <- signal
    } else {
        ERROR_FUN <- switch(
            match.arg(signal),
            warning = warning,
            error = stop,
            silent = capture.output
        )
    }

    ## Filter the symbols that is loaded via library/require
    exports <- .findVariables(fun, ERROR_FUN = ERROR_FUN)
    inpath <- exports$inpath

    result <- BPValidate(
        symbol = names(inpath),
        environment = unlist(inpath, use.names = FALSE),
        unknown = exports$unknown
    )

    ## error report
    msg <- character()
    test <- .bpvalidateEnvironment(result) %in% ".GlobalEnv"
    if (any(test))
        msg <- c(
            msg, "symbol(s) in .GlobalEnv:\n  ",
            paste(.bpvalidateSymbol(result)[test], collapse = "\n  "), "\n"
        )
    test <- .bpvalidateUnknown(result)
    if (length(test))
        msg <- c(
            msg, "unknown symbol(s):\n  ", paste(test, collapse = "\n  "), "\n"
        )
    if (length(msg))
        ERROR_FUN("\n", paste(msg, collapse = ""), call. = FALSE)

    result
}



