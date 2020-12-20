bpvalidate <- function(fun)
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
    unknown <- codetools::findGlobals(fun)
    f_env <- environment(fun)

    ## 'fun' environment is NAMESPACE
    if (length(unknown) && isNamespace(f_env)) {
        f_ls <- c(
            getNamespaceImports(f_env),
            setNames(list(ls(f_env, all.names=TRUE)), getNamespaceName(f_env))
        )
        f_symbols <- unique(unlist(f_ls, use.names=FALSE))
        unknown <- unknown[!unknown %in% f_symbols]
    }
 
    ## 'fun' body loads libraries
    warn <- err <- NULL
    if (any(c("require", "library") %in% unknown)) {
        xx <- withCallingHandlers(tryCatch({
            dep <- deparse(fun)
            i <- grepl("(library|require)", dep)
            sapply(dep[i], function(elt) {
                pkg <- gsub(")", "", strsplit(elt, "(", fixed=TRUE)[[1]][2])
                unknown %in% getNamespaceExports(pkg)
            })
        }, error=function(e) {
            err <<- append(err, conditionMessage(e))
            NULL
        }), warning=function(w) {
            warn <<- append(warn, conditionMessage(w))
            invokeRestart("muffleWarning")
        })
        if (!is.null(warn) || !is.null(err))
            stop("\nattempt to load library failed:\n    ",
                 paste(c(warn, err), collapse="\n    "))
        if (!is.matrix(xx))
            xx <- t(as.matrix(xx))
        unknown <- unknown[rowSums(xx) == 0L]
    }

    ## look in search path
    inpath <- structure(list(), names=character())
    if (length(unknown)) {
        ## exclude variables found in defining environment(s)
        env <- environment(fun)
        while (!identical(env, topenv(environment(fun)))) {
            inlocal <- ls(env, all.names = TRUE)
            unknown <- setdiff(unknown, inlocal)
            env <- parent.env(env)
        }

        inpath <- .foundInPath(unknown)
        unknown <- setdiff(unknown, names(inpath))
        inpath <- .filterDefaultPackages(inpath)
    }

    result <- BPValidate(
        symbol = names(inpath),
        environment = unlist(inpath, use.names = FALSE),
        unknown = unknown
    )

    if (any(inpath %in% ".GlobalEnv"))
        warning("function references symbol(s) in .GlobalEnv")

    result
}

.foundInPath <- function(symbols) {
    loc <- Map(function(elt) head(find(elt), 1), symbols)
    loc[lengths(loc) == 1L]
}

.filterDefaultPackages <- function(symbols) {
    pkgs <- c(
        "stats", "graphics", "grDevices", "utils", "datasets",
        "methods", "Autoloads", "base"
    )
    drop <- unlist(symbols, use.names = FALSE) %in% paste0("package:", pkgs)
    symbols[!drop]
}
