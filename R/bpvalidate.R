bpvalidate <- function(fun)
{
    if (typeof(fun) != "closure")
        stop("'fun' must be a closure")
    unknown <- codetools::findGlobals(fun)
    f_env <- environment(fun)

    ## 'fun' environment is NAMESPACE
    if (length(unknown) && isNamespace(f_env)) {
        f_ls <- c(getNamespaceImports(f_env),
                  setNames(list(ls(f_env, all.names=TRUE)),
                           getNamespaceName(f_env)))
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
        unknown <- unknown[rowSums(xx) == 0L]
    }

    ## look in search path
    inpath <- structure(list(), names=character())
    if (length(unknown)) {
        inpath <- .foundInPath(unknown)
        unknown <- setdiff(unknown, names(inpath))
        inpath <- .filterDefaultPackages(inpath)

        env <- environment(fun)
        while(!identical(env, topenv(environment(fun)))) {
            inlocal <- ls(env, all.names = TRUE)
            unknown <- setdiff(unknown, inlocal)
            env <- parent.env(env)
        }
    }

    if (length(unknown))
        warning("function references unknown symbol(s)")

    if (any(inpath %in% ".GlobalEnv"))
        warning("function references symbol(s) in .GlobalEnv")

    list(inPath=inpath, unknown=unknown)
}

.foundInPath <- function(symbols) {
    loc <- Map(function(elt) head(find(elt), 1), symbols)
    Filter(function(elt) length(elt) != 0, loc)
}

.filterDefaultPackages <- function(symbols) {
    defaults <- paste0("package:", c("stats", "graphics", "grDevices", 
                       "utils", "datasets", "methods", "Autoloads", "base"))
    Filter(function(elt) !elt %in% defaults, symbols)
}
