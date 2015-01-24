
bpvalidate <- function(fun)
{
    if (typeof(fun) != "closure")
        stop("'fun' must be a closure")

    required <- character()
    unknown <- codetools::findGlobals(fun)

    ## attempt to load libraries in 'fun'
    warn <- err <- tryLoad <- NULL
    if (any(c("require", "library") %in% unknown)) {
        tryLoad <- withCallingHandlers(tryCatch({
            dep <- deparse(fun)
            i <- grepl("library", dep, fixed=TRUE) |
                 grepl("require", dep, fixed=TRUE)
            xx <- sapply(dep[i], function(x) eval(parse(text=x)))
            NULL
        }, error=function(e) {
            err <<- append(err, conditionMessage(e))
            NULL
        }), warning=function(w) {
            warn <<- append(warn, conditionMessage(w))
            invokeRestart("muffleWarning")
        })
        if (!is.null(tryLoad)) {
            msg <- "attempt to load library failed:\n"
            stop(sprintf(msg, paste(c(warn, err), collapse="\n      ")))
        }
    }

    ## 'fun' environment is a NAMESPACE
    if (length(unknown) && isNamespace(environment(fun))) {
        f_env <- environment(fun)
        ## add package name
        required <- c(required, getNamespaceName(f_env))
        f_ls <- c(getNamespaceImports(f_env),
                  setNames(list(ls(f_env, all=TRUE)),
                           getNamespaceName(f_env)))
        f_symbols <- unique(unlist(f_ls, use.names=FALSE))
        ## filter symbols
        unknown <- unknown[unknown %in% f_symbols]
    }

    ## look for symbols on search path
    if (length(unknown)) {
        loc <- Map(function(elt) head(find(elt), 1), unknown)
        ## add package name
        required <- unique(c(required, sub("package:", "", unlist(loc))))
        ## filter symbols (.GlobalEnv -> unknown)
        unknown <- names(Filter(function(elt) length(elt) == 0, loc))
        if (length(global <- names(loc[loc %in% ".GlobalEnv"])))
            unknown <- c(unknown, global)
    }

    ## filter default packages
    defaults <- c("stats", "graphics", "grDevices", "utils", "datasets",
                  "methods", "Autoloads", "base", ".GlobalEnv")
    required <- required[!required %in% defaults]
    if (length(required))
        required <- paste(required, collapse=",")
    if (length(unknown))
        warning("function references unknown symbol(s)")

    list(RequiredPackages=required, UnknownSymbols=unknown)
}
