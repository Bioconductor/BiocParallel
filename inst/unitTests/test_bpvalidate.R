test_bpvalidate_basic_ok <- function()
{
    target <- list(inPath=structure(list(), names=character()),
                   unknown=character())
    checkIdentical(target, bpvalidate(function() {}         ))
    checkIdentical(target, bpvalidate(function(x) x         ))
    checkIdentical(target, bpvalidate(function(x) x()       ))
    checkIdentical(target, bpvalidate(function(..., x) x    ))
    checkIdentical(target, bpvalidate(function(..., x) x()  ))
    checkIdentical(target, bpvalidate(function(y, x) y(x)   ))
    checkIdentical(target, bpvalidate(function(y, x) y(x=x) ))
    checkIdentical(target, bpvalidate(function(y, ...) y(...) ))
}

test_bpvalidate_basic_fail <- function()
{
    target <- list(inPath=structure(list(), names=character()), unknown="x")

    suppressWarnings({
        checkIdentical(target, bpvalidate(function() x            ))
        checkIdentical(target, bpvalidate(function() x()          ))
        checkIdentical(target, bpvalidate(function(y) x + y       ))
        checkIdentical(target, bpvalidate(function(y) y(x)        ))
        checkIdentical(target, bpvalidate(function(y) y(x=x)      ))
        checkIdentical(target, bpvalidate(function(y, ...) y(x)   ))
        checkIdentical(target, bpvalidate(function(y, ...) y(x=x) ))
    })
}

test_bpvalidate_search_path <- function()
{
    target <- list(inPath=list(x=".test_env"), unknown=character())

    .test_env <- new.env(parent=emptyenv())
    .test_env$x <- NULL
    attach(.test_env)
    on.exit(detach(.test_env))

    checkIdentical(target, bpvalidate(function() x            ))
    checkIdentical(target, bpvalidate(function(...) x         ))
    checkIdentical(target, bpvalidate(function(y, ...) y(x)   ))
    checkIdentical(target, bpvalidate(function(y, ...) y(x=x) ))

    ## FIXME: should fail -- in search(), but not a function!
    ## checkIdentical(target, bpvalidate(function() x() ))
}

test_bpvalidate_library <- function()
{
    target <- list(inPath=structure(list(), names=character()),
                   unknown=character())

    checkException(bpvalidate(function() library("__UNKNOWN__")), silent=TRUE)
    checkException(bpvalidate(function() require("__UNKNOWN__")), silent=TRUE)

    ## FIXME: internally, bpvalidate code expects a matrix but gets a vector
    ## bpvalidate(function() library(BiocParallel))
    ## FIXME: bpvalidate expects unquoted arg to library() / require()
    ## bpvalidate(function() library("BiocParallel"))

    target1 <- list(inPath=list(bpvalidate = "package:BiocParallel"),
                    unknown = character(0))
    checkIdentical(target1, bpvalidate(function() bpvalidate())) # inPath
    fun <- function() { library(BiocParallel); bpvalidate() }
    checkIdentical(target, bpvalidate(fun))                      # in function
}
