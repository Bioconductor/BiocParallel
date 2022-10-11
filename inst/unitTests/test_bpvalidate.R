message("Testing bpvalidate")

BPValidate <- BiocParallel:::BPValidate

test_bpvalidate_basic_ok <- function()
{
    target <- BPValidate()
    checkIdentical(target, bpvalidate(function() {}         ))
    checkIdentical(target, bpvalidate(function(x) x         ))
    checkIdentical(target, bpvalidate(function(x) x()       ))
    checkIdentical(target, bpvalidate(function(..., x) x    ))
    checkIdentical(target, bpvalidate(function(..., x) x()  ))
    checkIdentical(target, bpvalidate(function(y, x) y(x)   ))
    checkIdentical(target, bpvalidate(function(y, x) y(x=x) ))
    checkIdentical(target, bpvalidate(function(y, ...) y(...) ))
    checkIdentical(target, bpvalidate(local({i = 2; function(y) y + i})))
    checkIdentical(
        target,
        bpvalidate(local({i = 2; local({function(y) y + i})}))
    )

    checkIdentical(target, bpvalidate(sqrt))
}

test_bpvalidate_basic_fail <- function()
{
    target <- BPValidate(unknown = "x")
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
    target <- BPValidate(symbol = "x", environment = "package:.test_env")

    .test_env <- new.env(parent=emptyenv())
    .test_env$x <- NULL
    attach(.test_env, name = "package:.test_env")
    on.exit(detach("package:.test_env"))

    checkIdentical(target, bpvalidate(function() x            ))
    checkIdentical(target, bpvalidate(function(...) x         ))
    checkIdentical(target, bpvalidate(function(y, ...) y(x)   ))
    checkIdentical(target, bpvalidate(function(y, ...) y(x=x) ))

    ## FIXME: should fail -- in search(), but not a function!
    ## checkIdentical(target, bpvalidate(function() x() ))
}

test_bpvalidate_defining_environemt <- function()
{
    target1 <- BPValidate()
    target2 <- BPValidate(unknown = "x")

    h = function() { x <- 1; f = function() x; function() f() }
    checkIdentical(target1, bpvalidate(h))

    h = function() { f = function() x; function() f() }
    checkIdentical(target2, bpvalidate(h, "silent"))
}

test_bpvalidate_library <- function()
{
    target <- BPValidate()

    checkException(bpvalidate(function() library("__UNKNOWN__"), signal = "error"),
                   silent=TRUE)
    checkException(bpvalidate(function() require("__UNKNOWN__"), signal = "error"),
                   silent=TRUE)
    checkIdentical(target, bpvalidate(function() library(BiocParallel)))

    ## FIXME: bpvalidate expects unquoted arg to library() / require()
    ## bpvalidate(function() library("BiocParallel"))

    target1 <- BPValidate(
        symbol = "bpvalidate", environment = "package:BiocParallel"
    )
    checkIdentical(target1, bpvalidate(function() bpvalidate())) # inPath
    fun <- function() { library(BiocParallel); bpvalidate() }
    checkIdentical(target, bpvalidate(fun))                      # in function
}
