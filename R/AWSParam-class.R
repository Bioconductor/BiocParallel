.AWSParam <- setRefClass(
    "AWSParam",
    contains = "BiocParallelParam",
    fields = list(
        username = "character"
    ),
    methods = list(
        initialize = function(..., username = NA_character_) {
            callSuper(...)
            initFields(username = username)
        },
        show = function() {
            callSuper()
            cat("  username: ", username(.self), "\n", sep = "")
        }
    )
)

AWSParam <-
    function(username = NA_character_)
{
    x <- .AWSParam(username = username)
    validObject(x)
    x
}

setMethod("username", "AWSParam",
    function(x)
{
    x$username
})

## TODO:
## - bpstart()
## - bpstop()
