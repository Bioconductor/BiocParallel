setMethod(bpmapply, c("ANY", "missing"),
    function(FUN, ..., MoreArgs=NULL, SIMPLIFY=TRUE, USE.NAMES=TRUE,
        BPRESUME=getOption("BiocParallel.BPRESUME", FALSE), 
        BPPARAM=bpparam())
{
    FUN <- match.fun(FUN)
    bpmapply(FUN, ..., MoreArgs=MoreArgs, SIMPLIFY=SIMPLIFY,
        USE.NAMES=USE.NAMES, BPRESUME=BPRESUME, BPPARAM=BPPARAM)
})
