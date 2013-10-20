test_bpaggregate <- 
    function() 
{
    x <- data.frame(a=1:10, b=10:1)
    by <- list(c(rep("a", 5), rep("b", 5)))
    simplify <- TRUE
    FUN <- mean

    x1 <- aggregate(x, by=by, FUN=FUN)
    x2 <- bpaggregate(x, by=by, FUN=FUN, simplify=simplify)
    checkEquals(x1, x2)

    by[[2]] <- c(rep("c", 8), rep("d", 2))
    x1 <- aggregate(x, by=by, FUN=FUN)
    x2 <- bpaggregate(x, by=by, FUN=FUN, simplify=simplify)
    checkEquals(x1, x2)
}
