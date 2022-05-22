message("Testing bpaggregate")

test_bpaggregate <- 
    function() 
{
    x <- data.frame(a=1:10, b=10:1)
    by <- list(c(rep("a", 5), rep("b", 5)))
    simplify <- TRUE
    FUN <- mean

    x1 <- aggregate(x, by=by, FUN=FUN)
    param <- bpparam()
    bpworkers(param) <- 2
    x2 <- bpaggregate(x, by=by, FUN=FUN, BPPARAM=param, simplify=simplify)
    checkEquals(x1, x2)

    by[[2]] <- c(rep("c", 8), rep("d", 2))
    x1 <- aggregate(x, by=by, FUN=FUN)
    x2 <- bpaggregate(x, by=by, FUN=FUN, BPPARAM=param, simplify=simplify)
    checkEquals(x1, x2)

    closeAllConnections()
    TRUE
}

test_bpaggregate_formula <-
    function()
{
    f <- Sepal.Length ~ Species
    iris1 <- iris
    iris1$Species <-       # FIXME: bpaggregate doesn't respect factor
        as.character(iris1$Species) 
    x1 <- aggregate(f, data=iris1, FUN=sum)
    x2 <- bpaggregate(f, data = iris1, FUN = sum)
    checkEquals(x1, x2)

    iris1 <- iris1[sample(nrow(iris1)),]
    x3 <- bpaggregate(f, data = iris1, FUN = sum)
    checkEquals(x2, x3)
}
