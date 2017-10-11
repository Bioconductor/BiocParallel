.AWSParam <- setRefClass("AWSParam",
    contains = "BiocParallelParam",
    fields = list(
        awsAccessKey = "character",
        awsSecretKey = "character",
        awsInstanceType = "character", ## instance type i.e "t2.micro"
        awsSubnet = "character",
        awsSecurityGroup = "character",
        awsInstance = "list",
        awsAmiId = "character"
    ),
    methods = list(
        initialize = function(...,
             awsAccessKey = NA_character_,
             awsSecretKey = NA_character_,
             awsInstanceType = NA_character_,
             awsSubnet = NA,
             awsSecurityGroup = NA,
             awsAmiId = NA_character_
             )
        {
            callSuper(...)
            initFields(
                awsAccessKey = awsAccessKey,
                awsSecretKey = awsSecretKey,
                awsInstanceType = awsInstanceType,
                awsSubnet = awsSubnet,
                awsSecurityGroup = awsSecurityGroup,
                awsAmiId = awsAmiId
            )
        },
        show = function() {
            callSuper()
            ## Display only half of AWS access and secret keys
            cat("  awsAccessKey: ",
                awsAccessKey(.self),
                "\n",
                "  awsSecretKey: ",
                paste(rep("*", nchar(awsSecretKey(.self))), collapse=""),
                "\n",
                "  awsInstanceType: ", awsInstanceType(.self),
                "\n",
                "  awsSubnet: ", awsSubnet(.self),
                "\n",
                "  awsSecurityGroup: ", awsSecurityGroup(.self),
                "\n",
                "  awsAmiId: ", awsAmiId(.self),
                "\n",
                sep = "")
        }
    )
)

## Get name of bioconductor release version AMI
#' @importFrom httr content
#' @importFrom httr stop_for_status
#' @importFrom httr GET
#' @importFrom yaml yaml.load
.getAwsAmiId <-
    function()
{
    res <- httr::GET("https://www.bioconductor.org/config.yaml")
    httr::stop_for_status(res)
    content <- httr::content(res, type="text", encoding="UTF-8")
    txt <- yaml::yaml.load(content)
    release_version <- sub(".", "_", txt$release_version, fixed=TRUE)
    txt$ami_ids[[paste0("bioc",release_version)]]
}


AWSParam <-
    function(workers = 1,
             awsAccessKey = NA_character_,
             awsSecretKey = NA_character_,
             awsInstanceType = NA_character_,
             awsSubnet = NA,
             awsSecurityGroup = NA,
             awsAmiId = NA_character_
             )
{
    stopifnot(
        !missing(awsAccessKey),
        !missing(awsSecretKey),
        !missing(awsInstanceType),
        !missing(awsSubnet) ,
        !missing(awsSecurityGroup)
    )
    ## If missing, default to release version of AMI
    if (missing(awsAmiId)) {
        awsAmiId <- .getAwsAmiId()
    }

    x <- .AWSParam(workers = workers,
                   awsAccessKey = awsAccessKey,
                   awsSecretKey = awsSecretKey,
                   awsInstanceType = awsInstanceType,
                   awsSubnet = awsSubnet,
                   awsSecurityGroup = awsSecurityGroup,
                   awsAmiId = awsAmiId)
    validObject(x)
    x
}


### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Accessors
###

awsAccessKey <- 
    function(x)
{
    x$awsAccessKey
}

awsSecretKey <- 
    function(x)
{
    x$awsSecretKey
}


awsWorkers <-
    function(x)
{
    x$workers
}

.awsCredentials <-
    function(x)
{
    fpath <- "~/.aws/credentials"
    ## AWS credential template
    template <-
        sprintf("[default]
aws_access_key_id = %s
aws_secret_access_key = %s", awsAccessKey(x), awsSecretKey(x))
    if (!file.exists(fpath)) {
        fileConn <- file(fpath)
        writeLines(template, fileConn)
        close(fileConn)
    }
    ## Use credentials written in file
    use_credentials()
}

awsInstance <-
    function(x)
{
    x$awsInstance
}

awsInstanceType <- 
    function(x)
{
    x$awsInstanceType
}

awsAmiId <- 
    function(x)
{
    x$awsAmiId
}

awsSubnet <-
    function(x)
{
    x$awsSubnet
}

awsSecurityGroup <-
    function(x)
{
    x$awsSecurityGroup
}

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Methods - control
###

.awsCluster <- local({
    cl <- NULL
    list(
        isup = function() !is.null(cl),
        get = function() cl,
        set = function(cluster) {
            stopifnot(is(cluster, "AWSParam"))
            cl <<- cluster
        }
    )
})

awsCluster <- function() {
    if (!.awsCluster$isup())
        stop("no existing cluster")
    .awsCluster$get()
}

#' @importFrom aws.ec2 run_instances
setMethod("bpstart", "AWSParam",
    function(x)
    {
        if (.awsCluster$isup())
            stop(
                "use 'bpstop(awsCluster())' to shut down existing AWS cluster",
                call. = FALSE
            )
        .awsCredentials(x)
        ## Set awsBiocVersion, devel vs release
        result <- run_instances(image=awsAmiId(x),
                                type=awsInstanceType(x),
                                min=awsWorkers(x),
                                keypair = "bioc-default",
                                subnet=awsSubnet(x),
                                sgroup=awsSecurityGroup(x))
        ## Print instance state to screen after starting instance
        x$awsInstance <- result
        .awsCluster$set(x)
        ## Wait for instance to be up.
        message("starting..", appendLF = FALSE)
        repeat{
            if (bpisup(x)) {
                break
            }
            message(".", appendLF = FALSE)
            Sys.sleep(1)
        }
        message(.awsInstanceStatus(x))
        invisible(x)
    })


# Check status of aws ec2 instance
#' @importFrom aws.ec2 instance_status
.awsInstanceStatus <-
    function(x)
{
    instance <- awsInstance(x)
    if (length(instance) == 0L) {
        "stopped"
    } else {
        status <- instance_status(instance)
        if (length(status) == 0L)
            "starting"
        else status$item$instanceState$name[[1]]
    }
}

#' @importFrom aws.ec2 terminate_instances
setMethod("bpstop", "AWSParam",
    function(x)
    {
        if (bpisup(x)) {
            result <- terminate_instances(x$awsInstance)
            message("stopping..", appendLF = FALSE)
            repeat {
                if (!bpisup(x))
                    break
                message(".", appendLF = FALSE)
            }
            message(.awsInstanceStatus(x))
        }
        ## Return terminated instance state to screen
        x$awsInstance <- list()
        invisible(x)
    })


setMethod("bpisup", "AWSParam",
    function(x)
    {
        .awsInstanceStatus(x) == "running"
    })



.awsClusterIps <- function(x)
{
    instances <- aws.ec2::describe_instances(awsInstance(aws))
    vapply(instances[[1]][["instancesSet"]], `[[`, character(1), "ipAddress")
}

