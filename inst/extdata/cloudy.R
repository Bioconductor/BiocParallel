###############################
# bpstart
aws.signature::use_credentials(profile = "nturaga")
image_devel <- "ami-7d342906"
image_release <- "ami-9fe2fee4"


image <- image_devel
describe_images(image)
library("aws.ec2")
s <- describe_subnets()
g <- describe_sgroups()
s
s[[1]]
s[[4]]
g[[40]]
image_types = c("t2.micro")
# sg <- "sg-748dcd07"
# subnet <- "subnet-d66a05ec"
sg_index <- g %>% sapply(., "[[", "groupId") %in% sg %>% which(.)
subnet_index <- s %>% sapply(., "[[", "subnetId") %in% subnet %>% which(.)
s[[4]]
i <- run_instances(image = image, 
                   type = "t2.micro", # <- you might want to change this
                   subnet = s[[4]], 
                   sgroup = g[[40]])

i <- run_instances(image = image, 
                   type = "t2.micro", # <- you might want to change this
                   subnet = "subnet-d66a05ec", 
                   sgroup = "sg-748dcd07")
image

## instance status
state <- instance_status(i)$item$instanceState$name[[1]]
state
if (state == "running") {
    print("yes")
}

# Bpstop
res <- terminate_instances(i)
###############################

## API

aws = AWSParam$new(aws_access_key,
                   aws_secret_key,
                   instance_type="t2.micro",
                   bioc_version="devel")

instance_type(aws)

bioc_version(aws)

aws$bpstart


