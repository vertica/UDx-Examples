# Copyright (c) 2005 - 2015 Hewlett Packard Enterprise Development LP  -*- C++ -*-

##############################################
# Description: Example User Defined Functions in R
#
# Create Date: April 10, 2012
##############################################

##############################################
# User Defined Scalar Functions in R
##############################################

##########
# Example: Multiplication
##########

###
# @brief multiplies col1 and col2 of the input data frame.
###
mul<- function(x)
{
	pr <- x[,1] * x[,2]
	pr
}

mulFactory <- function()
{
	list(name=mul,udxtype=c("scalar"),intype=c("float","float"), outtype=c("float"))
}


##############################################
# User Defined Transform Functions in R
##############################################

##########
# Example: K-means
##########

###
# @brief Runs K-means clustering algorithm (with K=2) on the input data frame.
#
# @param x input data frame with two float columns, representing
#          two-dimension points: (x float, y float).
# @return a data frame with three columns (the point coordinates plus
#         their assigned cluster {1..k}): (x float, y float, cluster int).
###
kmeansClu <- function(x)
{
	# Fix initial centroids to get predictable clustering.
	cx <- c(1.5, 2.5)
	cy <- c(3.5, 4.5)
	centroids <- data.frame(cx,cy)
	cl <- kmeans(x[,1:2], centroids)
	res <- data.frame(x[,1:2], cl$cluster)
	res
}

kmeansCluFactory <- function()
{
	list(name=kmeansClu,udxtype=c("transform"),intype=c("float","float"), outtype=c("float","float","int"), outnames=c("x","y","cluster"))
}
