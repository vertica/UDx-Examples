KMeans_User <- function(input.data.frame, parameters.data.frame) {
  # Take the clusters and nstart parameters passed by the user and assign them
  # to variables in the function. 
  if ( is.null(parameters.data.frame[['clusters']]) ) {
    stop("NULL value for clusters! clusters cannot be NULL.")
  } else {
    clusters <- parameters.data.frame[['clusters']]
  }
  if ( is.null(parameters.data.frame[['nstart']]) ) {
    stop("NULL value for nstart! nstart cannot be NULL.")
  } else {
    nstart.value <- parameters.data.frame[['nstart']]
  }
  # Apply the algorithm to the data.
  kmeans.clusters <- kmeans(input.data.frame[, 1], clusters,
                            nstart = nstart.value)
  final.output <- data.frame(kmeans.clusters$cluster)
  return(final.output)
}

KMeans_UserFactory <- function() {
  list(name    = KMeans_User,
       udxtype = c("scalar"),
       # Since this is a polymorphic function the intype must be any
       intype  = c("any"),
       outtype = c("int"),
       parametertypecallback=KMeansParameters)
}


KMeansParameters <- function() {
  parameters <- list(datatype = c("int", "int"),
                     length   = c("NA", "NA"),
                     scale    = c("NA", "NA"),
                     name     = c("clusters", "nstart"))
  return(parameters)
}
