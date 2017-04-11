LogTokenizer <- function(input.data.frame, parameters.data.frame) {
  # Take the spliton parameter passed by the user and assign it to a variable
  # in the function so we can use that as our tokenizer. 
  if ( is.null(parameters.data.frame[['spliton']]) ) {
    stop("NULL value for spliton! Token cannot be NULL.")
  } else {
    split.on <- as.character(parameters.data.frame[['spliton']])
  }
  # Tokenize the string.
  tokens <- vector(length=0)
  for ( string in input.data.frame[, 1] ) {
    tokenized.string <- strsplit(string, split.on)
    for ( token in tokenized.string ) {
      tokens <- append(tokens, token)
    }
  }
  final.output <- data.frame(tokens)
  return(final.output)
}

LogTokenizerFactory <- function() {
  list(name    = LogTokenizer,
       udxtype = c("transform"),
       intype  = c("varchar"),
       outtype = c("varchar"),
       outtypecallback=LogTokenizerReturn,
       parametertypecallback=LogTokenizerParameters)
}


LogTokenizerParameters <- function() {
  parameters <- list(datatype = c("varchar"),
                     length   = c("NA"),
                     scale    = c("NA"),
                     name     = c("spliton"))
  return(parameters)
}

LogTokenizerReturn <- function(arg.data.frame, parm.data.frame) {
  output.return.type <- data.frame(datatype = rep(NA,1),
                                   length   = rep(NA,1),
                                   scale    = rep(NA,1),
                                   name     = rep(NA,1))
  output.return.type$datatype <- c("varchar")
  output.return.type$name <- c("Token")
  return(output.return.type)
}

