SalesTaxCalculator <- function(input.data.frame) {
  # Not a complete list of states in the USA, but enough to get the idea.
  state.sales.tax <- list(ma=0.0625,
                          az=0.087,
                          la=0.0891,
                          tn=0.0945,
                          wi=0.0543,
                          ms=0.0707)
  for ( state_abbreviation in input.data.frame[, 2] ) {
    # Ensure state abbreviations are lowercase.
    lower_state <- tolower(state_abbreviation)
    # Check if the state is in our state.sales.tax list.
    if (is.null(state.sales.tax[[lower_state]])) {
      stop("State is not in our small sample!")
    } else {
      sales.tax.rate <- state.sales.tax[[lower_state]]
      item.price <- input.data.frame[, 1]
      # Calculate the price including sales tax.
      price.with.sales.tax <- (item.price) + (item.price * sales.tax.rate)
    }
  }
  return(price.with.sales.tax)
}

SalesTaxCalculatorFactory <- function() {
  list(name=SalesTaxCalculator,
       udxtype=c("scalar"),
       intype=c("float", "varchar"),
       outtype=c("float"))
}
