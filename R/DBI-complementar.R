

access_quoteDate <- function(conn, x, ...) {
  if (is(x, "SQL")) return(x)
  if (length(x) == 0L) return(SQL(character()))
  str <- paste0("#", x, "#")
  str[is.na(x)] <- "NULL"
  SQL(str)
}


#' @importFrom methods setMethod
NULL

#' @importMethodsFrom DBI dbQuoteLiteral
NULL

#' @export
setMethod("dbQuoteLiteral",
          c("ACCESS","POSIXt"),
          access_quoteDate)

#' @export
setMethod("dbQuoteLiteral",
          c("ACCESS","Date"),
          access_quoteDate)
