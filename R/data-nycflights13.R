#' Database versions of the nycflights13 data
#'
#' These functions cache the data from the \code{nycflights13} database in
#' a local database, for use in examples and vignettes. Indexes are created
#' to making joining tables on natural keys efficient.
#'
#' @keywords internal
#' @name nycflights13
NULL

#' @export
#' @rdname nycflights13
copy_nycflights13 <- function(src, ...) {
  all <- utils::data(package = "nycflights13")$results[, 3]
  tables <- setdiff(all, src_tbls(src))

  # Create missing tables
  for(table in tables) {
    df <- getExportedValue("nycflights13", table)
    message("Creating table: ", table)
    copy_to(src, df, table, temporary=FALSE, replace=T)
  }
  src
}
