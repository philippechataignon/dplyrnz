#A dplyr connector for the Netezza database.
setClass("NetezzaConnection", representation = representation(conn = "ANY"))

.NetezzaConnection <- function(conn) {
  new("NetezzaConnection",conn=conn)
}

#' Establishes a connection to Netezza and returns a dplyr 'src' object, src_netezza, using RODBC.
#'
#' @param dsn The name of the ODBC DSN to use for connecting to Netezza. Must be a string.
#' @return A dplyr::src object, src_netezza, to be used with dplyr functions.
#' @examples
#' \dontrun{
#' netezza_connection <- src_netezza(dsn="NetezzaDSN")
#' table <- tbl(netezza_connection, 'TABLE_NAME')
#' }
#' @import methods
#' @import assertthat
#' @import RODBC
#' @import dplyr
#' @export

src_netezza <- function(dsn) {
    if (!requireNamespace("assertthat", quietly = TRUE)) {
        stop("assertthat package required", call. = FALSE)
    }
    if (!requireNamespace("RODBC", quietly = TRUE)) {
        stop("RODBC package required to connect to Netezza ODBC", call. = FALSE)
    }
    assertthat::assert_that(assertthat::is.string(dsn))
    RODBC::odbcCloseAll()
    conn <- RODBC::odbcConnect(dsn)
    info <- RODBC::odbcGetInfo(conn)
    con <- .NetezzaConnection(conn=conn)
    vsrc <- src_sql("netezza", con = con, info = info)
    vsrc
}

#' @export
#' @rdname src_netezza
tbl.src_netezza <- function(src, from, ...) {
    tbl_sql("netezza", src = src, from = from, ...)
}


# Describes the connection
#' @export
src_desc.src_netezza <- function(x) {
    info <- x$info
    paste0("Netezza ODBC - DSN:", info["Data_Source_Name"], " - Host:", info["Server_Name"])
}


#' @export
src_translate_env.src_netezza <- function(x) {
    sql_variant(
        base_scalar,
        sql_translator(.parent = base_agg,
            n = function() sql("count(*)"),
            sd = sql_prefix("stddev"),
            var = sql_prefix("variance")
        )
    )
}

#' @export
query.NetezzaConnection <- function(con, sql, .vars) {
    assertthat::assert_that(assertthat::is.string(sql))
    Netezza.Query$new(con, sql(sql), .vars)
}

Netezza.Query <- R6::R6Class("Netezza.Query",
  private = list(
    .nrow = NULL,
    .vars = NULL
  ),
  public = list(
    con = NULL,
    sql = NULL,

    initialize = function(con, sql, vars) {
      self$con <- con
      self$sql <- sql
      private$.vars <- vars
    },

    print = function(...) {
      cat("<Query> ", self$sql, "\n", sep = "")
      print(self$con)
    },

    fetch = function(n = -1L) {
        out <- sqlQuery(self$con@conn, self$sql, n, believeNRows = FALSE)
        i <- sapply(out, is.factor)
        out[i] <- lapply(out[i], as.character)
      out
    },

    fetch_paged = function(chunk_size = 1e4, callback) {
        warning("This package does not support fetched_paged for Netezza")
        invisible(TRUE)
    },

    vars = function() {
      private$.vars
    },

    nrow = function() {
        if (!is.null(private$.nrow)) return(private$.nrow)
            private$.nrow <- db_query_rows(self$con, self$sql)
            private$.nrow
    },

    ncol = function() {
        length(self$vars())
    }
  )
)

#' @export
db_list_tables.NetezzaConnection <- function(con) {
  sqlTables(con@conn)[[3]]
}

#' @export
db_list_tables.src_netezza <- function(src) {
  db_list_tables(src$con)
}

#' @export
db_has_table.src_netezza <- function(src, table) {
  db_has_table(src$con, table)
}

#' @export
db_has_table.NetezzaConnection <- function(con, table) {
  table %in% db_list_tables(con)
}

#' @export
db_query_fields.NetezzaConnection <- function(con, sql, ...){
  assertthat::assert_that(assertthat::is.string(sql), is.sql(sql))
  fields <- build_sql("SELECT * FROM ", sql, " WHERE 0=1")
  qry <- send_query(con@conn, fields)
  names(qry)
}

#' @export
db_query_rows.NetezzaConnection <- function(con, sql, ...) {
  assertthat::assert_that(assertthat::is.string(sql), is.sql(sql))
  from <- sql_subquery(con, sql, "master")
  rows <- paste0("SELECT count(*) FROM ", from)
  as.integer(send_query(con@conn, rows)[[1]])
}

# Explains queries
#' @export
db_explain.NetezzaConnection <- function(con, sql, ...) {
  exsql <- build_sql("EXPLAIN ", sql, con = con)
  output <- send_query(con@conn, exsql)
  output <- apply(output,1,function(x){
    if(substring(x,1,1) == "|") x = paste0("\n",x)
    if(x == "") x = "\n"
    x
  })
}

# Query

send_query <- function(conn, query, ...) UseMethod("send_query")

send_query.RODBC <- function(conn, query, ...) {
  sqlQuery(conn, query, believeNRows=FALSE)
}
