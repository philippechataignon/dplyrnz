#A dplyr connector for the Netezza database.
setClass("NetezzaConnection", representation = representation(conn = "ANY"))

.NetezzaConnection <- function(conn) {
  new("NetezzaConnection",conn=conn)
}

#' Establishes a connection to Netezza and returns a dplyr 'src' object, src_netezza, using RODBC.
#'
#' @param dsn The name of the ODBC DSN to use for connecting to Netezza. Must be a string.
#' @param db  The name of the database if not in DSN definition (optional)
#' @param uid The name of the user if not in DSN definition (optional)
#' @param pwd The password if not in DSN definition (optional)
#' @return A dplyr::src object, dplyr::src_sql, src_netezza, to be used with dplyr functions.
#' @examples
#' \dontrun{
#' netezza_connection <- src_netezza(dsn="NetezzaDSN")
#' table <- tbl(netezza_connection, 'TABLE_NAME')
#' }
#' @import methods
#' @import R6
#' @import assertthat
#' @import RODBC
#' @import dplyr

#' @export
src_netezza <- function(dsn, db=NULL, uid=NULL, pwd=NULL, ...) {
    if (!requireNamespace("assertthat", quietly = TRUE)) {
        stop("assertthat package required", call. = FALSE)
    }
    if (!requireNamespace("RODBC", quietly = TRUE)) {
        stop("RODBC package required to connect to Netezza ODBC", call. = FALSE)
    }
    assertthat::assert_that(assertthat::is.string(dsn))

    st <- paste0("DSN=", dsn)
    if (!is.null(uid)) {
        st <- paste0(st, ";UID=", uid)
    }
    if (!is.null(pwd)) {
        st <- paste0(st, ";PWD=", pwd)
    }
    if (!is.null(db)) {
        st <- paste0(st, ";Database=", db)
    }

    conn <- RODBC::odbcDriverConnect(st, ...)
    info <- RODBC::odbcGetInfo(conn)
    con <- .NetezzaConnection(conn=conn)
    src_sql("netezza", con = con, info = info, disco = db_disconnector(con, "netezza"))
}

#' @export
tbl.src_netezza <- function(src, from, ...) {
    tbl_sql("netezza", src = src, from = from, ...)
}

# Describes the connection
#' @export
src_desc.src_netezza <- function(x) {
    info <- x$info
    paste0("Netezza ODBC [", info["Data_Source_Name"], "]")
}

#' @export
dim.tbl_netezza <- function(x) {
    p <- x$query$ncol()
    c(NA, p)
}

#' @export
src_translate_env.src_netezza <- function(x) {
    sql_variant(
        sql_translator(.parent = base_scalar,
            year = function(x) build_sql("date_part('year',", x, ")"),
            month = function(x) build_sql("date_part('month',", x, ")"),
            quarter = function(x) build_sql("date_part('quarter',", x, ")"),
            week = function(x) build_sql("date_part('week',", x, ")"),
            day = function(x) build_sql("date_part('day',", x, ")"),
            hour = function(x) build_sql("date_part('hour',", x, ")"),
            minute = function(x) build_sql("date_part('minute',", x, ")"),
            second = function(x) build_sql("date_part('second',", x, ")"),
            dow = function(x) build_sql("date_part('dow',", x, ")"),
            doy = function(x) build_sql("date_part('doy',", x, ")"),
            to_char = sql_prefix("to_char", 2),
            date_part = sql_prefix("date_part", 2),
            date_trunc = sql_prefix("date_trunc", 2)
        ),
        sql_translator(.parent = base_agg,
            n = function() sql("count(*)::integer"),
            sd = sql_prefix("stddev")
        ),
        base_win
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
        send_query(self$con@conn, self$sql)
    },

    fetch_paged = function(chunk_size = 1e4, callback) {
        warning("This package does not support fetched_paged for Netezza")
        invisible(TRUE)
    },

    vars = function() {
      private$.vars
    },

    nrow = function() {
        NA
    },

    ncol = function() {
        length(self$vars())
    }
  )
)

#' @export
db_list_tables.NetezzaConnection <- function(con) {
    query <- "SELECT tablename as name FROM _v_table where objtype in ('TABLE', 'TEMP TABLE')
        union SELECT viewname as name FROM _v_view where objtype='VIEW'
        union SELECT synonym_name as name FROM _v_synonym where objtype='SYNONYM'
    "
    res <- send_query(con@conn, query)
    res[[1]]
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
  fields <- build_sql("SELECT * FROM ", sql, " LIMIT 0")
  qry <- send_query(con@conn, fields)
  names(qry)
}

#' @export
db_query_rows.NetezzaConnection <- function(con, sql, ...) {
  assertthat::assert_that(assertthat::is.string(sql), is.sql(sql))
  from <- sql_subquery(con, sql, "master")
  rows <- build_sql("SELECT count(*) FROM ", from, con=con)
  as.integer(send_query(con@conn, rows)[[1]])
}

# Disconnect

db_disconnector <- function(con, name, quiet = TRUE) {
  reg.finalizer(environment(), function(...) {
    if (!quiet) {
        message("Auto-disconnecting ", name, " connection ",
            "(", paste(con@conn, collapse = ", "), ")")
    }
    odbcClose(con@conn)
    })
  environment()
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

# Analyse
#' @export
db_analyze.src_netezza <- function(src, table, ...) {
  db_analyze(src$con, table, ...)
}

#' @export
db_analyze.NetezzaConnection <- function(con, table, ...) {
  sql <- build_sql("GENERATE STATISTICS ON ", ident(table), con=con)
  send_query(con@conn, sql)
}

# Save
#' @export
db_save_query.NetezzaConnection <- function(con, sql, name, temporary = TRUE, ...) {
    if (db_has_table(con, name)) {
        stop(paste0("Table ", name, " already exists"))
    }

    ct_sql <- build_sql("CREATE ", if (temporary) sql("TEMPORARY "), "TABLE ",
                        ident(name), " AS (", sql, ")", con = con)
    send_query(con@conn, ct_sql)
    name
}

# Query

send_query <- function(conn, query, ...) {
  sqlQuery(conn, query, believeNRows=F, stringsAsFactors = F)
}
