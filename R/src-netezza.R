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
  tbl_query <- "SELECT tablename FROM _v_table WHERE objtype in ('TABLE', 'VIEW', 'SYNONYM')"
  res <- sqlQuery(con@conn,tbl_query,believeNRows = FALSE)
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
  tbl_query <- paste0("SELECT count(*) N FROM _v_table WHERE objtype = 'TABLE' and tablename='", escape(ident(table)), "'")
  res <- sqlQuery(con@conn,tbl_query,believeNRows = FALSE)
  res$N > 0
}

#' @export
db_query_fields.NetezzaConnection <- function(con, sql, ...){
  assertthat::assert_that(assertthat::is.string(sql), is.sql(sql))
  fields <- build_sql("SELECT * FROM ", sql, " WHERE 0=1")
  qry <- sqlQuery(con@conn, fields,believeNRows = FALSE)
  names(qry)
}

#' @export
db_query_rows.NetezzaConnection <- function(con, sql, ...) {
  assertthat::assert_that(assertthat::is.string(sql), is.sql(sql))
  from <- sql_subquery(con, sql, "master")
  rows <- paste0("SELECT count(*) FROM ", from)
  as.integer(sqlQuery(con@conn, rows, believeNRows = FALSE)[[1]])
}

# Explains queries
#' @export
db_explain.NetezzaConnection <- function(con, sql, ...) {
  exsql <- build_sql("EXPLAIN ", sql, con = con)
  output <- send_query(con@conn, exsql, useGetQuery=TRUE)
  output <- apply(output,1,function(x){
    if(substring(x,1,1) == "|") x = paste0("\n",x)
    if(x == "") x = "\n"
    x
  })
}

# Query

send_query <- function(conn, query, useGetQuery=FALSE, ...) UseMethod("send_query")

send_query.RODBC <- function(conn, query, ...) {
  sqlQuery(conn,query,believeNRows = FALSE)
}

#' @export
sql_escape_ident.NetezzaConnection <- function(con, x) {
    sql_quote(x, '"')
}
#' @export
sql_escape_string.NetezzaConnection <- function(con, x) {
    sql_quote(x, "'")
}

ident_schema_table <- function(tablename) {
    build_sql('PUBLIC', ".", tablename)
}


### Select

#' @rdname backend_sql
#' @export
sql_select <- function(con, select, from, where = NULL, group_by = NULL,
  having = NULL, order_by = NULL, limit = NULL, offset = NULL, ...) {
  UseMethod("sql_select")
}
#' @export
sql_select.NetezzaConnection <- function(con, select, from, where = NULL,
                                     group_by = NULL, having = NULL,
                                     order_by = NULL, limit = NULL,
                                     offset = NULL, ...) {

  out <- vector("list", 8)
  names(out) <- c("select", "from", "where", "group_by", "having", "order_by",
    "limit", "offset")

  assertthat::assert_that(is.character(select), length(select) > 0L)
  out$select <- build_sql("SELECT ", escape(select, collapse = ", ", con = con))

  assertthat::assert_that(is.character(from), length(from) == 1L)
  out$from <- build_sql("FROM ", from, con = con)

  if (length(where) > 0L) {
    assertthat::assert_that(is.character(where))
    out$where <- build_sql("WHERE ",
      escape(where, collapse = " AND ", con = con))
  }

  if (!is.null(group_by)) {
    assertthat::assert_that(is.character(group_by), length(group_by) > 0L)
    out$group_by <- build_sql("GROUP BY ",
      escape(group_by, collapse = ", ", con = con))
  }

  if (!is.null(having)) {
    assertthat::assert_that(is.character(having), length(having) == 1L)
    out$having <- build_sql("HAVING ",
      escape(having, collapse = ", ", con = con))
  }

  if (!is.null(order_by)) {
    assertthat::assert_that(is.character(order_by), length(order_by) > 0L)
    out$order_by <- build_sql("ORDER BY ",
      escape(order_by, collapse = ", ", con = con))
  }

  if (!is.null(limit)) {
    assertthat::assert_that(is.integer(limit), length(limit) == 1L)
    out$limit <- build_sql("LIMIT ", limit, con = con)
  }

  if (!is.null(offset)) {
    assertthat::assert_that(is.integer(offset), length(offset) == 1L)
    out$offset <- build_sql("OFFSET ", offset, con = con)
  }

  escape(unname(dplyr:::compact(out)), collapse = "\n", parens = FALSE, con = con)
}


#' @export
#' @rdname backend_sql
sql_subquery <- function(con, sql, name = random_table_name(), ...) {
  UseMethod("sql_subquery")
}
#' @export
sql_subquery.NetezzaConnection <- function(con, sql, name = unique_name(), ...) {
  if (is.ident(sql)) return(sql)

  build_sql("(", sql, ") AS ", ident(name), con = con)
}

#' @rdname backend_sql
#' @export
sql_join <- function(con, x, y, type = "inner", by = NULL, ...) {
  UseMethod("sql_join")
}
#' @export
sql_join.NetezzaConnection <- function(con, x, y, type = "inner", by = NULL, ...) {
  join <- switch(type,
    left = sql("LEFT"),
    inner = sql("INNER"),
    right = sql("RIGHT"),
    full = sql("FULL"),
    stop("Unknown join type:", type, call. = FALSE)
  )

  by <- dplyr:::common_by(by, x, y)
  using <- all(by$x == by$y)

  # Ensure tables have unique names
  x_names <- auto_names(x$select)
  y_names <- auto_names(y$select)
  uniques <- unique_names(x_names, y_names, by$x[by$x == by$y])

  if (is.null(uniques)) {
    sel_vars <- c(x_names, y_names)
  } else {
    x <- update(x, select = setNames(x$select, uniques$x))
    y <- update(y, select = setNames(y$select, uniques$y))

    by$x <- unname(uniques$x[by$x])
    by$y <- unname(uniques$y[by$y])

    sel_vars <- unique(c(uniques$x, uniques$y))
  }

  if (using) {
    cond <- build_sql("USING ", lapply(by$x, ident), con = con)
  } else {
    on <- dplyr:::sql_vector(paste0(sql_escape_ident(con, by$x), " = ", sql_escape_ident(con, by$y)),
      collapse = " AND ", parens = TRUE)
    cond <- build_sql("ON ", on, con = con)
  }

  from <- build_sql(
    'SELECT * FROM ',
    sql_subquery(con, x$query$sql), "\n\n",
    join, " JOIN \n\n" ,
    sql_subquery(con, y$query$sql), "\n\n",
    cond, con = con
  )
  attr(from, "vars") <- lapply(sel_vars, as.name)

  from
}

#' @rdname backend_sql
#' @export
sql_semi_join <- function(con, x, y, anti = FALSE, by = NULL, ...) {
  UseMethod("sql_semi_join")
}
#' @export
sql_semi_join.NetezzaConnection <- function(con, x, y, anti = FALSE, by = NULL, ...) {
  by <- dplyr:::common_by(by, x, y)

  left <- escape(ident("_LEFT"), con = con)
  right <- escape(ident("_RIGHT"), con = con)
  on <- dplyr:::sql_vector(paste0(
    left, ".", sql_escape_ident(con, by$x), " = ", right, ".", sql_escape_ident(con, by$y)),
    collapse = " AND ", parens = TRUE)

  from <- build_sql(
    'SELECT * FROM ', sql_subquery(con, x$query$sql, "_LEFT"), '\n\n',
    'WHERE ', if (anti) sql('NOT '), 'EXISTS (\n',
    '  SELECT 1 FROM ', sql_subquery(con, y$query$sql, "_RIGHT"), '\n',
    '  WHERE ', on, ')'
  )
  attr(from, "vars") <- x$select
  from
}

#' @rdname backend_sql
#' @export
sql_set_op <- function(con, x, y, method) {
  UseMethod("sql_set_op")
}
#' @export
sql_set_op.NetezzaConnection <- function(con, x, y, method) {
  sql <- build_sql(
    x$query$sql,
    "\n", sql(method), "\n",
    y$query$sql
  )
  attr(sql, "vars") <- x$select
  sql
}

db_data_type <- function(con, fields) UseMethod("db_data_type")

#' @export
db_data_type.NetezzaConnection <- function(con, fields, ...) {
    vapply(fields, dbDataType, FUN.VALUE=character(1))
}

dbDataType <- function(column, ...) {
    if (is.integer(column))
        return("INTEGER")
    if (is.numeric(column))
        return("DOUBLE")
    if (is.character(column) || is.factor(column)) {
        if (is.factor(column))
            column <- levels(column)
        len  <- max(nchar(column))
        type <- ifelse(any(Encoding(column) == "UTF-8"), "NVARCHAR", "VARCHAR")
        return(paste(type, "(", len, ")", sep=''))
    }
    stop("data type '", class(column), "' is not handled yet")
}

db_begin.NetezzaConnection <- function(con) {
    return(T)
}
db_rollback.NetezzaConnection <- function(con) {
    return(T)
}
db_commit.NetezzaConnection <- function(con) {
    return(T)
}

db_analyze.NetezzaConnection <- function(con, name) {
    return(T)
}

#' @export
db_create_table.src_netezza <- function(src, table, types, temporary=FALSE, ...)
{
  db_create_table(src$con, table, types, temporary, ...)
}

#' @export
db_create_table.NetezzaConnection <- function(con, table, types, temporary=FALSE, ...) {
    assertthat::assert_that(assertthat::is.string(table), is.character(types))
    field_names <- escape(ident(names(types)), collapse = NULL, con = con)
    fields <- dplyr:::sql_vector(paste0(field_names, " ", types), parens = TRUE,
                               collapse = ", ", con = con)
    sql <- build_sql("CREATE ", "TABLE ", ident(table), " ", fields, con = con)
    send_query(con@conn, sql)
    if(!db_has_table(con, table)) stop("Could not create table; are the data types specified in Netezza-compatible format?")
}

#' @export
db_create_table_from_file.src_netezza <- function(src, table, types, file.name, temporary=FALSE, ...)
{
  db_create_table_from_file(src$con, table, types, file.name, temporary, ...)
}

#' @export
db_create_table_from_file.NetezzaConnection <- function(con, table, types, file.name, temporary=FALSE, ...) {
    assertthat::assert_that(assertthat::is.string(table), is.character(types))
    field_names <- escape(ident(names(types)), collapse = NULL, con = con)
    fields <- dplyr:::sql_vector(paste0(field_names, " ", types), parens = TRUE,
                               collapse = ", ", con = con)
    fields_var <- dplyr:::sql_vector(field_names, parens = F,
                               collapse = ", ", con = con)
    sql <- build_sql("CREATE ", "TABLE ", ident(table), " AS SELECT ", fields_var,
                     " FROM EXTERNAL ", file.name, fields, " USING (delim ',' nullvalue '' QuotedValue DOUBLE remotesource 'ODBC')", con = con)
    send_query(con@conn, sql)
    if(!db_has_table(con, table)) stop("Could not create table; are the data types specified in Netezza-compatible format?")
}

#' @export
db_insert_into.NetezzaConnection <- function(con, table, values, ...) {
    if (nrow(values) == 0)
        return(NULL)
    cols <- lapply(values, escape, collapse = NULL, parens = FALSE, con = con)
    col_mat <- matrix(unlist(cols, use.names = FALSE), nrow = nrow(values))

    rows <- apply(col_mat, 1, paste0, collapse = ", ")
    values <- paste0("INSERT INTO ", ident(table), " VALUES ", "(", rows, ")", collapse = ";\n")
    sql <- build_sql(sql(values))
    send_query(con@conn, sql)
}

#' @export
copy_to.src_netezza <- function(dest, df, name = deparse(substitute(df)),
                                temporary=FALSE, replace=FALSE, ...) {
    assert_that(is.data.frame(df), is.string(name))

    name <- toupper(name)


    if (db_has_table(dest$con, name)) {
        if (replace) {
            warning(name, " already exists and replaced.")
            db_drop_table(dest$con, name)
        } else {
            stop(name, " already exists")
        }
    }

    types <- db_data_type(dest$con, df)
    names(types) <- names(df)

    if(temporary) warning("Copying to a temporary table is not supported. Writing to a permanent table.")


    tmpfilename = paste0("/tmp/", "dplyr_", name, ".csv")
    write.table(df, file=tmpfilename, sep=",", row.names=FALSE, col.names = FALSE, quote=T, na='')
    db_create_table_from_file.NetezzaConnection(dest$con, name, types, tmpfilename, temporary=FALSE)
    # file.remove(tmpfilename)
    tbl(dest, name)
}

#' @export
db_drop_table.src_netezza <- function(src, table, force = FALSE, ...) {
  db_drop_table(src$con, table, force, ...)
}

#' @export
db_drop_table.NetezzaConnection <- function(con, table, force = FALSE, ...) {
  assert_that(is.string(table))

  if(!db_has_table(con, table)) stop("Table does not exist in database.")

  sql <- build_sql("DROP TABLE ", escape(ident(table)), con = con)
  send_query(con@conn, sql)
}
