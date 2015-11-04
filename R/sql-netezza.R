#' @import assertthat
#' @import RODBC
#' @import dplyr

### Select

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
sql_subquery <- function(con, sql, name = random_table_name(), ...) {
  UseMethod("sql_subquery")
}

#' @export
sql_subquery.NetezzaConnection <- function(con, sql, name = unique_name(), ...) {
  if (is.ident(sql)) return(sql)

  build_sql("(", sql, ") AS ", ident(name), con = con)
}

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

### escape

#' @export
sql_escape_ident.NetezzaConnection <- function(con, x) {
    sql_quote(x, '"')
}
#' @export
sql_escape_string.NetezzaConnection <- function(con, x) {
    sql_quote(x, "'")
}

ident_schema_table <- function(tablename) {
    escape(ident(tablename))
}


