#' @import assertthat
#' @import RODBC
#' @import dplyr

### Select

#' @export
sql_set_op.NetezzaConnection <- dplyr:::sql_set_op.DBIConnection

#' @export
sql_join.NetezzaConnection <- dplyr:::sql_join.DBIConnection

#' @export
sql_semi_join.NetezzaConnection <- dplyr:::sql_semi_join.DBIConnection

#' @export
sql_subquery.NetezzaConnection <- dplyr:::sql_subquery.DBIConnection

#' @export
sql_select.NetezzaConnection <- dplyr:::sql_select.DBIConnection

###

db_data_type <- function(con, fields) UseMethod("db_data_type")

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

db_create_index <- function(con, name) {
    warning("No indexes in Netezza")
    return(T)
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
