#A dplyr connector for the Netezza database.

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
#' @import RNetezza
#' @import dplyr
#' @export
src_netezza <- function(dsn, ...) {
    if (!requireNamespace("RNetezza", quietly = TRUE)) {
        stop("RNetezza package required to connect to Netezza", call. = FALSE)
    }
    con <- dbConnect(RNetezza::Netezza(), dsn=dsn, ...)
    info <- dbGetInfo(con)
    # src_sql("netezza", con = con, info = info, disco = dplyr:::db_disconnector(con, "netezza"))
    src_sql("netezza", con = con, info = info)
}

#' @export
tbl.src_netezza <- function(src, from, ...) {
    tbl_sql("netezza", src = src, from = from, ...)
}

# Describes the connection
#' @export
src_desc.src_netezza <- function(x) {
    info <- x$info
    paste0("Netezza ODBC [", info$sourcename, "]")
}

##' @export
#dim.tbl_netezza <- function(x) {
#    p <- x$query$ncol()
#    c(NA, p)
#}

### Translate

#' @export
sql_translate_env.NetezzaConnection <- function(x) {
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
###

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

db_data_type.NetezzaConnection <- function(con, fields, ...) {
    vapply(fields, dbDataType, FUN.VALUE=character(1))
}

### collect

#' @export
collect.tbl_netezza <- function(x, ...) {
  sql <- sql_render(x)
  res <- dbSendQuery(x$src$con, sql)
  on.exit(dbClearResult(res))

  out <- dbFetch(res, -1)
  grouped_df(out, groups(x))
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


#' @export
copy_to.src_netezza <- function(dest, df, name = deparse(substitute(df)),
            temporary=TRUE, indexes=NULL, analyze=TRUE, replace=FALSE, ...) {
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

    tmpfilename = paste0("/tmp/", "dplyr_", name, ".csv")
    write.table(df, file=tmpfilename, sep=",", row.names=FALSE, col.names = FALSE, quote=T, na='')

    field_names <- escape(ident(names(types)), collapse = NULL, con = dest$con)
    fields <- dplyr:::sql_vector(paste0(field_names, " ", types), parens = TRUE,
                               collapse = ", ", con = dest$con)
    fields_var <- dplyr:::sql_vector(field_names, parens = F,
                               collapse = ", ", con = dest$con)
    sql <- build_sql("CREATE ", if (temporary) sql("TEMPORARY "), "TABLE ",
                     ident(name), " AS SELECT ", fields_var,
                     " FROM EXTERNAL ", tmpfilename, fields,
                     " USING (delim ',' nullvalue '' QuotedValue DOUBLE remotesource 'ODBC')",
                con = dest$con)
    r <- dbGetQuery(dest$con, sql)

    if(!db_has_table(dest$con, name)) {
        stop("Could not create table; are the data types specified in Netezza-compatible format?")
    }
    file.remove(tmpfilename)
    tbl(dest, name)
}
