#' @import assertthat
#' @import RODBC
#' @import dplyr


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
    if(!db_has_table(con, table)) {
        stop("Could not create table; are the data types specified in Netezza-compatible format?")
    }
}

db_create_table_from_file <- function(con, table, types, file.name, temporary=FALSE, ...) {
    assertthat::assert_that(assertthat::is.string(table), is.character(types))
    field_names <- escape(ident(names(types)), collapse = NULL, con = con)
    fields <- dplyr:::sql_vector(paste0(field_names, " ", types), parens = TRUE,
                               collapse = ", ", con = con)
    fields_var <- dplyr:::sql_vector(field_names, parens = F,
                               collapse = ", ", con = con)
    sql <- build_sql("CREATE ", "TABLE ", ident(table), " AS SELECT ", fields_var,
                     " FROM EXTERNAL ", file.name, fields,
                     " USING (delim ',' nullvalue '' QuotedValue DOUBLE remotesource 'ODBC')",
                con = con)
    send_query(con@conn, sql)

    if(!db_has_table(con, table)) {
        stop("Could not create table; are the data types specified in Netezza-compatible format?")
    }
}

#' @export
db_insert_into.NetezzaConnection <- function(con, table, values, ...) {
    # very slow : only 1 row at a time
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
            temporary=TRUE, indexes=NULL, analyze=TRUE, replace=FALSE, ...) {

    assertthat::assert_that(is.data.frame(df), assertthat::is.string(name))
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

    if(temporary) {
        name <- paste0('TMP_', name)
        warning("Copying to a temporary table is not supported yet.\n Writing to permanent table : ", name)
    }

    tmpfilename = paste0("/tmp/", "dplyr_", name, ".csv")
    write.table(df, file=tmpfilename, sep=",", row.names=FALSE, col.names = FALSE, quote=T, na='')
    db_create_table_from_file(dest$con, name, types, tmpfilename, temporary=FALSE)
    file.remove(tmpfilename)
    if (analyze) {
        db_analyze(dest$con, name)
    }
    tbl(dest, name)
}

#' @export
db_drop_table.src_netezza <- function(src, table, ...) {
  db_drop_table(src$con, table, ...)
}

#' @export
db_drop_table.NetezzaConnection <- function(con, table, ...) {
  assertthat::assert_that(assertthat::is.string(table))
  if(!db_has_table(con, table))
    stop("Table does not exist in database.")

  sql <- build_sql("DROP TABLE ", escape(ident(table)), con = con)
  send_query(con@conn, sql)
  invisible(0)
}
