dots <- function(...) {
  eval(substitute(alist(...)))
}

named_dots <- function(...) {
  auto_name(dots(...))
}

auto_names <- function(x) {
  nms <- names2(x)
  missing <- nms == ""
  if (all(!missing)) return(nms)

  deparse2 <- function(x) paste(deparse(x, 500L), collapse = "")
  defaults <- vapply(x[missing], deparse2, character(1), USE.NAMES = FALSE)

  nms[missing] <- defaults
  nms
}

deparse_trunc <- function(x, width = getOption("width")) {
  text <- deparse(x, width.cutoff = width)
  if (length(text) == 1 && nchar(text) < width) return(text)

  paste0(substr(text[1], 1, width - 3), "...")
}

auto_name <- function(x) {
  names(x) <- auto_names(x)
  x
}

is.lang <- function(x) {
  is.name(x) || is.atomic(x) || is.call(x)
}
is.lang.list <- function(x) {
  if (is.null(x)) return(TRUE)

  is.list(x) && all_apply(x, is.lang)
}
only_has_names <- function(x, nms) {
  all(names(x) %in% nms)
}

all_apply <- function(xs, f) {
  for (x in xs) {
    if (!f(x)) return(FALSE)
  }
  TRUE
}
any_apply <- function(xs, f) {
  for (x in xs) {
    if (f(x)) return(TRUE)
  }
  FALSE
}

drop_last <- function(x) {
  if (length(x) <= 1L) return(NULL)
  x[-length(x)]
}

compact <- function(x) Filter(Negate(is.null), x)

names2 <- function(x) {
  names(x) %||% rep("", length(x))
}

"%||%" <- function(x, y) if(is.null(x)) y else x

is.wholenumber <- function(x, tol = .Machine$double.eps ^ 0.5) {
  abs(x - round(x)) < tol
}

as_df <- function(x) {
  class(x) <- "data.frame"
  attr(x, "row.names") <- c(NA_integer_, -length(x[[1]]))

  x
}

deparse_all <- function(x) {
  deparse2 <- function(x) paste(deparse(x, width.cutoff = 500L), collapse = "")
  vapply(x, deparse2, FUN.VALUE = character(1))
}

commas <- function(...) paste0(..., collapse = ", ")

in_travis <- function() identical(Sys.getenv("TRAVIS"), "true")

named <- function(...) {
  x <- c(...)

  missing_names <- names2(x) == ""
  names(x)[missing_names] <- x[missing_names]

  x
}

unique_name <- local({
  i <- 0

  function() {
    i <<- i + 1
    paste0("zzz", i)
  }
})

isFALSE <- function(x) identical(x, FALSE)

substitute_q <- function(x, env) {
  call <- substitute(substitute(x, env), list(x = x))
  eval(call)
}


succeeds <- function(x, quiet = FALSE) {
  tryCatch({x; TRUE}, error = function(e) {
    if (!quiet) message("Error: ", e$message)
    FALSE
  })
}

is_1d <- function(x) {
  # is.atomic() is TRUE for atomic vectors AND NULL!
  # dimension check is for matrices and data.frames
  ((is.atomic(x) && !is.null(x)) || is.list(x)) && length(dim(x)) <= 1
}


unique_names <- function(x_names, y_names, by, x_suffix = ".x", y_suffix = ".y") {
    common <- setdiff(intersect(x_names, y_names), by)
    if (length(common) == 0) return(NULL)

    x_match <- match(common, x_names)
    x_new <- x_names
    x_new[x_match] <- paste0(x_names[x_match], x_suffix)

    y_match <- match(common, y_names)
    y_new <- y_names
    y_new[y_match] <- paste0(y_names[y_match], y_suffix)

    list(x = setNames(x_new, x_names), y = setNames(y_new, y_names))
}


