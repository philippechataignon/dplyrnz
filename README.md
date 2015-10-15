#  dplyrnz : dplyr SQL backend for Netezza

R package that extends dplyr and provides SQL backend to Netezza (tested on V6.0.8).

This package wraps Netezza ODBC driver and use RODBC package.

#Installation

Install via github:

```R
install.packages('devtools')
devtools::install_github('philippechataignon/dplyrnz')
```

#Usage

```R
library(dplyrnz)

nzr <- src_netezza('DSN')
table <- tbl(nzr, "TABLE_NAME")
```

#Credits

*  [dplyr](https://github.com/hadley/dplyr)
