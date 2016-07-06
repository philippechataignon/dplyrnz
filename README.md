#  dplyrnz : dplyr SQL backend for Netezza

R package that extends dplyr and provides SQL backend to Netezza (tested on V6.0.8).

This is a complete rewrite for compatibility with the 0.5.0 release
of [dplyr](https://github.com/hadley/dplyr).

[dplyr] needs now a DBI compliant database backend. So,
the package [RNetezza] is a new dependance for dplyrnz. 

#Installation

Install via github:

```R
install.packages('devtools')
devtools::install_github('philippechataignon/RNetezza')
devtools::install_github('philippechataignon/dplyrnz')
```

#Usage

```R
library(dplyrnz)

nzr <- src_netezza('DSN')
table <- tbl(nzr, "TABLE_NAME")
```

#Credits

*  [dplyr]
*  [RNetezza]

[dplyr]: https://github.com/hadley/dplyr
[RNetezza]: https://github.com/philippechataignon/RNetezza
