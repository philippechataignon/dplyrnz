library(nycflights13)
s <- src_netezza('NZN')
db_drop_table(s, 'AIRLINES')
db_drop_table(s, 'AIRPORTS')
db_drop_table(s, 'FLIGHTS')
db_drop_table(s, 'PLANES')
db_drop_table(s, 'WEATHER')
copy_nycflights13(s, index=NULL)

fl <- tbl(s, 'FLIGHTS')
fl  %>% filter(origin %in% c('LGA', 'EWR')) %>% tally() %>% collect()  %>% as.numeric() -> eff2
test_that("filter OK",	{
    expect_equal(eff2, 225497)
})

appa <- data.frame(ok=c('EWR', 'LGA'), test=1)
fl %>% semi_join(appa, by=c('origin' = 'ok'), copy=T) %>% tally()  %>% collect()  %>% as.numeric() -> eff1

test_that("semi_join with copy OK",	{
    expect_equal(eff1, 225497)
})