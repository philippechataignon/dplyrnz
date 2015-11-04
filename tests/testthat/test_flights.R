library(nycflights13)
s <- src_netezza('NZN')
# copy_to(s, flights, replace=T)

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