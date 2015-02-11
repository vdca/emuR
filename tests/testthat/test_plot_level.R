##' testthat tests for plot_level
##'
##' @author Raphael Winkelmann
context("testing plot_level functions")

path2ae = system.file("extdata/emu/DBs/ae/", package = "emuR")

ae = load.emuDB(path2ae, verbose = F)

##############################
test_that("bad calls to plot_level", {
  
  
})


##############################
test_that("test1", {
  
  plot_level(ae, 'Phonetic')
  
})
