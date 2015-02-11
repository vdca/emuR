plot_level <- function(db, levelName, sessionName, bundleName, startSample=-1, endSample=-1){
  
  levelDef = get.levelDefinition(db$DBconfig, levelName)
  
  if(endSample == -1){
    print('read in audio file to get end sample')
  }
  
  # colors
  LEVELCOLOR = '#E7E7E7'
  STARTBOUNDARYCOLOR = 'red'
  ENDBOUNDARYCOLOR = 'blue'
  
  plot.new()
  text(10, 20, "0. 'blank'"   ,  adj=c(0,.5))
  plot(1, type="n", xlab="", ylab="", xlim=c(0, 100), ylim=c(0, 100), axes=F, ann=F)

  rect(10, 10, 90, 90, col='#E7E7E7')
  # draw leveName and type
  text(10, 50, paste0(levelName, "\n(", levelDef$type,")"))

  # start positions in percent (hardcoded for now)
  boundaryPositions = c(0, 10, 40, 80)
  labelPosition = diff(boundaryPositions)/2
  labels = c('x', 'y', 'z')
  
  
  lines(boundaryPositions, c(50,0,50,50), col = STARTBOUNDARYCOLOR)

}

# FOR DEVELOPMENT
library('testthat')
test_file('tests/testthat/test_plot_level.R')
