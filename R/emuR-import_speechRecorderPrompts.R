##' @export
import_speechRecorderPrompts = function (emuDBhandle,
                                         speechRecorderScript,
                                         attributeDefinition,
                                         sequenceIndex = 1,
                                         sessionPattern = ".*",
                                         bundlePattern = ".*",
                                         correspondenceFunction = NULL,
                                         verbose = T) {
  ##
  ## Restrict operations to bundles specified by sessionPattern/bundlePattern
  ##
  bundleList = list_bundles(emuDBhandle)
  bundleList = bundleList %>% dplyr::filter(grepl(sessionPattern, session))
  bundleList = bundleList %>% dplyr::filter(grepl(bundlePattern, name))
  
  
  ##
  ## Read SpeechRecorder XML script
  ##
  script = xml2::read_xml(speechRecorderScript)
  
  ##
  ## Find recording nodes
  ##
  nodes = xml2::xml_find_all(script, "//recording")
  
  ##
  ## Generate a data frame for each recording node and concatentate the data frames
  ##
  labels = plyr::rbind.fill(
    lapply(
      X = nodes,
      FUN = generateLabelList,
      bundleList = bundleList,
      correspondenceFunction = correspondenceFunction,
      attributeDefinition = attributeDefinition,
      sequenceIndex = sequenceIndex
    )
  )
  
  ##
  ## Run the actual function
  ##
  change_labels(emuDBhandle, labels, verbose)
}

##
## Given a <recording> node and a list of bundles, see if any of the bundles are affected by the recording
## and return a data frame
##
## @ todo do not use the term "bundle list" here - it is slightly misleading - maybe "search space" instead?
##
generateLabelList = function (currentNode,
                              bundleList,
                              correspondenceFunction,
                              attributeDefinition,
                              sequenceIndex) {
  itemcode = xml2::xml_attr(currentNode, "itemcode")
  
  ## @TODO speechrecorder concatenates itemcode and speaker - is there a better approach than mine to accomodating this?
  pattern = paste0(".*", itemcode)
  subset = bundleList %>% dplyr::filter(grepl(pattern, name))
  
  
  if (nrow(subset) > 0) {
    if (is.null(correspondenceFunction)) {
      label = xml2::xml_text(xml2::xml_find_first(currentNode, './/recprompt/mediaitem'))
    } else {
      label = correspondenceFunction(emuDBhandle, currentNode)
    }
  } else {
    label = ""
  }
  
  df = data.frame (
    session = subset$session,
    bundle = subset$name,
    attributeDefinition = rep(attributeDefinition, nrow(subset)),
    sequenceIndex = rep(sequenceIndex, nrow(subset)),
    label = rep(label, nrow(subset))
  )
  
  return(df)
}
