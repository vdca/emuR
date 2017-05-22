##' @export
import_speechRecorderPrompts = function (emuDBhandle,
                                         speechRecorderScript,
                                         attributeDefinition,
                                         sessionPattern = ".*",
                                         bundlePattern = ".*",
                                         matchItemcodeAndBundleName = NULL,
                                         matchPromptAndRecording = NULL,
                                         sequenceIndex = 1,
                                         verbose = T) {
  ##
  ## Restrict operations to bundles specified by sessionPattern/bundlePattern
  ##
  bundles = list_bundles(emuDBhandle)
  bundles = bundles %>% dplyr::filter(grepl(sessionPattern, session))
  bundles = bundles %>% dplyr::filter(grepl(bundlePattern, name))
  
  
  ##
  ## Read SpeechRecorder XML script
  ##
  script = xml2::read_xml(speechRecorderScript)
  
  ##
  ## Find recording nodes
  ##
  nodes = xml2::xml_find_all(script, "//recording")
  
  ##
  ## Loop over <recording> nodes and map them to bundles.
  ## This will generate a data frame containing all bundles that will be
  ## modified along with their future labels.
  ##
  modifications.list = lapply(nodes, function(currentNode) {
    itemcode = xml2::xml_attr(currentNode, "itemcode")
    
    # If the user does not specify a custom matching function,
    # we match each <recording> to each bundle whose name ends in the recording's
    # item code (because the file names that SpeechRecorder outputs are SpeakerCode_ItemCode).
    if (is.null(matchItemcodeAndBundleName)) {
      pattern = paste0(".*", itemcode)
      subset = bundles %>% dplyr::filter(grepl(pattern, name))
    } else {
      subset = matchItemcodeAndBundleName(itemcode, bundles)
    }
    
    # If the user does not specify a custom matching function,
    # we use the children of each <recording> to determine the label.
    if (is.null(matchPromptAndRecording)) {
      labelNode = xml2::xml_find_first(currentNode, './/recprompt/mediaitem')
      label = xml2::xml_text(labelNode)
    } else {
      label = matchPromptAndRecording(emuDBhandle, currentNode)
    }
    
    # Generate the data frame that specifies the new labels for those bundles
    # that have been matched to the currently inspected <recording> node.
    df = data.frame (
      session = subset$session,
      bundle = subset$name,
      attributeDefinition = rep(attributeDefinition, nrow(subset)),
      sequenceIndex = rep(sequenceIndex, nrow(subset)),
      label = rep(label, nrow(subset))
    )
    
    return(df)
  })
  
  ## Combine the individual data frames into a single one
  modifications = plyr::rbind.fill(modifications.list)
  
  ##
  ## Run the actual function
  ##
  change_labels(emuDBhandle, modifications, verbose)
}
