##' Send emuDB to the doctor
##' 
##' Does a number of checks on a loaded emuDB and returns 
##' the result of the in the form of an error object
##' @param dbName name of loaded emuDB
##' @param dbUUID dbUUID optional UUID of emuDB
##' @export
doctor <- function(dbName = NULL, dbUUID = NULL){
  #########################
  # get dbObj
  dbUUID = get_emuDB_UUID(dbName = dbName, dbUUID = dbUUID)
  dbObj = .load.emuDB.DBI(uuid = dbUUID)
  
  diagnosis = list()
  
  #########################
  # check _annot.json files
  afps = list.files(dbObj$basePath, pattern = "_annot.json$", recursive = T, full.names = T)
  
  diagnosis$annotJSON = list()
  diagnosis$annotJSON$overview = data.frame(path=afps, 
                                            foundProblem=logical(length(afps)), 
                                            stringsAsFactors = F)
  
  diagnosis$annotJSON$errors = list()
  diagnosis$annotJSON$warnings = list()
  
  for(i in 1:length(afps)){
    annotation = jsonlite::fromJSON(readLines(afps[i]), simplifyVector=F)
    annotDFs = annotJSONtoListOfDataFrames(annotation)
    links = do.call(rbind.data.frame, annotation$links)
    
    wavPath = file.path(dirname(afps[i]), annotation$annotates)
    
    diagnosis$annotJSON$errors[[i]] = list()
    diagnosis$annotJSON$warnings[[i]] = list()
    errorCounter = 1
    warningCounter = 1
    
    ado = read.AsspDataObj(wavPath, begin = 0, end = 25)
    # check for bad sampleRate 
    if(attributes(ado)$sampleRate != annotation$sampleRate){
      diagnosis$annotJSON$overview$foundProblem[i] = TRUE
      diagnosis$annotJSON$errors[[i]][[errorCounter]] = list(message = "sampleRate in annotJSON not the same as in wav")
      errorCounter = errorCounter + 1
    }
    
    # check only correct levels are present
    for(l in annotation$levels){
      lDef = get.levelDefinition(DBconfig = dbObj$DBconfig, l$name)
      # get linkDefs (where cur. level is superLevel)
      linkDefs = list()
      for(ld in dbObj$DBconfig$linkDefinitions){
        if(ld$superlevelName == l$name){
          linkDefs[[length(linkDefs) + 1]] = ld
        }
      }
      
      
      # check if level has levelDef
      if(is.null(lDef)){
        diagnosis$annotJSON$overview$foundProblem[i] = TRUE
        diagnosis$annotJSON$errors[[i]][[errorCounter]] = list(message = "contains level that is has no levelDefintion in DBconfig",
                                                               levelName = l$name)
        errorCounter = errorCounter + 1
      }
      # check levelType
      if(l$type != lDef$type){
        diagnosis$annotJSON$overview$foundProblem[i] = TRUE
        diagnosis$annotJSON$errors[[i]][[errorCounter]] = list(message = "contains level that does not have the same type as levelDefintion in DBconfig",
                                                               levelName = l$name)
        errorCounter = errorCounter + 1
        
      }
      
      #
      for(j in 1:length(l$items)){
        
        ################################################
        # check if sequence is correct and items are order correctly 
        # and flush with the neighbours
        
        if(l$type == "SEGMENT"){
          if(j > 1){
            # check previous
            if(l$items[[j - 1]]$sampleStart + l$items[[j - 1]]$sampleDur + 1 != l$items[[j]]$sampleStart){
              diagnosis$annotJSON$overview$foundProblem[i] = TRUE
              diagnosis$annotJSON$errors[[i]][[errorCounter]] = list(message = "Item of type SEGMENT not flush with left neighbour",
                                                                     itemID = l$items[[j]]$id)
              errorCounter = errorCounter + 1
            }
          }
        }
        else if(l$type == "EVENT"){
          if(j > 1){
            # check previous
            if(l$items[[j - 1]]$samplePoint > l$items[[j]]$samplePoint){
              diagnosis$annotJSON$overview$foundProblem[i] = TRUE
              diagnosis$annotJSON$errors[[i]][[errorCounter]] = list(message = "Item of type EVENT not in correct sequence",
                                                                     itemID = l$items[[j]]$id)
              errorCounter = errorCounter + 1
            }
          }
        }
        
        ###########################################
        # check if links are ok
        iLinks = annotDFs$links[annotDFs$links$fromID == l$items[[j]]$id, ]
        
        parent = annotDFs$items[annotDFs$items$itemID == l$items[[j]]$id, ]
        kids = annotDFs$items[annotDFs$items$itemID %in% iLinks$toID, ]
        if(nrow(kids) > 0){
          for(krow in 1:nrow(kids)){
            # check that links adhere to levelDef
            hasLD = F
            foundLD = NULL
            for(ld in dbObj$DBconfig$linkDefinitions){
              if(ld$superlevelName == parent$level && ld$sublevelName == kids[krow, ]$level){
                hasLD = T
                foundLD = ld
                break
              }
            }
            if(!hasLD){
              diagnosis$annotJSON$overview$foundProblem[i] = TRUE
              diagnosis$annotJSON$errors[[i]][[errorCounter]] = list(message = "Item found containing links to children that don't match any linkDefinition",
                                                                     itemID = l$items[[j]]$id)
              errorCounter = errorCounter + 1
            }
            # check that links adhere to linkDefinition$type
            for(linkDef in linkDefs){
              kLinksUp = annotDFs$links[annotDFs$links$toID == kids[krow, ]$itemID, ]
              curParents = annotDFs$items[annotDFs$items$itemID %in% kLinksUp$fromID, ]
              curParents = curParents[curParents$level == linkDef$superlevelName,]
              if(ld$type == "ONE_TO_MANY" && nrow(curParents) > 1){
                diagnosis$annotJSON$overview$foundProblem[i] = TRUE
                diagnosis$annotJSON$errors[[i]][[errorCounter]] = list(message = "Item found containing links to parents that don't match the linkDefinition$type",
                                                                       itemID = l$items[[j]]$id,
                                                                       linkDefType = "ONE_TO_MANY")
                errorCounter = errorCounter + 1
              }
              if(ld$type == "ONE_TO_ONE" && nrow(curParents) != 1){
                diagnosis$annotJSON$overview$foundProblem[i] = TRUE
                diagnosis$annotJSON$errors[[i]][[errorCounter]] = list(message = "Item found containing links to parents that don't match the linkDefinition$type",
                                                                       itemID = l$items[[j]]$id,
                                                                       linkDefType = "ONE_TO_ONE")
                errorCounter = errorCounter + 1
              }
            }
            
          }
        }
        
        for(linkDef in linkDefs){
          curKids = kids[kids$level == linkDef$sublevelName,]
          print(curKids)
          leftestKid = curKids[curKids$seqIdx == min(curKids$seqIdx),]
          rightestKid = curKids[curKids$seqIdx == max(curKids$seqIdx),]
          
          for(curItemNr in 1:length(l$items)){
            linksCurItemNr = annotDFs$links[annotDFs$links$fromID == l$items[[curItemNr]]$id, ]
            parentCurItemNr = annotDFs$items[annotDFs$items$itemID == l$items[[curItemNr]]$id, ]
            kidsCurItemNr = annotDFs$items[annotDFs$items$itemID %in% linksCurItemNr$toID, ]
          }
        }
        
        
      } # for loop
      
      
      
    }
    
  }
  
  return(diagnosis)
  
}

############################
# FOR DEVELOPMENT
# dia = doctor(n)
# print(dia$annotJSON$overview)
