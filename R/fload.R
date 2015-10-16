# (Fast) insert of bundle into SQL tables
finsert_bundle <- function(dbName, annotFilePath, sessionName, 
                           bundleName, dbUUID = NULL) {
  
  dbUUID = get_emuDB_UUID(dbName, dbUUID)
  con = get_emuDBcon(dbUUID)
  
  bundle = jsonlite::fromJSON(normalizePath(annotFilePath), simplifyVector = FALSE)
  listOfDfs = annotJSONtoListOfDataFrames(bundle)
  
  # calculate MD5 sum of bundle annotJSON      
  MD5annotJSON = md5sum(normalizePath(annotFilePath))
  names(MD5annotJSON) = NULL
  
  # insert bundle table entry
  dbWriteTable(con, "bundle", data.frame(db_uuid = dbUUID, 
                                         session = sessionName,
                                         name = bundleName,
                                         annotates = bundle$annotates,
                                         sampleRate = bundle$sampleRate,
                                         mediaFilePath = bundle$annotates, # SIC will have to be deleted once portable cache is merged into master
                                         MD5annotJSON = MD5annotJSON), append = T)
  # insert items table entries
  listOfDfs$items = data.frame(db_uuid = rep(dbUUID, nrow(listOfDfs$items)), 
                               session = rep(sessionName, nrow(listOfDfs$items)),
                               bundle = rep(bundleName, nrow(listOfDfs$items)),
                               listOfDfs$items)
  
  dbWriteTable(con, "items", listOfDfs$items, append = T)
  
  # insert labels table entries
  listOfDfs$labels =  data.frame(db_uuid = rep(dbUUID, nrow(listOfDfs$labels)), 
                                 session = rep(sessionName, nrow(listOfDfs$labels)),
                                 bundle = rep(bundleName, nrow(listOfDfs$labels)),
                                 listOfDfs$labels)
  
  dbWriteTable(con, "labels", listOfDfs$labels, append = T)
  
  # insert links table entries
  listOfDfs$links =  data.frame(db_uuid = rep(dbUUID, nrow(listOfDfs$links)), 
                                session = rep(sessionName, nrow(listOfDfs$links)),
                                bundle = rep(bundleName, nrow(listOfDfs$links)),
                                listOfDfs$links,
                                label = rep(NA, nrow(listOfDfs$links)))
  
  dbWriteTable(con, "links", listOfDfs$links, append = T)
  
}

##' (Fast) Load emuDB
##' 
##' @param databaseDir directory of the emuDB
##' @param inMemoryCache cache the loaded DB in memory
##' @param verbose be verbose
##' @return name of emuDB
##' @import jsonlite
##' @export
##' @keywords emuDB database schema Emu
##' @examples
##' \dontrun{
##' ## Load database 'ae' in directory /homes/mylogin/EMUnew/ae
##' 
##' dbName=load_emuDB(/homes/mylogin/EMUnew/ae")
##' 
##' }
fload_emuDB <- function(databaseDir, inMemoryCache = TRUE, verbose=TRUE){
  progress=0
  
  # check database dir
  if(!file.exists(databaseDir)){
    stop("Database dir ",databaseDir," does not exist!")
  }
  dbDirInfo=file.info(databaseDir)
  if(!dbDirInfo[['isdir']]){
    stop(databaseDir," exists, but is not a directory.")
  }
  
  
  # load db schema file
  dbCfgPattern=paste0('.*',database.schema.suffix,'$')
  dbCfgFiles=list.files(path=databaseDir,dbCfgPattern)
  dbCfgFileCount=length(dbCfgFiles)
  if(dbCfgFileCount==0){
    stop("Could not find global DB config JSON file (regex pattern: ",dbCfgPattern,") in ",databaseDir)
  }
  if(dbCfgFileCount>1){
    stop("Found multiple global DB config JSON files (regex pattern: ",dbCfgPattern,") in ",databaseDir)
  }
  
  dbCfgPath=file.path(databaseDir,dbCfgFiles[[1]])
  if(!file.exists(dbCfgPath)){
    stop("Could not find database info file: ",dbCfgPath,"\n")
  }
  # calc. md5 sum
  MD5DBconfigJSON = md5sum(normalizePath(dbCfgPath))
  # load DBconfig
  schema=load.emuDB.DBconfig(dbCfgPath)
  # set transient values
  schema=.update.transient.schema.values(schema)
  # create db object
  db=create.database(name = schema[['name']], basePath = normalizePath(databaseDir),DBconfig = schema)
  
  dbUUID = schema$UUID
  
  # add new connection
  if(inMemoryCache){
    con = dbConnect(RSQLite::SQLite(), ":memory:")
    con = add_emuDBcon(con)
  }else{
    dbPath = file.path(normalizePath(databaseDir), paste0(schema$name, database.cache.suffix))
    con = dbConnect(RSQLite::SQLite(), dbPath)
    con = add_emuDBcon(con, dbPath)
  }
  
  # check if database is already loaded (if so perform update_cache)
  dbsDf=dbGetQuery(con,paste0("SELECT * FROM emuDB WHERE uuid='",schema[['UUID']],"'"))
  if(nrow(dbsDf)>0){
    # always update basePat (SIC!!! This will be removed once the portable-cache branch is merged)
    dbGetQuery(con, paste0("UPDATE emuDB SET basePath = '", normalizePath(databaseDir) , "' ",
                           "WHERE uuid = '", dbUUID, "'"))
    
    fupdate_cache(schema[['name']], dbUUID = dbUUID, verbose = verbose)
    return(schema$name)
  }
  
  .store.emuDB.DBI(con, db, MD5DBconfigJSON)
  
  # list sessions
  sessPattern=paste0('^.*',session.suffix,'$')
  sessDirs=dir(databaseDir,pattern=sessPattern)
  
  # calculate bundle count
  bundleCount=0
  for(sd in sessDirs){
    absSd=file.path(databaseDir,sd)
    bundleDirs=dir(absSd,pattern=paste0('.*',bundle.dir.suffix,'$'))
    bundleCount=bundleCount+length(bundleDirs)
  }
  
  # create progress bar
  pMax=bundleCount
  if(pMax==0){
    pMax=1
  }
  if(verbose){ 
    cat(paste0("INFO: Loading EMU database from ",databaseDir,"... (",bundleCount ," bundles found)\n"))
    pb=txtProgressBar(min=0L,max=pMax,style=3)
    setTxtProgressBar(pb,progress)
  }
  
  
  for(sd in sessDirs){
    sessionName=gsub(pattern = paste0(session.suffix,'$'),replacement = '',x = sd)
    bundles=list()
    absSd=file.path(databaseDir,sd)
    bundleDirs=dir(absSd,pattern=paste0('.*',bundle.dir.suffix,'$'))
    # insert session table entry
    dbWriteTable(con, "session", data.frame(db_uuid = db$DBconfig$UUID, 
                                            name = sessionName), append = T)
    
    # bundles
    for(bd in bundleDirs){
      
      absBd=file.path(absSd,bd)
      
      bName=gsub(paste0(bundle.dir.suffix,'$'),'',bd)
      
      annotFilePath = file.path(absBd, paste0(bName, bundle.annotation.suffix, '.json'))
      
      finsert_bundle(schema$name, annotFilePath, sessionName, bName)
      
      # increase progress bar  
      progress=progress+1L
      if(verbose){
        setTxtProgressBar(pb,progress)
      }
      
    }
  }
  
  # build redundat links and calc positions
  cat("\nbuilding redundant links and position of links... (this may take a while)\n")
  build.redundant.links.all(db)
  calculate.postions.of.links(dbUUID)
  
  return(schema$name)
  
}


##' (Fast) Update cache of emuDB
##' 
##' Updates sqlite cache of loaded emuDB. This can and should be used
##' to update changes to precached/loaded DBs as it only updates the deltas 
##' in the cache which is considerably faster than reloading and therefore 
##' recacheing the entire DB.
##' @param dbName name of EMU database
##' @param dbUUID optional UUID of emuDB
##' @param verbose display infos
##' @author Raphael Winkelmann
##' @export
##' @keywords emuDB database Emu 
fupdate_cache <- function(dbName, dbUUID=NULL, verbose = TRUE){
  #########################
  # get dbObj
  dbUUID = get_emuDB_UUID(dbName = dbName, dbUUID = dbUUID)
  dbObj = .load.emuDB.DBI(uuid = dbUUID)
  
  ######################################
  # check if DBconfig needs reload
  
  dbCfgPattern=paste0('.*',database.schema.suffix,'$')
  dbCfgFiles=list.files(path=dbObj$basePath,dbCfgPattern)
  dbCfgFileCount=length(dbCfgFiles)
  if(dbCfgFileCount==0){
    stop("Could not find global DB config JSON file (regex pattern: ",dbCfgPattern,") in ",dbObj$basePath)
  }
  if(dbCfgFileCount>1){
    stop("Found multiple global DB config JSON files (regex pattern: ",dbCfgPattern,") in ",dbObj$basePath)
  }
  
  dbCfgPath=file.path(dbObj$basePath,dbCfgFiles[[1]])
  if(!file.exists(dbCfgPath)){
    stop("Could not find database info file: ",dbCfgPath,"\n")
  }
  # calc. new md5 sum
  new.MD5DBconfigJSON = md5sum(normalizePath(dbCfgPath))[[1]]
  
  old.MD5DBconfigJSON = dbGetQuery(get_emuDBcon(dbUUID), paste0("SELECT MD5DBconfigJSON FROM emuDB WHERE uuid='", dbUUID, "'"))[[1]]
  if(is.na(old.MD5DBconfigJSON) | old.MD5DBconfigJSON != new.MD5DBconfigJSON){
    if(verbose){
      print('Reloading _DBconfig.json ...')
    }
    # load DBconfig
    schema=load.emuDB.DBconfig(dbCfgPath)
    # set transient values
    schema=.update.transient.schema.values(schema)
    # create db object
    db=create.database(name = schema[['name']],basePath = normalizePath(dbObj$basePath),DBconfig = schema)
    
    dbCfgJSON=jsonlite::toJSON(db$DBconfig, auto_unbox=TRUE, force=TRUE, pretty=TRUE)
    # update entry
    dbGetQuery(get_emuDBcon(dbUUID), paste0("UPDATE emuDB SET DBconfigJSON = '", dbCfgJSON , "', ",
                                            "MD5DBconfigJSON = '", new.MD5DBconfigJSON, "' ",
                                            "WHERE uuid = '", dbUUID, "'"))
    
  }
  
  ######################################
  # check which _annot.json files need reloading
  bt = dbReadTable(get_emuDBcon(dbUUID), "bundle")
  bndls = list_bundles(dbName = dbName, dbUUID = dbUUID)
  
  sesPattern=paste0('.*', session.suffix, '$')
  sesPaths=list.files(path=dbObj$basePath, sesPattern)
  
  curSes = list_sessions(dbName, dbUUID=dbUUID)
  curSes["found"] = F
  
  curBndls = list_bundles(dbName, dbUUID = dbUUID)
  curBndls["found"] = F
  
  progress = 0
  
  if(verbose){
    bndlPaths = list.dirs(dbObj$basePath, recursive=T)
    nrOfBndls = length(bndlPaths[grepl(paste0(".", "*", session.suffix, ".*", bundle.dir.suffix, "$"), bndlPaths) == T])
    cat("INFO: Checking if cache needs update for ", nrOfBndls, " bundles...\n")
    pb = txtProgressBar(min = 0, max = nrOfBndls, initial = progress, style=3)
    setTxtProgressBar(pb, progress)
  }
  
  for(s in sesPaths){
    sn = gsub(session.suffix,"", s)
    bndlPattern=paste0('.*', bundle.dir.suffix, '$')
    bndlPaths=list.files(path=file.path(dbObj$basePath, s), bndlPattern)
    
    if(!sn %in% curSes$name){
      dbGetQuery(get_emuDBcon(dbUUID), paste0("INSERT INTO session VALUES ('", dbUUID, "', '", sn, "')"))
    }
    
    # mark as found
    curSes$found[curSes$name == sn] = T
    
    for(b in bndlPaths){
      
      bn = gsub(bundle.dir.suffix,"", b)
      # mark as found
      curBndls$found[curBndls$session == sn & curBndls$name == bn] = T
      
      # calc. new md5 sum
      annotFilePath = file.path(dbObj$basePath, s, b, paste0(bn, bundle.annotation.suffix, ".json"))
      new.MD5annotJSON = md5sum(normalizePath(annotFilePath))
      
      old.MD5annotJSON = dbGetQuery(get_emuDBcon(dbUUID), paste0("SELECT MD5annotJSON FROM bundle WHERE ",
                                                                 "db_uuid='", dbUUID, "' AND ",
                                                                 "session='", sn, "' AND ",
                                                                 "name='", bn, "'"))$MD5annotJSON
      
      # set to empty string if NA or empty character()
      if(length(old.MD5annotJSON) == 0){
        old.MD5annotJSON = ""
      }
      if(is.na(old.MD5annotJSON)){
        old.MD5annotJSON = ""
      }
      
      
      if(old.MD5annotJSON != new.MD5annotJSON){
        if(verbose){
          print(paste0("Reloading _annot.json for bundle in session : '", sn,"' with name: '", bn, "'"))
        }
        
        # delete bundle table entry
        qRes = dbGetQuery(get_emuDBcon(dbUUID), paste0("DELETE FROM bundle WHERE ",
                                                       "db_uuid = '", dbUUID, "' AND ", 
                                                       "session = '", sn, "' AND ", 
                                                       "name = '", bn, "'"))
        
        # delete old items/label/links entries
        .remove.bundle.annot.DBI(dbUUID=dbUUID,bundle=list(name=bn, session=sn))
        # and store new values in tables
        finsert_bundle(dbName, annotFilePath, sessionName = sn, bundleName = bn, dbUUID = dbUUID)
        
        # only build redunant links if non-empty bundle
        qRes = dbGetQuery(get_emuDBcon(dbUUID), paste0("SELECT * FROM items WHERE ",
                                                       "db_uuid = '", dbUUID, "' AND ", 
                                                       "session = '", sn, "' AND ", 
                                                       "bundle = '", bn, "'"))
        if(nrow(qRes) > 0){
          build.redundant.links.all(database = dbObj,sessionName=sn,bundleName=bn)
        }
        
        calculate.postions.of.links(dbUUID)
        
      }
      
      # update pb
      progress = progress + 1
      if(verbose){
        setTxtProgressBar(pb, progress)
      }
      
    }
    
    
  }
  
  if(verbose){
    cat("\n")
  }
  
  # remove bundles from cache that where not found
  for(i in 1:nrow(curBndls)){
    if(!curBndls[i,]$found){
      hackBndl = list(name = curBndls[i,]$name, session = curBndls[i,]$session)
      .remove.bundle.annot.DBI(dbUUID=dbUUID,bundle=hackBndl)
      # delete bundle entry
      dbGetQuery(get_emuDBcon(dbUUID), paste0("DELETE FROM bundle WHERE db_uuid = '",dbUUID,"' ",
                                              "AND session = '",curBndls[i,]$session, "' ", 
                                              "AND name = '",curBndls[i,]$name,"'"))
    }
  }
  
  # remove session from cache that where not found
  for(i in 1:nrow(curSes)){
    if(!curSes[i,]$found){
      # delete session entry
      dbGetQuery(get_emuDBcon(dbUUID), paste0("DELETE FROM session WHERE db_uuid = '",dbUUID,"' ",
                                              "AND name = '",curSes[i,]$name, "' "))
      
    }
  }
}

# depthFirstTraversal = function(graph, curNodeName, path = NULL){
#   
#   graph$nodes[graph$nodes$name == curNodeName,]$visited = T
#   
#   kidNodeNames = graph$links$sublevelName[graph$links$superlevelName == curNodeName]
#   
#   # if leaf node
#   if(length(kidNodeNames) == 0){
#     print("LEAF NODE!!!!!!!")
#     print(path)
#     print('###############')
#   }else{
#     for(kidNodeName in kidNodeNames){
#       if(graph$nodes[graph$nodes$name == curNodeName,]$visited == T){
#         path = c(path, kidNodeName)
#         depthFirstTraversal(graph, kidNodeName, path)
#       }
#     }
#   }
#   
#   if(all(graph$nodes$visited)){
#     print("Done")
#   }
# }
# 
# 
# build_hierarchyViews = function(dbName, dbUUID = NULL){
#   
#   dbUUID = get_emuDB_UUID(dbName = dbName, dbUUID = dbUUID)
#   
#   DBconfigJSON = dbGetQuery(get_emuDBcon(dbUUID), paste0("SELECT DBconfigJSON FROM emuDB WHERE uuid='", dbUUID,"'"))
#   
#   DBconfig = jsonlite::fromJSON(DBconfigJSON$DBconfigJSON)
#   
#   linkDefs = DBconfig$linkDefinitions
#   
#   nodes = data.frame(name = unique(c(linkDefs$superlevelName, linkDefs$sublevelName)),
#                      visited = F)
#   
#   graph = list(nodes = nodes, links = linkDefs)
#   
#   rootLinkDefs = linkDefs[!linkDefs$superlevelName %in% linkDefs$sublevelName,]
#   
#   
#   
#   depthFirstTraversal(graph, rootLinkDefs[1,]$superlevelName, rootLinkDefs[1,]$superlevelName)
#   
# }


#####################
# FOR DEVELOPMENT
# library('testthat')
# test_file('tests/testthat/test_aaa_initData.R')
# test_file('tests/testthat/test_fload.R')


