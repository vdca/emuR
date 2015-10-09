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
    
    update_cache(schema[['name']], dbUUID = dbUUID, verbose = verbose)
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
      bundle = jsonlite::fromJSON(normalizePath(annotFilePath), simplifyVector = FALSE)
      listOfDfs = annotJSONtoListOfDataFrames(bndl)
      
      # calculate MD5 sum of bundle annotJSON      
      MD5annotJSON = md5sum(normalizePath(annotFilePath))
      names(MD5annotJSON) = NULL
      
      # insert bundle table entry
      dbWriteTable(con, "bundle", data.frame(db_uuid = db$DBconfig$UUID, 
                                             session = sessionName,
                                             name = bName,
                                             annotates = bundle$annotates,
                                             sampleRate = bundle$sampleRate,
                                             mediaFilePath = bundle$annotates, # SIC will have to be deleted once portable cache is merged into master
                                             MD5annotJSON = MD5annotJSON), append = T)
      # insert items table entries
      listOfDfs$items = data.frame(db_uuid = rep(db$DBconfig$UUID, nrow(listOfDfs$items)), 
                                   session = rep(sessionName, nrow(listOfDfs$items)),
                                   bundle = rep(bName, nrow(listOfDfs$items)),
                                   listOfDfs$items)
      
      dbWriteTable(con, "item", listOfDfs$items, append = T)
      
      # insert labels table entries
      listOfDfs$labels =  data.frame(db_uuid = rep(db$DBconfig$UUID, nrow(listOfDfs$labels)), 
                                     session = rep(sessionName, nrow(listOfDfs$labels)),
                                     bundle = rep(bName, nrow(listOfDfs$labels)),
                                     listOfDfs$labels)
      
      dbWriteTable(con, "labels", listOfDfs$labels, append = T)
      
      # insert links table entries
      listOfDfs$links =  data.frame(db_uuid = rep(db$DBconfig$UUID, nrow(listOfDfs$links)), 
                                    session = rep(sessionName, nrow(listOfDfs$links)),
                                    bundle = rep(bName, nrow(listOfDfs$links)),
                                    listOfDfs$links,
                                    label = rep(NA, nrow(listOfDfs$links)))
      
      dbWriteTable(con, "links", listOfDfs$links, append = T)
      
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

#####################
# FOR DEVELOPMENT
# purge_all_emuDBs(interactive = F)
# ae = fload_emuDB("~/Desktop/QE_MAuS/")
# print(ae)

