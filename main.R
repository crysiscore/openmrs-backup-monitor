library(RMySQL)
source(file = 'config.R')
source(file = 'misc_functions.R')

# Objecto de connexao com a bd openmrs
con_openmrs = dbConnect(
  MySQL(),
  user = openmrs.user,
  password = openmrs.password,
 # dbname = openmrs.db.name,
  host = openmrs.host,
  port = openmrs.port
)

db_names <- c("1_junho","1_maio","albasine","altomae","bagamoio","chamanculo","hulene","josemacamo","josemacamo_hg","magoanine","polana_canico","porto","xipamanine","zimpeto")

for(i in 1:length(db_names)){
  
  db_name <- db_names[i]
  
  # check if there is a record on report db
  record_count <- getMySQLData(con.mysql = con_openmrs,mysql.query = paste0("select count(*) as total from report.daily_log where us_name ='",db_name,"' ;"))
  
  # primeira sincronizacao
  if(record_count$total==0){
    
    # check if tables exists
    check_query <- paste0("SELECT * FROM information_schema.tables WHERE table_schema = '" ,db_name,"' AND table_name = 'encounter' LIMIT 1;")
    check <- getMySQLData(con.mysql = con_openmrs, mysql.query = check_query)
    if(nrow(check)>0){
      count_encounter <- getMySQLData(con.mysql = con_openmrs,mysql.query = paste0("select count(*) as total from ", db_name,".encounter ;"))
      
      # insert records into report db
      insert_query <- paste0(
        " INSERT INTO report.daily_log(us_name, table_name, date, total_last_encounter, n_rows_inserted) VALUES ( '",db_name ,"' ",
        ", 'encounter' , ", " now()", ", ", count_encounter$total, " , ", 0 , " ) ;")
      dbExecute(conn = con_openmrs,statement = insert_query)
      
    } else {
      ## skip db ( maybe its an empty db)
    }
    
    
        
  } else {
    
    count_encounter <- getMySQLData(con.mysql = con_openmrs,mysql.query = paste0("select count(*) as total from ", db_name,".encounter ;"))
    count_last_inserted_encounter <- getMySQLData(con.mysql = con_openmrs,mysql.query = paste0("select total_last_encounter as total from report.daily_log 
                                                                                               where us_name ='",db_name,"' order by date desc limit 1;"))
    if( !is.na(count_encounter$total) & !is.na(count_last_inserted_encounter$total) ){
      
      count_new_rows <- count_encounter$total - count_last_inserted_encounter$total
      if (count_new_rows > 0){
        # new rows
        # insert records into report db
        insert_query <- paste0(
          " INSERT INTO report.daily_log(us_name, table_name, date, total_last_encounter, n_rows_inserted) VALUES ( '",db_name ,"' ",
          ", 'encounter' , ", " now()", ", ", count_encounter$total, " , ", count_new_rows , " ) ;")
        dbExecute(conn = con_openmrs,statement = insert_query )
      }
      
    }

  }
  
}