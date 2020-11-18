library(RMySQL)
library(stringr)
library(mailR)
library(writexl)


setwd('/home/agnaldo/Git/monitorar-backups-openmrs') 
#setwd('/home/ccsadmin/R/projects/monitorar-backups-openmrs') 


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

db_names <-processFile("unidades_sanitarias.txt")


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
         
         #last_sync_date <- getMySQLData(con.mysql = con_openmrs,mysql.query = paste0("select date  from report.daily_log 
         #                                                                                     where us_name ='",db_name,"' order by date desc limit 1;"))
         
         update_last_sync_date_query <- paste0("update  report.sync_status set last_sync = curdate() where  us_name ='", db_name,"' ;")
         dbExecute(conn = con_openmrs,statement = update_last_sync_date_query )
      } else {
        last_sync_date <- getMySQLData(con.mysql = con_openmrs,mysql.query = paste0("select date  from report.daily_log 
                                                                                               where us_name ='",db_name,"' order by date desc limit 1;"))
        if(!is.na(last_sync_date)){
          if(nrow(last_sync_date)>0){
            last_sync_date <- substr(last_sync_date$date, 1,10)
            print(paste0("US: ",db_name, " nao sincronizou novos dados no dia :", Sys.Date()))
            print(paste0( "A ultima sincronizacao foi no dia: ", last_sync_date ))
          } else {
            print(paste0("US: ",db_name, " nao sincronizou novos dados no dia: ", Sys.Date()))
            
          }
        }

        
      }
      
    }

  }
  
}

last_sync <- getMySQLData(con.mysql = con_openmrs, mysql.query = paste0("select * from report.sync_status ;"))
if(!is.na(last_sync)){
   names(last_sync)[3] <- "data_ultima_sincr"
   write_xlsx(x = last_sync,path =paste0(getwd(), '/sync_status.xlsx'))
   
   
    #pendentes "alcinobuque@ccsaude.org.mz"
   
  send.mail(from = "mea.ccs.backups@gmail.com",
             to = c("mauriciotimecane@ccsaude.org.mz","agnaldosamuel@ccsaude.org.mz",
                    "antoniomanaca@ccsaude.org.mz","waltermacueia@ccsaude.org.mz","brunomadeira@ccsaude.org.mz",
                    "leoneluqueio@ccsaude.org.mz",  ,"deligenciasambo@ccsaude.org.mz",
                    "tchezaraul@ccsaude.org.mz", "reginaldojetimane@ccsaude.org.mz","isabeldjeco@ccsaude.org.mz",
                    "marciajasse@ccsaude.org.mz", "edsonmoreira@ccsaude.org.mz", "angelomanhique@ccsaude.org.mz",
                    "joaomandlate@ccsaude.org.mz"),
             #replyTo = c("Reply to someone else <someone.else@gmail.com>")
             subject = paste0("Report de backups - ",Sys.Date()),
             body = " Prezados segue no anexo o status da sincronizacao dos backups openmrs",
             smtp = list(host.name = "smtp.gmail.com", port = 465, user.name = "mea.ccs.backups", passwd = "Borbolet@2020", ssl = TRUE),
             authenticate = TRUE,
             send = TRUE,
             attach.files =paste0(getwd(), '/sync_status.xlsx')  )
}
