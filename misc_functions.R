

getMySQLData <- function(con.mysql, mysql.query) {
  rs  <-    dbSendQuery(con.mysql,  paste0(mysql.query)  )
  data <- fetch(rs, n = -1)
  RMySQL::dbClearResult(rs)
  return(data)
  
}
