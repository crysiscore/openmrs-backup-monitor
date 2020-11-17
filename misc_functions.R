

getMySQLData <- function(con.mysql, mysql.query) {
  rs  <-    dbSendQuery(con.mysql,  paste0(mysql.query)  )
  data <- fetch(rs, n = -1)
  RMySQL::dbClearResult(rs)
  return(data)
  
}


processFile = function(filepath) {
  con = file(filepath, "r")
  vec_us <- c()
  while ( TRUE ) {
    line = readLines(con, n = 1)
    if ( length(line) == 0 ) {
      break
    }
    #print(line)
    vec_us <- c(vec_us, line)
  }
  
  close(con)
  vec_us
}