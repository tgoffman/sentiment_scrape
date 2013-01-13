library('sentiment')
library('RMySQL')

con <- dbConnect(MySQL(), user= "root", dbname ="sentiment")
resultSet <- dbSendQuery(con, "select id, subject, additional_detail from topics inner join topic_sentiments on topics.id = topic_sentiments.topic_id where topic_sentiments.positive is null limit 500")

# Build a SQL value statement for topic sentiment row eg: ... VALUES (Col1Val, Col2Val, Col3Val)
buildSentimentRowValue <- function(topic_id, positive, negative) {
  inner_value <- paste(topic_id, positive, negative, sep = ",")
  return(paste("(", inner_value, ")", sep = ""))
}

BATCH_SIZE <- 500
while (!dbHasCompleted(resultSet)) {
  batch = fetch(resultSet, n = BATCH_SIZE)

  if (length(batch) != 3){ # Make sure our row set has items with 3 columns
    break
  }

   values <- ""

  for(i in 1:length(batch[,3])) {
    text <- paste(batch[i,2], batch[1,3], sep = " ")
    value = classify_polarity(text, algorithm="bayes")
    values <- paste(values, buildSentimentRowValue(batch[i,1], value[1,1], value[1,2]), ",", sep = "")
  }

  values <- substr(values, 1, nchar(values)-1) # Remove last ','
  sql_statement <- paste( "insert into topic_sentiments (topic_id, positive, negative) values ", values, " on duplicate key update positive = values(positive), negative = values(negative);", sep = "")

  con2 <- dbConnect(MySQL(), user= "root", dbname ="sentiment")
  print(sql_statement)
  res <- dbSendQuery(con2, sql_statement)
  dbDisconnect(con2)    
}

# Cleanup connections
dbClearResult(resultSet)
dbDisconnect(con)

