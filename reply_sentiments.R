library('sentimentfork')
library('RMySQL')
library(XML)

con <- dbConnect(MySQL(), user= "root", dbname ="sentiment")
resultSet <- dbSendQuery(con, "select id, formatted_content from replies limit 10")

# Build a SQL value statement for topic sentiment row eg: ... VALUES (Col1Val, Col2Val, Col3Val)
buildSentimentRowValue <- function(reply_id, positive, negative) {
  inner_value <- paste(reply_id, positive, negative, sep = ",")
  return(paste("(", inner_value, ")", sep = ""))
}

BATCH_SIZE <- 500
while (!dbHasCompleted(resultSet)) {
  batch = fetch(resultSet, n = BATCH_SIZE)

  if (length(batch) != 2){ # Make sure our row set has items with 2 columns
    break
  }

  values <- ""
  sentenceRows <- c()

  for(i in 1:length(batch[,2])) {
    text <- batch[i,2]
    replyId <- batch[i,1]

    # Scrub text of unwanted characters setting up for XML clean
    cleaned = gsub("&", "", text) # Remove ampersands

    # Pull Text only from XML
    doc.html <- xmlParse(c("<p>",cleaned,"</p>"),asText=TRUE)
    root <- xmlRoot(doc.html)
    removeNodes(xmlElementsByTagName(root, "br")) # Remove BR tags
    cleaned = getChildrenStrings(root) # Take only text from XML tree

                                            # remove unnecessary spaces
    cleaned = gsub("[ \t]{2,}", "", cleaned)
    cleaned = gsub("^\\s+|\\s+$", "", cleaned)

    withPunctuation <- cleaned # Save punctuation to split on sentences within `classify_polarity`

    # Safe to clean after XML parse
                                        # remove punctuation
    cleaned = gsub("[[:punct:]]", "", cleaned)
                                        # remove numbers
    cleaned = gsub("[[:digit:]]", "", cleaned)
                                        # remove html links
    cleaned = gsub("http\\w+", "", cleaned)
                                        # remove unnecessary spaces
    cleaned = gsub("[ \t]{2,}", "", cleaned)
    cleaned = gsub("^\\s+|\\s+$", "", cleaned)

    value = classify_polarity(text, withPunctuation, algorithm="bayes")

    topSentences <- value[[5]]

    if (length(topSentences) > 1) {
      sentenceRows <- rbind(sentenceRows, paste(replyId, paste("\"", topSentences[1,1] , "\"", sep=""), topSentences[1,2], sep="\t"))
    }

    if (length(topSentences) > 3) {
      sentenceRows <- rbind(sentenceRows, paste(replyId, paste("\"", topSentences[2,1] , "\"", sep=""), topSentences[2,2], sep="\t"))
    }    

    #values <- paste(values, buildSentimentRowValue(batch[i,1], value[1], value[2]), ",", sep = "")
  }
  write(paste(sentenceRows, collapse="\n"), file="~/workspace/sentiment/sentiment_sentence.txt", sep="\n", append=TRUE)

  # values <- substr(values, 1, nchar(values)-1) # Remove last ','

}

# Cleanup connections
dbClearResult(resultSet)
dbDisconnect(con)

