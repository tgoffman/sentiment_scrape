library('sentimentfork')
library('RMySQL')
library(XML)

con <- dbConnect(MySQL(), user= "root", dbname ="sentiment")
resultSet <- dbSendQuery(con, "select id, formatted_content from replies where company_id=24")

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

  sentimentRows <- c()
  sentenceRows <- c()

  for(i in 1:length(batch[,2])) {
    text <- batch[i,2]

    replyId <- batch[i,1]

    # Scrub text of unwanted characters setting up for XML clean
    cleaned = gsub("&", "", text) # Remove ampersands
    cleaned = gsub("\x92ve", "", text) # Remove invalid utf-8 character

    # Pull Text only from XML
    doc.html <- htmlParse(c("<p>",cleaned,"</p>"),asText=TRUE)
    root <- xmlRoot(doc.html)
    removeNodes(xmlElementsByTagName(root, "br")) # Remove BR tags
    cleaned = getChildrenStrings(root) # Take only text from XML tree

                                            # remove unnecessary spaces
    cleaned = gsub("[ \t]{2,}", "", cleaned)
    cleaned = gsub("^\\s+|\\s+$", "", cleaned)

    cleaned = gsub("(?!\\.)[[:punct:]]", "", cleaned, perl=TRUE) # clean punctuation except periods

    withPeriods <- cleaned # Save punctuation to split on sentences within `classify_polarity`

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

    value = classify_polarity(text, withPeriods, algorithm="bayes")

    negative <- value[[1]]
    positive <- value[[2]]
    sentiment <- value[[4]]

    sentimentRows <- rbind(sentimentRows, paste(replyId, negative, positive, sentiment, sep="\t"))

    topSentences <- value[[5]]

    if (length(topSentences) > 1) {
      sentenceRows <- rbind(sentenceRows, paste(replyId, paste("\"", topSentences[1,1] , "\"", sep=""), topSentences[1,2], sep="\t"))
    }

    if (length(topSentences) > 3) {
      sentenceRows <- rbind(sentenceRows, paste(replyId, paste("\"", topSentences[1,3] , "\"", sep=""), topSentences[1,4], sep="\t"))
    }    

  }
  write(paste(sentimentRows, collapse="\n"), file="~/workspace/sentiment/reply_sentiments.txt", sep="\n", append=TRUE)
  write(paste(sentenceRows, collapse="\n"), file="~/workspace/sentiment/reply_sentiment_sentences.txt", sep="\n", append=TRUE)
}

# Cleanup connections
dbClearResult(resultSet)
dbDisconnect(con)

