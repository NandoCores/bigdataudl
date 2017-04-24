library(dplyr)

# Put the root path to start the analyse

setwd("C:\\Users\\kinta\\Documents\\GitHub\\bigdataudl\\Big Data Project\\dictation")

frenchdictation <- read.csv("frenchdic\\ds71_student_step_All_Data_60_2015_0729_174907.txt",  sep="\t")
spanishdictation <- read.csv("spanishdic\\ds72_student_step_All_Data_61_2015_0729_174948.txt",  sep="\t")
chinesedictation <- read.csv("chinesedic\\ds73_student_step_All_Data_62_2016_0401_112846.txt",  sep="\t")

fix(frenchdictation)
fix(spanishdictation)
fix(chinesedictation)

## French class analyse ##
frenchstudents <- as.character(unique(frenchdictation$Anon.Student.Id))
i <- as.integer(1)
for(french in frenchstudents){
  resultfrench <- frenchdictation[frenchdictation$Anon.Student.Id == french,]

  if(i == 1){
    listfrench <- data.frame(id = i, student = french, totalproblem = nrow(resultfrench), 
                             correctproblem = sum(resultfrench$Corrects),
                             percentage = (sum(resultfrench$Corrects)/nrow(resultfrench))*100)
  }else{
    listfrench <- rbind(listfrench, c(id = i , student = 2, totalproblem = nrow(resultfrench), 
                        correctproblem = sum(resultfrench$Corrects),
                        percentage = (sum(resultfrench$Corrects)/nrow(resultfrench))*100))
    
  }
  i <- i+1
}
listfrench$student <- frenchstudents


## Spanish class analyse ##
spanishstudents <- as.character(unique(spanishdictation$Anon.Student.Id))
i <- as.integer(1)
for(spanish in spanishstudents){
  resultspanish <- spanishdictation[spanishdictation$Anon.Student.Id == spanish,]
  
  if(i == 1){
    listspanish <- data.frame(id = i, student = spanish, totalproblem = nrow(resultspanish), 
                             correctproblem = sum(resultspanish$Corrects),
                             percentage = (sum(resultspanish$Corrects)/nrow(resultspanish))*100)
  }else{
    listspanish <- rbind(listspanish, c(id = i , student = 2, totalproblem = nrow(resultspanish), 
                                      correctproblem = sum(resultspanish$Corrects),
                                      percentage = (sum(resultspanish$Corrects)/nrow(resultspanish))*100))
    
  }
  i <- i+1
}
listspanish$student <- spanishstudents


## Chinese class analyse ##
chinesestudents <- as.character(unique(chinesedictation$Anon.Student.Id))
i <- as.integer(1)
for(chinese in chinesestudents){
  resultchinese <- chinesedictation[chinesedictation$Anon.Student.Id == chinese,]
  
  if(i == 1){
    listchinese <- data.frame(id = i, student = chinese, totalproblem = nrow(resultchinese), 
                              correctproblem = sum(resultchinese$Corrects),
                              percentage = (sum(resultchinese$Corrects)/nrow(resultchinese))*100)
  }else{
    listchinese <- rbind(listchinese, c(id = i , student = 2, totalproblem = nrow(resultchinese), 
                                        correctproblem = sum(resultchinese$Corrects),
                                        percentage = (sum(resultchinese$Corrects)/nrow(resultchinese))*100))
  }
  i <- i+1
}
listchinese$student <- chinesestudents

############### ###############
fix(listfrench)
fix(listspanish)
fix(listchinese)


orderlistfrench <- listfrench[order(listfrench$totalproblem, listfrench$correctproblem, decreasing = TRUE),]
orderlistspanish <- listspanish[order(listspanish$totalproblem, listspanish$correctproblem, decreasing = TRUE),]
orderlistchinese <- listchinese[order(listchinese$totalproblem, listchinese$correctproblem, decreasing = TRUE),]













