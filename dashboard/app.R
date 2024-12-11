# importing package
list.of.packages <- c("rstudioapi")
new.packages <- list.of.packages[!(list.of.packages %in% installed.packages()[,"Package"])]
if(length(new.packages)) install.packages(new.packages)
library(rstudioapi) 

# retrieving path from getSourceEditorContext() 
local <- dirname(getSourceEditorContext()$path)
source(file.path(local, "server.R"))
source(file.path(local, "ui.R"))

# Run the application 
shinyApp(ui = ui, server = server)



