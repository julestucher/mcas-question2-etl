
# importing package
library(rstudioapi) 

# retrieving path from getSourceEditorContext() 
local <- dirname(getSourceEditorContext()$path)
source(file.path(local, "ui.R"))
source(file.path(local, "server.R"))

# Run the application 
shinyApp(ui = ui, server = server)



