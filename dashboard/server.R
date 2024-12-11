list.of.packages <- c("DBI", "RPostgres", "tidyverse", "sf", "assertthat", "rstudioapi", "leaflet")
new.packages <- list.of.packages[!(list.of.packages %in% installed.packages()[,"Package"])]
if(length(new.packages)) install.packages(new.packages)
library(DBI)
library(RPostgres)
library(tidyverse)
library(sf)
library(assertthat)
library(rstudioapi) 
library(leaflet)

# retrieving path from getSourceEditorContext() 
local <- dirname(getSourceEditorContext()$path)
source(file.path(local, "app_data.R"))

server <- function(input, output){
  
  output$map <- renderLeaflet({
    
    # color palette
    pal <- colorNumeric("YlOrRd", domain = shapefile$prop_yes)
    
    labels <- sprintf(
      "<strong>%s</strong><br/>Proportion Yes on Question 2: %g<br/>Proportion of Students Pass ELA MCAS: %g<br/>Graduation Rate: %g",
      shapefile$district_name, shapefile$prop_yes, shapefile$prop_pass_mcas_ela, shapefile$percent_grad
    ) %>% lapply(htmltools::HTML)
    
    leaflet(shapefile) %>% 
      setView(lng = -71.9266, lat = 42.1, zoom = 8) %>% # center the map in Worcester, MA
      addPolygons(fillColor = ~pal(prop_yes),
                  weight = 2,
                  opacity = 1,
                  color = "white",
                  dashArray = "3",
                  fillOpacity = 0.7,
                  highlightOptions = highlightOptions(
                    weight = 5,
                    color = "#666",
                    dashArray = "",
                    fillOpacity = 0.7,
                    bringToFront = TRUE),
                  label = labels,
                  labelOptions = labelOptions(
                    style = list("font-weight" = "normal", padding = "3px 8px"),
                    textsize = "15px",
                    direction = "auto")) %>%
      addLegend(pal = pal, values = ~prop_yes, opacity = 0.7, title = "Percent Yes on Q2",
                position = "bottomleft")
    
  })
}
