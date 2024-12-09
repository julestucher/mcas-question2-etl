library(shiny)
library(leaflet)

ui <- fluidPage(
  leafletOutput(outputId = "map")
)