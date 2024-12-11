list.of.packages <- c("shiny", "leaflet")
new.packages <- list.of.packages[!(list.of.packages %in% installed.packages()[,"Package"])]
if(length(new.packages)) install.packages(new.packages)
library(shiny)
library(leaflet)

mapTab <- fluidPage(
  leafletOutput(outputId = "map")
)

aboutTab <- fluidPage(strong("Data Extraction Overview"),
                      p("In the 2024 election, Massachusetts Question 2 would repeal the state-wide MCAS high school graduation requirement and allow for districts to set graduation requirements for students. In my data engineering final project, I will pull together voting outcome data along with school district-level MCAS achievement data and graduation rates to create a data set for analysis. I will use Python to scrape the data from public MA Department of Elementary and Secondary Education (DESE) reports as well as from the Secretary of the Commonwealth of MA election statistics website. Finally, I will extract GIS data available from the Mass.gov website to link the town-level election restults with school district outcomes. This GIS data will also provide Shapefile geometries to create an interactive map output."))

ui <- navbarPage("Q2 MCAS ETL Dashboard",
                 tabPanel("About", aboutTab),
                 tabPanel("Map", mapTab)
)
