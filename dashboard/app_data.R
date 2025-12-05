# Connect to Postgres and read in data
con <- dbConnect(RPostgres::Postgres(),
                 dbname = "juliatucher",
                 host = "127.0.0.1",
                 user = "juliatucher",
                 password = "dataeng690",
                 port = '5432')

# pull from sql query the combines school district data with town data and aggregates
school_town_data <- dbGetQuery(con, read_file(file.path(local, "sql/get_school_analysis_data.sql")))

# town data
town_data <- dbGetQuery(con,
                        'SELECT
    county,
    town,
    SUM(response_yes) as response_yes,
    SUM(response_no) as response_no,
    SUM(response_blank) as response_blank,
    SUM(response_total) as response_total
  FROM election_result
  GROUP BY county, town')

# pull GIS data
table_name <- "district_shapes"
geom_column <- "geometry"
query <- paste0("SELECT *, ST_AsText(", geom_column, ") AS geom_text FROM ", table_name)
district_shapes <- st_read(con, query = query)

# disconnect from Postgres
dbDisconnect(con)

# Add analytic columns to analytic dataset
school_town_df <- school_town_data %>%
  mutate(
    prop_yes = round(response_yes / response_total * 100, 1),
    prop_pass_mcas_ela = round(num_meets_exceeds_ela / (num_meets_exceeds_ela + num_partial_meet_ela + num_not_meet_ela) * 100, 1),
  )

assert_that(length(unique(school_town_df$district_code)) == nrow(school_town_df))

# create shapefile for plotting
shapefile <- district_shapes %>%
  mutate(district_code = as.integer(district_code)) %>%
  left_join(school_town_df,
            by = c("district_code", "district_name")) %>%
  arrange(!is.na(prop_yes), prop_yes)
