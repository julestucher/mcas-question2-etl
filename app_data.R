
# Connect to Postgres and read in data
con <- dbConnect(RPostgres::Postgres(),
                 dbname = "juliatucher",
                 host = "localhost",
                 port = 5432)

school_district <- dbGetQuery(con, "SELECT * FROM school_district")
election_result <- dbGetQuery(con, "SELECT * FROM election_result")
district_town_lu <- dbGetQuery(con, "SELECT * FROM district_town_lookup")

# pull GIS data
table_name <- "district_shapes"
geom_column <- "geometry"
query <- paste0("SELECT *, ST_AsText(", geom_column, ") AS geom_text FROM ", table_name)
district_shapes <- st_read(con, query = query)

dbDisconnect(con)

# Create analytic database -- REFACTOR THIS IN SQL
school_town_df <- school_district %>%
  left_join(district_town_lu %>% select(-id), by = c("district_code", "district_name")) %>%
  drop_na() %>%
  left_join(election_result %>%
              select(county, town, starts_with("response_")) %>%
              group_by(county, town) %>%
              summarize_all(sum), 
            by = "town") %>%
  group_by(id, district_code, district_name, year) %>%
  summarize(
    counties = list(unique(county)),
    towns = list(unique(town)),
    num_meets_exceeds_ela = mean(num_meets_exceeds_ela),
    num_partial_meet_ela = mean(num_partial_meet_ela),
    num_not_meet_ela = mean(num_not_meet_ela),
    percent_grad = mean(percent_grad),
    response_yes = sum(response_yes),
    reseponse_no = sum(response_no),
    response_blank = sum(response_blank),
    response_total = sum(response_total),
    .groups='keep'
  ) %>%
  ungroup() %>%
  mutate(
    prop_yes = response_yes / response_total,
    prop_pass_mcas_ela = num_meets_exceeds_ela / (num_meets_exceeds_ela + num_partial_meet_ela + num_not_meet_ela),
  )

assert_that(length(unique(school_town_df$id)) == nrow(school_town_df))

# create shapefile for plotting
shapefile <- district_shapes %>%
  mutate(district_code = as.numeric(district_code)) %>%
  left_join(school_town_df,
            by = c("district_code", "district_name"))
