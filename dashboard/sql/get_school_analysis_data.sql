SELECT
  school_district.district_code,
  school_district.district_name,
  school_district.year, 
  STRING_AGG(distinct town_data.county, ', ') as counties,
  STRING_AGG(town_data.town, ', ') as towns,
  AVG(school_district.num_meets_exceeds_ela) as num_meets_exceeds_ela,
  AVG(school_district.num_partial_meet_ela) as num_partial_meet_ela,
  AVG(school_district.num_not_meet_ela) as num_not_meet_ela,
  AVG(school_district.percent_grad) as percent_grad,
  SUM(town_data.response_yes) as response_yes,
  SUM(town_data.response_no) as response_no,
  SUM(town_data.response_blank) as response_blank,
  SUM(town_data.response_total) as response_total
FROM school_district
LEFT JOIN district_town_lookup ON school_district.district_code = district_town_lookup.district_code
INNER JOIN (
  SELECT
    county,
    town,
    SUM(response_yes) as response_yes,
    SUM(response_no) as response_no,
    SUM(response_blank) as response_blank,
    SUM(response_total) as response_total
  FROM election_result
  GROUP BY county, town
) town_data ON district_town_lookup.town = town_data.town
GROUP BY school_district.district_code, school_district.district_name, school_district.year