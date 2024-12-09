# ETL Pipeline for MCAS Achievement, Graduation Rates, and 2024 Question 2 Polling in Massachusetts

## Database models -- create single, one-time script that creates the databases
# Define a database model for district-level analysis
class ElectionResult(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    county = db.Column(db.String(100), nullable=False)
    town = db.Column(db.String(100), nullable=False)
    response_yes = db.Column(db.Integer, nullable=False)
    response_no = db.Column(db.Integer, nullable=False)
    response_blank = db.Column(db.Integer, nullable=False)
    response_total = db.Column(db.Integer, nullable=False)

CREATE TABLE election_result (
    id SERIAL PRIMARY KEY,
    county VARCHAR(100),
    town VARCHAR(100),
    response_yes NUMERIC,
    response_no NUMERIC,
    response_blank NUMERIC,
    response_total NUMERIC
);

# Define a database model for district-level analysis
class SchoolDistrict(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    district_code = db.Column(db.Integer, nullable=False)
    district_name = db.Column(db.String(100), nullable=False)
    year = db.Column(db.Integer, nullable=False)
    num_meets_exceeds_ela = db.Column(db.Integer, nullable=False)
    num_partial_meet_ela = db.Column(db.Integer, nullable=False)
    num_not_meet_ela = db.Column(db.Integer, nullable=False)
    percent_grad = db.Column(db.Float, nullable=False)

# Create School District-Town crosswalk
CREATE TABLE district_town_lookup (
    id SERIAL PRIMARY KEY,
    district_code NUMERIC,
    district_name VARCHAR(100),
    town VARCHAR(100)
);

# Define a database model for GIS data
CREATE TABLE district_shapes (
    id SERIAL PRIMARY KEY,
    district_code NUMERIC,
    district_name VARCHAR(100),
    geometry GEOMETRY(MULTIPOLYGON, 4326)
);