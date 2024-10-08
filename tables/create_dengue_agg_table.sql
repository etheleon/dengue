CREATE TABLE IF NOT EXISTS national_analysis.dengue_agg (
    date DATE,
    serotype_strain VARCHAR(20)[],
    serotype_count,
    cases_total INTEGER
    cases_serotyped INTEGER
);

COMMENT ON TABLE national_analysis.dengue_fever_agg IS 'Agg data from dengue fever cases';

COMMENT ON COLUMN national_analysis.dengue_fever_agg.date IS 'First day of the epi week';
COMMENT ON COLUMN national_analysis.dengue_fever_agg.serotype_strain IS 'The strain of the serotype';
COMMENT ON COLUMN national_analysis.dengue_fever_agg.serotype_count IS 'Number of serotyped cases belonging to strain';
COMMENT ON COLUMN national_analysis.dengue_fever_agg.cases_total IS 'the total reported cases';
COMMENT ON COLUMN national_analysis.dengue_fever_agg.cases_serotyped IS 'the total number of cases serotyped';
