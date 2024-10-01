CREATE TABLE IF NOT EXISTS national_analysis.dengue_fever_cases (
    case_id VARCHAR(20) PRIMARY KEY,
    cluster_id INTEGER,
    date DATE,
    serotype VARCHAR(20)[],
    residential VARCHAR(20),
    postal INTEGER
);

COMMENT ON COLUMN national_analysis.df_cases.case_id IS 'Unique identifier for each case';
COMMENT ON COLUMN national_analysis.df_cases.cluster_id IS 'Unique identifier of the associated cluster';
COMMENT ON COLUMN national_analysis.df_cases.date IS 'Onset date of the case';
COMMENT ON COLUMN national_analysis.df_cases.serotype IS 'Serotype of the dengue case could be, could be multiple serotype due to paste infections';
COMMENT ON COLUMN national_analysis.df_cases.residential IS 'Residential information of the case';
COMMENT ON COLUMN national_analysis.df_cases.postal IS 'Postal code of the residence';
