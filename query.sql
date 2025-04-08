-- Create a view for each CSV file
CREATE VIEW fake_namr_out AS SELECT * FROM read_csv_auto('example-data/fake_namr_out.csv');
CREATE VIEW fake_namr_rpx AS SELECT * FROM read_csv_auto('example-data/fake_namr_rpx.csv');

-- Get all unique IDs from files that have ID_BB_GLOBAL
WITH all_ids AS (
    SELECT DISTINCT ID_BB_GLOBAL 
    FROM (
        SELECT ID_BB_GLOBAL FROM fake_namr_out 
        WHERE ID_BB_GLOBAL IS NOT NULL
        UNION 
        SELECT ID_BB_GLOBAL FROM fake_namr_rpx
        WHERE ID_BB_GLOBAL IS NOT NULL
    )
)
-- Create the final materialized table
CREATE TABLE data_matrix AS
SELECT 
    all_ids.ID_BB_GLOBAL,
    -- Add columns from fake_namr_out
    MAX(o.*) EXCLUDE (ID_BB_GLOBAL),
    -- Add columns from fake_namr_rpx
    MAX(r.*) EXCLUDE (ID_BB_GLOBAL)
FROM all_ids
LEFT JOIN fake_namr_out o ON all_ids.ID_BB_GLOBAL = o.ID_BB_GLOBAL
LEFT JOIN fake_namr_rpx r ON all_ids.ID_BB_GLOBAL = r.ID_BB_GLOBAL
GROUP BY all_ids.ID_BB_GLOBAL;

-- Show summary
SELECT COUNT(*) as row_count FROM data_matrix;
