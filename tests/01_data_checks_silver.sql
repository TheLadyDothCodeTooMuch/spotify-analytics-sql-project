-- Check that dates are standardised. Expected result is 0
SELECT
    release_date
FROM silver.spotify_top_podcasts
WHERE LEN(release_date) != 10 
--OR ISDATE(release_date) = 0;

-- confirms that these are the 200 top ranked episodes for the day and not more. Expected result is 0
SELECT DISTINCT
    episode_rank
FROM silver.spotify_top_podcasts
WHERE episode_rank > 200;

-- confirms that is indeed a total of 200 eposides for the day. Expected result is 200
SELECT COUNT(DISTINCT episode_rank) AS missing_rank_count
FROM silver.spotify_top_podcasts;

-- checks that each regional daily rank is unique. Expected result is 0.
SELECT 
    episode_date, 
    region, 
    COUNT(episode_rank) AS duplicate_ranks
FROM silver.spotify_top_podcasts
GROUP BY episode_date, region, episode_rank
HAVING COUNT(*) > 1;

-- confirms that is indeed the 22 top ranked episodes for the day and not more. Expected result is 22
SELECT COUNT(DISTINCT region) AS no_of_regions
FROM silver.spotify_top_podcasts;

--selects all regions where the ranks are based. The regions should all be spelt out clearly
SELECT DISTINCT 
    region
FROM silver.spotify_top_podcasts;

-- Check for leading or trailing spaces in the chart_rank_move column
SELECT 
    chart_rank
FROM silver.spotify_top_podcasts
WHERE TRIM(chart_rank) != chart_rank;

-- Check for NULL or empty values in chart_rank, show_uri, episode_uri, episode_name, show_name and episode_description etc. Expected result is 0
SELECT 
    episode_description
FROM silver.spotify_top_podcasts
WHERE episode_description IS NULL OR LTRIM(RTRIM(episode_description)) IN ('', 'N/A', 'null');

-- Check for any rows in 'chart_rank_move' that contain numbers, which may indicate invalid or inconsistent entries. Expected result is 0
SELECT
    chart_rank
FROM silver.spotify_top_podcasts
WHERE chart_rank LIKE '%[0-9]%';

-- Inspect distinct character lengths of values in 'show_uri' 
-- to check for inconsistencies or formatting issues across records.
SELECT DISTINCT
    LEN(show_uri)
FROM silver.spotify_top_podcasts;

--helps detect irregular or malformed URI values that may indicate data formatting issues or mixed URI. Expected result is 0
SELECT
*
FROM (
    SELECT
        episode_name,
        LEN(show_uri) AS length_show_uri,
        AVG(LEN(show_uri)) AS avg_show_url_length,
        AVG(LEN(episode_uri)) AS avg_epi_url_length
    FROM silver.spotify_top_podcasts
    GROUP BY episode_name, LEN(show_uri) 
) AS t
WHERE length_show_uri != avg_show_url_length;

-- checks for duplicate show or episode uris. Expected value is 0
SELECT
    show_name,
    episode_name,
    COUNT(DISTINCT episode_uri) AS distinct_show_uri
FROM silver.spotify_top_podcasts
GROUP BY
    show_name,
    episode_name
HAVING COUNT(DISTINCT episode_uri) > 1;

-- checks that the URI does not have multiple show names
SELECT
*
FROM(
    SELECT
        show_name,
        show_uri,
        DENSE_RANK() OVER(PARTITION BY show_uri ORDER BY show_name ASC) AS ranked
    FROM silver.spotify_top_podcasts
    GROUP BY
    show_name,
    show_uri
    ) AS g 
WHERE ranked > 1; 

-- Finds text encoding issues (e.g., misencoded special characters like “â€” or â€”) 
-- that may have resulted from incorrect UTF-8 handling during data ingestion.
SELECT
*
FROM silver.spotify_top_podcasts
WHERE episode_name LIKE '%â€%';

