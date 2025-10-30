/*
**Script:** create_silver_spotify_top_podcasts.sql
**Purpose:** Creates the final, clean "Silver" table for our podcast data.

This table stores the Spotify data after it has been:
1.  Cleaned (e.g., '???' changed to 'Unknown')
2.  Standardized (e.g., 'jp' changed to 'Japan')
3.  Fixed (e.g., missing show names are filled in)

The `DROP TABLE IF EXISTS` line is important:
-   It lets us run this script over and over without errors.
-   It deletes the old table first, ensuring a fresh start.

The `dwh_load_timestamp` column automatically stamps each row
with the time it was loaded.
*/

-- This command deletes the old table, if it exists, so we can create a new one.
DROP TABLE IF EXISTS silver.spotify_top_podcasts;

-- This command creates the new, empty table.
CREATE TABLE silver.spotify_top_podcasts (
    episode_date DATE,
    episode_rank INT,
    region VARCHAR(50),
    chart_rank VARCHAR(50),
    episode_uri VARCHAR(100),
    show_uri VARCHAR(100),
    episode_name VARCHAR(1000),
    episode_description VARCHAR(MAX),
    show_name VARCHAR(1000),
    show_description VARCHAR(MAX),
    show_publisher VARCHAR(1000),
    duration_ms INT,
    explicit_content VARCHAR(10),
    languages VARCHAR(100),
    release_date DATE,
    release_date_precision VARCHAR(10),
    show_media_type VARCHAR(10),
    show_total_episodes INT,
    dwh_load_timestamp DATETIME DEFAULT GETDATE()
);


