-- ==============================================================
-- Description:
--   This script defines the base table structure for the Bronze layer.
--   It holds raw Spotify Top Podcasts data ingested via BULK INSERT,
--   preserving the source format before any cleaning or transformation.
--
--   The table design intentionally uses VARCHAR(MAX) to accommodate
--   flexible, untyped ingestion from diverse CSV sources.
-- ==============================================================

DROP TABLE IF EXISTS bronze.spotify_top_podcasts;

CREATE TABLE bronze.spotify_top_podcasts (
    episode_date VARCHAR(MAX),
    fact_episode_performancerank VARCHAR(MAX),
    region VARCHAR(MAX),
    chart_rank_move VARCHAR(MAX),
    episode_uri VARCHAR(MAX),
    show_uri VARCHAR(MAX),
    episode_name VARCHAR(MAX),
    episode_description VARCHAR(MAX),
    show_name VARCHAR(MAX),
    show_description VARCHAR(MAX),
    show_publisher VARCHAR(MAX),
    duration_ms VARCHAR(MAX),
    explicit_content VARCHAR(MAX),
    languages VARCHAR(MAX),
    release_date VARCHAR(MAX),
    release_date_precision VARCHAR(MAX),
    show_media_type VARCHAR(MAX),
    show_total_episodes VARCHAR(MAX)
);
