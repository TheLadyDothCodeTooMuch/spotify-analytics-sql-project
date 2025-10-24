-- ==============================================================
-- Description:
--   This script sets up the foundational structure for the project:
--   - Creates the main database (SpotifyDB)
--   - Establishes the schema layers: bronze, silver, and gold
--
--   These schemas represent the three-tier data warehouse design:
--     • Bronze → Raw ingestion layer
--     • Silver → Cleaned and transformed layer
--     • Gold   → Analytical and reporting layer
-- ==============================================================

CREATE DATABASE SpotifyDB;

USE SpotifyDB;

CREATE SCHEMA bronze
GO
CREATE SCHEMA silver
GO
CREATE SCHEMA gold
GO
