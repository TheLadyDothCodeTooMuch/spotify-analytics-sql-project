/***********************************************************************************************

 Procedure: bronze.load_bronze
 Description:
    This stored procedure performs the Bronze Layer data ingestion step for the Spotify Top Podcasts dataset.
    It truncates the target table and reloads it using BULK INSERT, ensuring the data lake’s raw zone is
    refreshed with the most recent CSV extract.

 Example Usage:
EXEC bronze.load_bronze;
***********************************************************************************************/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
    BEGIN TRY
        -- Simple runtime logging provides transparency during ETL execution.
        -- This can later be replaced with structured logging into an audit table.
        PRINT '=====================';
        PRINT 'Loading Bronze Layer';
        PRINT '=====================';

        -- Track load duration for performance benchmarking across runs.
        DECLARE @start_time DATETIME, @end_time DATETIME;
        SET @start_time = GETDATE();

        -- Truncating ensures the bronze table always reflects a full snapshot,
        -- not incremental changes. Suitable for datasets fully regenerated daily.
        PRINT '>> Truncating Table bronze.spotify_top_podcasts';
        TRUNCATE TABLE bronze.spotify_top_podcasts;

        -- BULK INSERT provides high-throughput ingestion for static CSVs.
        -- Using FORMAT='CSV' and CODEPAGE='65001' enforces consistency with UTF-8 exports from APIs or Kaggle.
        -- The 'TABLOCK' hint minimizes logging and improves speed during full loads.
        BULK INSERT bronze.spotify_top_podcasts
        FROM '...\SQL_Project\top_podcasts.csv'
        WITH (
            FORMAT = 'CSV',
            FIRSTROW = 2,               -- header row intentionally skipped to prevent schema misalignment
            FIELDTERMINATOR = ',',      -- consistent with Kaggle/Spotify dataset CSV format
            ROWTERMINATOR = '0x0a',     -- explicit LF termination avoids OS-specific issues
            CODEPAGE = '65001',         -- guarantees multi-language text fidelity
            TABLOCK                     -- improves load performance for large flat files
        );

        -- Record duration for observability; can be extended with row counts in future versions.
        SET @end_time = GETDATE();
        PRINT 'Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS VARCHAR) + ' secs';
    END TRY

    BEGIN CATCH
        -- Error handling captures runtime issues like file access, format mismatch, or permission errors.
        -- Future iterations could log ERROR_NUMBER(), ERROR_SEVERITY(), and ERROR_LINE() for deeper diagnostics.
        PRINT '=====================';
        PRINT 'An error occurred loading the bronze layer';
        PRINT 'Error Message: ' + error_message();
        PRINT '=====================';
    END CATCH
END;

