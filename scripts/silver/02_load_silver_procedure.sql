/*
**Stored Procedure:** silver.load_silver
**Purpose:** Cleans, standardizes, and loads data from the bronze layer table (`bronze.spotify_top_podcasts`) into the final `silver.spotify_top_podcasts` table.

**Key Logic:**
1.  **Standardizes `show_uri`s:** Uses a window function to fix "URI drift," ensuring one consistent URI per show.
2.  **Fixes Show Names:** Uses a large `CASE` statement to manually map over 50 known `show_uri`s to their correct names.
3.  **Cleans Data:** Sets corrupted data (e.g., '???') and `NULL`s to 'Unknown'.
4.  **Transforms Data:** Converts codes (e.g., 'jp') to full names (e.g., 'Japan').

Example Usage:
EXEC silver.load_silver
*/

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME;

    BEGIN TRY
        PRINT '=====================';
        PRINT 'Loading Silver Layer';
        PRINT '=====================';
        SET @start_time = GETDATE();

        /* Clear the *actual* destination table for a full refresh. 
        This makes the procedure idempotent (rerunnable) without creating duplicate rows.
        */
        PRINT '>> Truncating Table: silver.spotify_top_podcasts';
        TRUNCATE TABLE silver.spotify_top_podcasts;

        PRINT '>> Inserting Data Into: silver.spotify_top_podcasts';

        /*
        STEP 1: Pre-process the bronze data in a Common Table Expression (CTE).
        The primary goal here is to fix "URI drift," where the same show (e.g., "All Night Nippon")
        has different `show_uri` values on different days in the source data.
        */
        WITH StandardizedData AS (
            SELECT
                episode_date,
                fact_episode_performancerank,
                region,
                chart_rank_move,
                episode_uri,
                episode_name,
                episode_description,
                show_publisher,
                duration_ms,
                explicit_content,
                languages,
                release_date,
                release_date_precision,
                show_media_type,
                show_total_episodes,
                
                /*
                CONTEXT: A show's URI can change. This logic standardizes it.
                It groups all episodes by their `show_name` and `show_publisher`, 
                finds the *most recent* `show_uri` for that group, and then applies that
                single URI back to *all* episodes in that group.
                */
                FIRST_VALUE(show_uri) OVER (
                    PARTITION BY show_name, show_publisher 
                    ORDER BY CAST(episode_date AS DATE) DESC
                ) AS clean_show_uri,
                
                /*
                Pass the original, dirty show_name through. It is needed for the partition above,
                but the final cleaning logic will be applied to it in the next step.
                */
                show_name AS raw_show_name

            FROM 
                bronze.spotify_top_podcasts
        )
        
        /*
        STEP 2: Select from the standardized CTE data, apply all cleaning/transformation
        logic, and insert the final, clean data into the silver table.
        */
        INSERT INTO silver.spotify_top_podcasts (
            episode_date,
            episode_rank,
            region,
            chart_rank,
            episode_uri,
            show_uri,
            episode_name,
            episode_description,
            show_name,
            show_description,
            show_publisher,
            duration_ms,
            explicit_content,
            languages,
            release_date,
            release_date_precision,
            show_media_type,
            show_total_episodes
        )
        SELECT
            episode_date,
            fact_episode_performancerank,
            
            /* Decode 2-letter country codes into human-readable region names. */
            CASE LOWER(region)
                WHEN 'ar' THEN 'Argentina'
                WHEN 'at' THEN 'Austria'
                WHEN 'au' THEN 'Australia'
                WHEN 'br' THEN 'Brazil'
                WHEN 'ca' THEN 'Canada'
                WHEN 'cl' THEN 'Chile'
                WHEN 'co' THEN 'Colombia'
                WHEN 'de' THEN 'Germany'
                WHEN 'es' THEN 'Spain'
                WHEN 'fr' THEN 'France'
                WHEN 'gb' THEN 'United Kingdom'
                WHEN 'id' THEN 'Indonesia'
                WHEN 'ie' THEN 'Ireland'
                WHEN 'in' THEN 'India'
                WHEN 'it' THEN 'Italy'
                WHEN 'jp' THEN 'Japan'
                WHEN 'mx' THEN 'Mexico'
                WHEN 'nl' THEN 'Netherlands'
                WHEN 'nz' THEN 'New Zealand'
                WHEN 'ph' THEN 'Philippines'
                WHEN 'pl' THEN 'Poland'
                WHEN 'us' THEN 'United States'
                ELSE 'Unknown'
            END AS region,
            TRIM(chart_rank_move) AS chart_rank,
            episode_uri,
            
            /* Use the standardized URI from the CTE. This is now the official show URI. */
            clean_show_uri AS show_uri,
            
            /* Basic cleanup for episode-level fields. */
            CASE 
                WHEN episode_name IS NULL OR TRIM(episode_name) IN ('', 'N/A', 'null') THEN 'Unknown'
                WHEN episode_name LIKE '%???%' THEN 'Unknown'
                WHEN episode_name LIKE '?%' THEN 'Unknown'
                ELSE TRIM(episode_name)
            END AS episode_name,
            
            /* More thorough cleanup for description fields. */
            CASE 
                WHEN episode_description IS NULL OR TRIM(episode_description) IN ('', 'N/A', 'null') THEN 'Unknown' 
                WHEN episode_description LIKE '%???%' THEN 'Unknown'
                WHEN episode_description LIKE '?%' THEN 'Unknown'
                WHEN episode_description LIKE '??%' THEN 'Unknown'
                WHEN TRIM(episode_description) = '' THEN 'Unknown'
                ELSE TRIM(episode_description)
            END AS episode_description,

            /* (CRITICAL) Clean the show_name. 
            The order of operations in this CASE statement is essential.
            */
            CASE
                /* STAGE 1: Manual Overrides. 
                Use the now cleaned `clean_show_uri` as the single source to
                fix known-bad, NULL, or corrupted show names. This MUST run first.
                */
                WHEN LOWER(TRIM(clean_show_uri)) = '0qaw6rxkjbyazjqnkkovaj' THEN 'The Rest Is Politics'
                WHEN LOWER(TRIM(clean_show_uri)) = '0vexaznsdn2s7umiiuw41m' THEN 'Chiquillas, un cafecito??'
                WHEN LOWER(TRIM(clean_show_uri)) = '1r4g2polby1rfkbflx7ipo' THEN 'Die Woche - der funk-Podcast'
                WHEN LOWER(TRIM(clean_show_uri)) = '2fdnegvjz5j1k5rrbwrlz' THEN 'The Rest Is Football'
                WHEN LOWER(TRIM(clean_show_uri)) = '2opawdgrvuuvefbvidu' THEN 'Casual Folklore Radio'
                WHEN LOWER(TRIM(clean_show_uri)) = '2p57queim1vqwsjh2rr2ey' THEN 'Context'
                WHEN LOWER(TRIM(clean_show_uri)) = '2qoipucjh332voedyxlr3a' THEN 'Whiskey Ginger with Andrew Santino'  
                WHEN LOWER(TRIM(clean_show_uri)) = '3dn8ygspc2zva7xayvrmpg' THEN 'Punchline with Alex Calleja!'
                WHEN LOWER(TRIM(clean_show_uri)) = '6uvnib9rxunnonyzgzk9' THEN 'Que no surti d''aquí'
                WHEN LOWER(TRIM(clean_show_uri)) = '6x9rcowhyb3m5nxtlihlii' THEN 'Shuukan! ShabeLaser'
                WHEN LOWER(TRIM(clean_show_uri)) = '2jldfmovnrdamhqdjdxgmm' THEN 'AL CIELO CON ELLA con Henar Álvarez'
                WHEN LOWER(TRIM(clean_show_uri)) = '0dyjawio3siacdfp9knoic' THEN 'Otonari'
                WHEN LOWER(TRIM(clean_show_uri)) = '1b3ejbqjbvnhhlfs7unax' THEN 'Magical Lovely no All Night Nippon Zero'
                WHEN LOWER(TRIM(clean_show_uri)) = '1dxbkwouct4ehulyi9vhs' THEN 'Creepy Nuts no All Night Nippon'
                WHEN LOWER(TRIM(clean_show_uri)) = '1fgjw82dxf72gwjsanou90' THEN 'SixTONES no All Night Nippon Saturday Special'
                WHEN LOWER(TRIM(clean_show_uri)) = '1nbacx3nyaqjfmm2devmen' THEN 'Kyokyoraji'
                WHEN LOWER(TRIM(clean_show_uri)) = '2dr6n5shwm2sgaehyltw' THEN 'Sakuma Nobuyuki no All Night Nippon Zero'
                WHEN LOWER(TRIM(clean_show_uri)) = '2lk0pm4xgesvt24gvqgnwz' THEN 'Kaiwaroku'
                WHEN LOWER(TRIM(clean_show_uri)) = '36lsxl5qfrxlfdqfki3sq9' THEN 'Hoshino Gen no All Night Nippon'
                WHEN LOWER(TRIM(clean_show_uri)) = '392h0myfvmtndevzf2covc' THEN 'Kamaitachi no Hey! Master'
                WHEN LOWER(TRIM(clean_show_uri)) = '3mi1keaudu3tjvunivcmnh' THEN 'King Gnu Iguchi Satoru no All Night Nippon Zero'
                WHEN LOWER(TRIM(clean_show_uri)) = '3tvwmshacxweffgpzfajwx' THEN 'Sanshirō no All Night Nippon'
                WHEN LOWER(TRIM(clean_show_uri)) = '3v3gvtv9tyihcvcubmc5alb' THEN 'Shimofuri Myōjō no All Night Nippon'
                WHEN LOWER(TRIM(clean_show_uri)) = '3zltckmwprnnrod9chgxtc' THEN 'Ōdorī no All Night Nippon'
                WHEN LOWER(TRIM(clean_show_uri)) = '4hxv8gx5lgmbpmkp2jasgk' THEN 'All Night Nippon GOLD'
                WHEN LOWER(TRIM(clean_show_uri)) = '4qagjvubac25jovfezlwar' THEN 'Abareru-kun no Ignition Radio'
                WHEN LOWER(TRIM(clean_show_uri)) = '4r16jumyzx3jfbrjkgyfsk' THEN 'All Night Nippon'
                WHEN LOWER(TRIM(clean_show_uri)) = '5fotfgtsawek43srjeltmx' THEN 'All Night Nippon Cross'
                WHEN LOWER(TRIM(clean_show_uri)) = '5iqso4toroafie0xgr1v7p' THEN 'Nogizaka46 no All Night Nippon'
                WHEN LOWER(TRIM(clean_show_uri)) = '5xcikgndgu37piujtxpmd' THEN 'Nippon Hōsō "Momoiro Clover Z Momoclo Club xoxo"'
                WHEN LOWER(TRIM(clean_show_uri)) = '61je2l4irwo112sohic1pk' THEN 'AKB48 no All Night Nippon'
                WHEN LOWER(TRIM(clean_show_uri)) = '6bum3v2xy3vgolvqogaj6y' THEN 'Miki no All Night Nippon'
                WHEN LOWER(TRIM(clean_show_uri)) = '6cjea2w5dck9xlsjsdruai' THEN 'Ninety-Nine no All Night Nippon'
                WHEN LOWER(TRIM(clean_show_uri)) = '6h1xxqdrmlzlxwusyoima' THEN 'T.M. Revolution Nishikawa Takanori no All Night Nippon'
                WHEN LOWER(TRIM(clean_show_uri)) = '6lofzlgpxjbwflk6m98ftq' THEN 'WANIMA no All Night Nippon'
                WHEN LOWER(TRIM(clean_show_uri)) = '6ph5vt0gli6ogovensqdlw' THEN 'Kamishiraishi Mone no All Night Nippon'
                WHEN LOWER(TRIM(clean_show_uri)) = '6qyhtjqcwxcd4jsprwwjz' THEN 'King & Prince Nagase Ren no Radio GARDEN'
                WHEN LOWER(TRIM(clean_show_uri)) = '6skqktb0fdlnfm1dtywmfa' THEN 'Sexy Zone no All Night Nippon'
                WHEN LOWER(TRIM(clean_show_uri)) = '6x7wb2oe7ttljnfki0q9eu' THEN 'Keyakizaka46 Kochira Yūrakuchō Hoshizora Hōsōkyoku'
                WHEN LOWER(TRIM(clean_show_uri)) = '6zheyhwu1baqfvs4fqbbhx' THEN 'All Night Nippon'
                WHEN LOWER(TRIM(clean_show_uri)) = '7jdifqfos4dhic8cvhnxh' THEN 'Johnny''s WEST Kiriyama Akito・Nakama Junta no Recomen!'
                WHEN LOWER(TRIM(clean_show_uri)) = '6rexpxidshntq08lildpe6' THEN 'Nicole Fujita''s Tomorrow is Sunday'
                WHEN LOWER(TRIM(clean_show_uri)) = '6xappzzwpxuowdltnbdubr' THEN 'Naniwa Danshi''s First Love Radio!'
                WHEN LOWER(TRIM(clean_show_uri)) = '0dev2gujx4tgwblpjfix7j' THEN 'Naniwa Danshi''s All Night Nippon Premium'
                WHEN LOWER(TRIM(clean_show_uri)) = '5ovlwhwn3geq1fyvded9lr' THEN 'Hey! Say! JUMP''s Fab! -Music speaks.-'
                WHEN LOWER(TRIM(clean_show_uri)) = '6f34z6k3gal1ilirdxhdgt' THEN 'JO1''s All Night Nippon X (Cross)'
                WHEN LOWER(TRIM(clean_show_uri)) = '6tger2lvllybp1n0qytfnx' THEN 'Ryokuōshoku Shakai - Haruko Nagaya''s All Night Nippon X (Cross)'
                WHEN LOWER(TRIM(clean_show_uri)) = '1usizyj9kygtsmdmtm4zf7' THEN 'Ado''s All Night Nippon'
                WHEN LOWER(TRIM(clean_show_uri)) = '1lcigywohm39aw1ceqelht' THEN 'NCT 127 Yuta''s YUTA at Home'
                WHEN LOWER(TRIM(clean_show_uri)) = '5vs9xitabhvs6kr2awzriw' THEN 'ENHYPEN''s All Night Nippon X (Cross)'
                WHEN LOWER(TRIM(clean_show_uri)) = '1jlui2kd7wzszyduxfloma' THEN 'Yūki Yamada''s All Night Nippon'
                WHEN LOWER(TRIM(clean_show_uri)) = '3r6whgx5hixdustztppdpp' THEN 'Yūki Yamada''s All Night Nippon X (Cross)'
                WHEN LOWER(TRIM(clean_show_uri)) = '5njkphqrmyisdery7w9hpx' THEN 'EXIT''s All Night Nippon X (Cross)'
                WHEN LOWER(TRIM(clean_show_uri)) = '0tiao9intdfjofurpxptyb4' THEN 'Podcast "JUNK"'
                WHEN LOWER(TRIM(clean_show_uri)) = '1v3bjweab1ljnf5mcl0y6l' THEN 'Pekopa''s All Night Nippon X (Cross)'
                WHEN LOWER(TRIM(clean_show_uri)) = '7alicwyhvjahmyrpkemma' THEN 'Fuwa-chan''s All Night Nippon X (Cross)'
                WHEN LOWER(TRIM(clean_show_uri)) = '4pzm9bjzn4gh6tyo4dl9wf' THEN 'Fuwa-chan''s All Night Nippon 0 (ZERO)'
                WHEN LOWER(TRIM(clean_show_uri)) = '1ccaqzns6ftwk6v8dsf8aj' THEN 'Uika''s All Night Nippon 0 (ZERO)'
                WHEN LOWER(TRIM(clean_show_uri)) = '0ydyxy2hli2q2taohphrfa' THEN 'Listen anime "SPY×FAMILY"'

                /* STAGE 2: Garbage Collection. 
                Catch corrupted names (e.g., '?? a-LunA ??') that weren't fixed by a URI override.
                These specific checks avoid flagging legitimate names that contain one '?'.
                */
                WHEN raw_show_name LIKE '%???%' OR raw_show_name LIKE '?????%' OR raw_show_name LIKE '??%' THEN 'Unknown'

                /* STAGE 3: NULL/Blank Handling. 
                Set any remaining NULLs, empty strings, or 'N/A' literals to a standard 'Unknown'.
                */
                WHEN raw_show_name IS NULL OR raw_show_name IN ('', 'N/A', 'null') THEN 'Unknown'
                WHEN TRIM(raw_show_name) = '' THEN 'Unknown'
                ELSE TRIM(raw_show_name)
            END AS show_name,
            CASE 
                WHEN show_description IS NULL OR TRIM(show_description) IN ('', 'N/A', 'null') THEN 'Unknown' 
                WHEN show_description LIKE '%???%' THEN 'Unknown'
                WHEN show_description LIKE '?%' THEN 'Unknown'
                WHEN show_description LIKE '??%' THEN 'Unknown'
                WHEN TRIM(show_description) = '' THEN 'Unknown'
                ELSE TRIM(show_description)
            END AS show_description,
            CASE 
                WHEN TRIM(show_publisher) IS NULL OR show_publisher = '' THEN 'Unknown'
                ELSE TRIM(show_publisher)
            END AS show_publisher,
            
            /* Data Type Conversion: Source 'duration_ms' is a string/float (e.g., '12345.0').
            Strip the decimal and cast to INT for clean mathematics.
            */
            TRY_CAST(
                REPLACE(duration_ms, '.0', '') AS INT
            ) AS duration_ms,
            CASE 
                WHEN TRIM(explicit_content) IS NULL OR explicit_content = '' THEN 'Unknown'
                ELSE TRIM(explicit_content)
            END AS explicit_content,
            
            /* Standardize language arrays (e.g., "['en-US']", "['en']") 
            into a single, clean category (e.g., 'English').
            */
            CASE
                WHEN languages IN ('[''en'']', '[''en-IN'']', '[''en-IE'']', '[''en-AU'']', '[''en-GB'']', '[''en-NZ'']', '[''en-CA'']', '[''en-US'']', '[''en-PH'']') THEN 'English'
                WHEN languages IN ('[''es'']', '[''es-MX'']', '[''es-CL'']', '[''es-VE'']', '[''es-DO'']', '[''es-AR'']', '[''es-PA'']', '[''es-ES'']', '[''es-CO'']', '[''es-UY'']', '[''es-EC'']') THEN 'Spanish'
                WHEN languages IN ('[''fr'']', '[''fr-FR'']', '[''fr-CA'']') THEN 'French'
                WHEN languages IN ('[''pt'']', '[''pt-BR'']', '[''pt-PT'']') THEN 'Portuguese'
                WHEN languages IN ('[''de'']', '[''de-DE'']', '[''de-AT'']') THEN 'German'
                WHEN languages IN ('[''nl'']', '[''nl-NL'']') THEN 'Dutch'
                WHEN languages IN ('[''it'']', '[''it-IT'']') THEN 'Italian'
                WHEN languages IN ('[''zh-CN'']') THEN 'Chinese'
                WHEN languages IN ('[''hi'']', '[''hi-HI'']') THEN 'Hindi'
                WHEN languages = '[''ar'']' THEN 'Arabic'
                WHEN languages = '[''bn'']' THEN 'Bengali'
                WHEN languages = '[''te'']' THEN 'Telugu'
                WHEN languages = '[''gu'']' THEN 'Gujarati'
                WHEN languages = '[''ta'']' THEN 'Tamil'
                WHEN languages = '[''ur'']' THEN 'Urdu'
                WHEN languages = '[''pa'']' THEN 'Punjabi'
                WHEN languages = '[''mr'']' THEN 'Marathi'
                WHEN languages = '[''ml'']' THEN 'Malayalam'
                WHEN languages = '[''eu-ES'']' THEN 'Basque'
                WHEN languages IN ('[''tl'']', '[''fil'']') THEN 'Filipino'
                WHEN languages = '[''sv'']' THEN 'Swedish'
                WHEN languages = '[''ga'']' THEN 'Irish'
                WHEN languages IN ('[''id'']', '[''id-ID'']') THEN 'Indonesian'
                WHEN languages IN ('[''ja'']', '[''ja-JP'']') THEN 'Japanese'
                WHEN languages = '[''jv'']' THEN 'Javanese'
                WHEN languages IN ('[''pl'']', '[''pl-PL'']') THEN 'Polish'
                WHEN languages = '[''ca'']' THEN 'Catalan'
                WHEN languages = '[''und'']' THEN 'Unknown'
                ELSE 'Other'
            END AS languages,
            TRY_CAST(release_date AS DATE) AS release_date,
            CASE 
                WHEN TRIM(release_date_precision) IS NULL OR release_date_precision = '' THEN 'Unknown'
                ELSE TRIM(release_date_precision)
            END AS release_date_precision,
            CASE 
                WHEN TRIM(show_media_type) IS NULL OR show_media_type = '' THEN 'Unknown'
                ELSE TRIM(show_media_type)
            END AS show_media_type,
            
            /* Cast total episodes to INT, stripping any stray '.0' decimals. */
            TRY_CAST(
                REPLACE(show_total_episodes, '.0', '') AS INT
            ) AS show_total_episodes
        FROM 
            StandardizedData; -- Select from the clean CTE, not the raw bronze table

        SET @end_time = GETDATE();
        PRINT '=====================';
        PRINT 'Silver Layer Load Complete';
        PRINT 'Execution Time (ms): ' + CAST(DATEDIFF(ms, @start_time, @end_time) AS VARCHAR(100));
        PRINT '=====================';

    END TRY
    /*
    Standard T-SQL error handling block. If anything in the TRY block fails 
    (e.g., a data type conversion that TRY_CAST can't handle), this will
    catch it, print the error, and exit gracefully without crashing the procedure.
    */
    BEGIN CATCH
        PRINT '=====================';
        PRINT 'An error occurred loading the silver layer';
        PRINT 'Error Message: ' + ERROR_MESSAGE();
        PRINT '=====================';
    END CATCH
END;
