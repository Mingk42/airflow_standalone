-- DT=$1

USE history_db;

INSERT INTO cmd_usage
	SELECT
        	COALESCE(STR_TO_DATE(dt,'%Y-%m-%d'), STR_TO_DATE('1970-01-01','%Y-%m-%d')) dt,
                command,
                CAST(cnt as unsigned) cnt
	FROM tmp_cmd_usage
--        WHERE dt='$DT'
	WHERE dt = '2024-07-17'
;
