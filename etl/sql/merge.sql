MERGE INTO `{{ table }}` t
USING `{{ temp }}` tmp
ON t.matchId = tmp.matchId
WHEN NOT MATCHED THEN INSERT ROW
