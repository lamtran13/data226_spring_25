SELECT
  sessionId,
  ts
FROM {{ source('raw', 'session_timestamp') }}
WHERE sessionId IS NOT NULL

