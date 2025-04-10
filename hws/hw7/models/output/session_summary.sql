{{ config(materialized='table') }}

SELECT u.userId, u.sessionId, u.channel, st.ts
FROM {{ ref("user_session_channel") }} u
JOIN {{ ref("session_timestamp") }} st
  ON u.sessionId = st.sessionId

