-- The most recent message from each thread that has no response yet

SELECT
  m.*
FROM Messages m
LEFT JOIN Messages m2 
    ON m.ThreadID = m2.ThreadID 
   AND m.dateSent < m2.dateSent
-- m.dateSent is earlier than m.dateSent to show which message isn't followed by another
WHERE
  m2.MessageID IS NULL;
-- no response yet