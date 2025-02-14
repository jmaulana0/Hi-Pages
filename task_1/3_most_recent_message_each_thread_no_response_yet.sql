-- Do this using where userIDRecipient 

SELECT
  m.*
FROM Messages m
LEFT JOIN Messages m2 
    ON m.ThreadID = m2.ThreadID 
   AND m.dateSent < m2.dateSent
WHERE m2.MessageID IS NULL;
