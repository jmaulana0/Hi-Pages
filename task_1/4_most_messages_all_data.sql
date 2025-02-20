-- For the conversation with the most messages: all user data and message contents ordered
-- chronologically so one can follow the whole conversation

SELECT 
    t.ThreadID,
    t.Subject,
    m.MessageID,
    m.messageContent,
    m.dateSent,
    uSender.UserID AS senderID,
    uSender.name   AS senderName,
    uRecipient.UserID AS recipientID,
    uRecipient.name   AS recipientName
FROM Threads t
JOIN Messages m 
    ON t.ThreadID = m.ThreadID
JOIN Users uSender 
    ON m.UserIDSender = uSender.UserID
JOIN Users uRecipient 
    ON m.UserIDRecipient = uRecipient.UserID
WHERE t.ThreadID = (
-- Sub-query to find conversation with the most messages
-- could have also used a CTE at the start (e.g. With MostMessages ( ))
    SELECT t2.ThreadID
    FROM Threads t2
    JOIN Messages m2 ON t2.ThreadID = m2.ThreadID
    GROUP BY t2.ThreadID
    ORDER BY COUNT(m2.MessageID) DESC
    LIMIT 1
)
ORDER BY m.dateSent;
