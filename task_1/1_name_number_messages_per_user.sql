-- The names and the number of messages sent by each user

SELECT 
    u.name AS user_name,
    COUNT(m.MessageID) AS messages_sent
FROM Users u
JOIN Messages m ON u.UserID = m.UserIDSender
GROUP BY u.name
ORDER BY COUNT(m.MessageID) DESC;
