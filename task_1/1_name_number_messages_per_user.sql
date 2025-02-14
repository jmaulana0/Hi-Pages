SELECT 
    u.name AS user_name,
    COUNT(m.MessageID) AS messages_sent
FROM Users u
WHERE 1=1
JOIN Messages m ON u.UserID = m.UserIDSender
GROUP BY u.UserID, u.name
ORDER BY messages_sent DESC;
