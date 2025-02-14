SELECT 
    DAYNAME(m.dateSent) AS weekday,
    COUNT(*) AS total_messages
FROM Messages m
GROUP BY DAYNAME(m.dateSent)
-- ORDER BY FIELD(DAYNAME(m.dateSent), 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday');
-- Removed Order By as not needed for exercise + would want to test working with real code --
