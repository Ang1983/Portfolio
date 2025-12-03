-- Крупные заказы + статус и время выполнения (если shipped)
SELECT 
    o.orderNumber,
    o.orderDate,
    o.status,
    o.shippedDate,
    julianday(o.shippedDate) - julianday(o.orderDate) AS days_to_ship,
    t.total
FROM orders o
JOIN (
    SELECT 
        orderNumber,
        SUM(quantityOrdered * priceEach) AS total
    FROM orderdetails
    GROUP BY orderNumber
    HAVING total > 59000
) t ON o.orderNumber = t.orderNumber
ORDER BY t.total DESC;
