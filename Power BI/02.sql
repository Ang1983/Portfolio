-- Заказы > 59 000 ₽ + сравнение с медианой всех заказов
SELECT 
    orderNumber,
    total,
    (SELECT MEDIAN(total) FROM (
        SELECT SUM(quantityOrdered * priceEach) AS total
        FROM orderdetails
        GROUP BY orderNumber
    )) AS median_order_value,
    CASE 
        WHEN total > 2.5 * (SELECT MEDIAN(total) FROM (
            SELECT SUM(quantityOrdered * priceEach) AS total
            FROM orderdetails
            GROUP BY orderNumber
        )) THEN 'very high'
        ELSE 'high'
    END AS tier
FROM (
    SELECT 
        orderNumber,
        SUM(quantityOrdered * priceEach) AS total
    FROM orderdetails
    GROUP BY orderNumber
) t
WHERE total > 59000
ORDER BY total DESC;
