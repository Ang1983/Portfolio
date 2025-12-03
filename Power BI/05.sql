-- Топ-10 товаров по выручке + средняя цена и объём
SELECT 
    p.productName,
    p.productLine,
    SUM(od.quantityOrdered) AS total_quantity,
    AVG(od.priceEach) AS avg_price,
    SUM(od.quantityOrdered * od.priceEach) AS total_revenue
FROM orderdetails od
JOIN products p ON od.productCode = p.productCode
GROUP BY p.productCode, p.productName, p.productLine
ORDER BY total_revenue DESC
LIMIT 10;
