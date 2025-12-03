-- Топ-10 позиций по выручке + доля в общем обороте
SELECT 
    od.orderNumber,
    p.productName,
    od.quantityOrdered,
    od.priceEach,
    od.quantityOrdered * od.priceEach AS item_revenue,
    ROUND(
        (od.quantityOrdered * od.priceEach) * 100.0 / 
        SUM(od.quantityOrdered * od.priceEach) OVER (),
        2
    ) AS revenue_share_percent
FROM orderdetails od
JOIN products p ON od.productCode = p.productCode
ORDER BY item_revenue DESC
LIMIT 10;
