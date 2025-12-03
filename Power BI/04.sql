-- Клиенты с крупными заказами + частота заказов
SELECT 
    c.customerName,
    c.country,
    o.orderNumber,
    o.orderDate,
    o.status,
    t.total,
    customer_orders.order_count
FROM orders o
JOIN customers c ON o.customerNumber = c.customerNumber
JOIN (
    SELECT 
        orderNumber,
        SUM(quantityOrdered * priceEach) AS total
    FROM orderdetails
    GROUP BY orderNumber
    HAVING total > 59000
) t ON o.orderNumber = t.orderNumber
JOIN (
    SELECT 
        customerNumber,
        COUNT(*) AS order_count
    FROM orders
    GROUP BY customerNumber
) customer_orders ON c.customerNumber = customer_orders.customerNumber
ORDER BY t.total DESC;
