-- Менеджеры и их клиенты + кол-во клиентов на менеджера
SELECT 
    e.firstName,
    e.lastName,
    e.jobTitle,
    c.customerName,
    COUNT(c.customerNumber) OVER (PARTITION BY e.employeeNumber) AS clients_assigned
FROM employees e
LEFT JOIN customers c ON e.employeeNumber = c.salesRepEmployeeNumber
ORDER BY e.lastName, c.customerName;
