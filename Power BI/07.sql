-- Руководители и их подчинённые + уровень должности
SELECT 
    boss.firstName || ' ' || boss.lastName AS manager,
    boss.jobTitle AS manager_role,
    emp.firstName || ' ' || emp.lastName AS report,
    emp.jobTitle AS report_role
FROM employees boss
LEFT JOIN employees emp ON boss.employeeNumber = emp.reportsTo
WHERE boss.jobTitle LIKE '%Manager%' OR boss.jobTitle LIKE '%VP%'
ORDER BY boss.lastName, emp.lastName;
