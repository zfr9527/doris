package mv.pre_rewrite
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

suite("mtmv_rbo_pre_rewrite") {
    String db = context.config.getDbNameByFile(context.file)
    sql "use ${db}"
    sql "set runtime_filter_mode=OFF";
    sql "SET ignore_shape_nodes='PhysicalDistribute,PhysicalProject'"


    def tb_name = "employees"
    sql """drop table if exists ${tb_name};"""
    sql """
        CREATE TABLE ${tb_name} (
            id INT,
            name VARCHAR(100),
            department_id INT,
            salary DECIMAL(10, 2),
            commission_pct DECIMAL(3, 2),
            hire_date DATE,
            manager_id INT,
            status VARCHAR(20),
            email VARCHAR(100),
            phone VARCHAR(20),
            address VARCHAR(200),
            performance_rating INT,
            last_promotion_date DATE,
            years_of_experience INT
        ) ENGINE=OLAP
        DUPLICATE KEY(`is`)
        COMMENT "OLAP"
        DISTRIBUTED BY HASH(`id`) BUCKETS AUTO
        PROPERTIES (
          "replication_num" = "1"
        );
        """
    sql """
        INSERT INTO ${tb_name} VALUES
        (1, 'John Doe', 10, 50000.00, 0.10, '2020-01-15', NULL, 'Active', 'john.doe@company.com', '123-456-7890', '123 Main St, New York, NY', 4, '2021-06-01', 5),
        (2, 'Jane Smith', 10, 60000.00, 0.15, '2019-03-20', 1, 'Active', 'jane.smith@company.com', '123-456-7891', '456 Oak Ave, New York, NY', 5, '2022-01-15', 6),
        (3, 'Robert Johnson', 20, 75000.00, NULL, '2018-05-10', 1, 'Active', 'robert.johnson@company.com', '123-456-7892', '789 Pine Rd, New York, NY', 3, '2020-09-10', 7),
        (4, 'Sarah Williams', 20, 80000.00, 0.20, '2017-11-30', 3, 'On Leave', 'sarah.williams@company.com', '123-456-7893', '101 Elm St, New York, NY', 4, '2019-12-01', 8),
        (5, 'Michael Brown', 30, 45000.00, 0.05, '2021-02-28', 3, 'Active', 'michael.brown@company.com', '123-456-7894', '202 Maple Dr, New York, NY', 2, NULL, 3),
        (6, 'Emily Davis', 30, 55000.00, NULL, '2020-07-15', 4, 'Active', 'emily.davis@company.com', '123-456-7895', '303 Cedar Ln, New York, NY', 5, '2022-03-01', 4),
        (7, 'David Wilson', 40, 90000.00, 0.25, '2016-09-05', 4, 'Active', 'david.wilson@company.com', '123-456-7896', '404 Birch Ave, New York, NY', 5, '2018-07-15', 9),
        (8, 'Jennifer Taylor', 40, 95000.00, 0.30, '2015-12-12', 7, 'Terminated', 'jennifer.taylor@company.com', '123-456-7897', '505 Spruce St, New York, NY', 4, '2017-11-20', 10),
        (9, 'James Anderson', 50, 70000.00, 0.12, '2019-08-22', 7, 'Active', 'james.anderson@company.com', '123-456-7898', '606 Willow Way, New York, NY', 3, '2021-04-10', 6),
        (10, 'Lisa Martinez', 50, 65000.00, 0.08, '2020-04-17', 9, 'Active', 'lisa.martinez@company.com', '123-456-7899', '707 Poplar Blvd, New York, NY', 4, NULL, 5),
        (11, 'Mark Thompson', 10, 48000.00, 0.07, '2021-06-30', 2, 'Active', 'mark.thompson@company.com', '123-456-7800', '808 Magnolia Ct, New York, NY', 3, NULL, 4),
        (12, 'Amy Rodriguez', 20, 72000.00, 0.18, '2019-11-15', 3, 'Active', 'amy.rodriguez@company.com', '123-456-7801', '909 Redwood Dr, New York, NY', 5, '2021-08-22', 6),
        (13, 'Kevin White', 30, 52000.00, NULL, '2020-09-22', 5, 'Active', 'kevin.white@company.com', '123-456-7802', '1010 Sequoia Ln, New York, NY', 2, NULL, 4),
        (14, 'Laura Lee', 40, 88000.00, 0.22, '2018-07-10', 7, 'Active', 'laura.lee@company.com', '123-456-7803', '1111 Cypress St, New York, NY', 4, '2020-05-15', 7),
        (15, 'Steven Harris', 50, 68000.00, 0.10, '2020-03-05', 9, 'Active', 'steven.harris@company.com', '123-456-7804', '1212 Hemlock Way, New York, NY', 3, NULL, 5),
        (16, 'Maria Garcia', 10, 53000.00, 0.09, '2021-01-10', 2, 'Active', 'maria.garcia@company.com', '123-456-7805', '1313 Juniper Ave, New York, NY', 4, NULL, 4),
        (17, 'Daniel Clark', 20, 77000.00, 0.16, '2018-12-03', 3, 'Active', 'daniel.clark@company.com', '123-456-7806', '1414 Fir St, New York, NY', 5, '2020-10-05', 7),
        (18, 'Jessica Lewis', 30, 58000.00, NULL, '2020-05-20', 5, 'Active', 'jessica.lewis@company.com', '123-456-7807', '1515 Pinecone Dr, New York, NY', 3, NULL, 4),
        (19, 'Matthew Walker', 40, 92000.00, 0.24, '2017-04-18', 7, 'Active', 'matthew.walker@company.com', '123-456-7808', '1616 Acorn Ln, New York, NY', 4, '2019-06-12', 8),
        (20, 'Ashley Hall', 50, 67000.00, 0.11, '2019-10-25', 9, 'Active', 'ashley.hall@company.com', '123-456-7809', '1717 Chestnut Blvd, New York, NY', 5, '2021-11-30', 6);
        """

    sql """analyze table ${tb_name} with sync"""

    def mtmv_sql_lists = [
            """
        -- 基础SELECT
        SELECT id, name, salary FROM employees;""",
            """
        -- 包含表达式计算
        SELECT id, name, salary, salary * 12 AS annual_salary FROM employees;""",
            """
        -- 包含普通函数计算
        SELECT id, UPPER(name) AS upper_name, salary FROM employees;""",
            """
        -- 包含聚合函数
        SELECT department_id, SUM(salary) AS total_salary FROM employees GROUP BY department_id;""",

                          """
        -- 包含CASE WHEN
        SELECT id, name, salary, 
               CASE WHEN salary > 70000 THEN 'High' 
                    WHEN salary > 50000 THEN 'Medium' 
                    ELSE 'Low' 
               END AS salary_grade 
        FROM employees;""",

                          """
        -- 包含子查询
        SELECT id, name, salary, 
               (SELECT AVG(salary) FROM employees) AS avg_salary 
        FROM employees;
        """,

                          """
        -- 基础FROM
        SELECT * FROM employees;""",
                          """
        -- FROM子查询
        SELECT * FROM (SELECT id, name, salary FROM employees) AS emp_subquery;""",
                          """
        -- FROM带条件的子查询
        SELECT * FROM (
            SELECT id, name, salary 
            FROM employees 
            WHERE salary > 60000
        ) AS high_earners;
        """,
                          """
        -- 基础WHERE
        SELECT * FROM employees WHERE salary > 60000;""",
                          """
        -- WHERE带AND/OR
        SELECT * FROM employees WHERE salary > 60000 AND department_id = 10;""",
                          """
        -- WHERE带函数计算
        SELECT * FROM employees WHERE YEAR(hire_date) > 2019;""",
                          """
        -- WHERE带IS NULL
        SELECT * FROM employees WHERE commission_pct IS NULL;""",
                          """
        -- WHERE带算术运算
        SELECT * FROM employees WHERE salary + 10000 > 70000;""",
                          """
        -- WHERE带IN (值列表)
        SELECT * FROM employees WHERE department_id IN (10, 20);""",
                          """
        -- WHERE带IN (子查询)
        SELECT * FROM employees 
        WHERE department_id IN (
            SELECT department_id 
            FROM employees 
            GROUP BY department_id 
            HAVING COUNT(*) > 2
        );""",
                          """
        -- WHERE带EXISTS
        SELECT * FROM employees e1
        WHERE EXISTS (
            SELECT 1 
            FROM employees e2 
            WHERE e2.salary > 80000 AND e2.department_id = e1.department_id
        );
        """,
                          """
        -- 基础GROUP BY
        SELECT department_id, AVG(salary) FROM employees GROUP BY department_id;""",
                          """
        -- GROUP BY带函数
        SELECT YEAR(hire_date) AS hire_year, AVG(salary) 
        FROM employees 
        GROUP BY YEAR(hire_date);""",
                          """
        -- GROUP BY带表达式
        SELECT department_id, 
               CASE WHEN salary > 70000 THEN 'High' ELSE 'Normal' END AS salary_level,
               COUNT(*)
        FROM employees
        GROUP BY department_id, 
                 CASE WHEN salary > 70000 THEN 'High' ELSE 'Normal' END;""",
                          """
        -- GROUP BY带GROUPING SETS
        SELECT department_id, YEAR(hire_date) AS hire_year, AVG(salary)
        FROM employees
        GROUP BY GROUPING SETS ((department_id), (YEAR(hire_date)), ());""",
                          """
        -- GROUP BY带CUBE
        SELECT department_id, YEAR(hire_date) AS hire_year, AVG(salary)
        FROM employees
        GROUP BY CUBE (department_id, YEAR(hire_date));""",
                          """
        -- GROUP BY带字符串函数
        SELECT SUBSTRING(name, 1, 1) AS first_letter, COUNT(*)
        FROM employees
        GROUP BY SUBSTRING(name, 1, 1);
        """,
                          """
        -- 基础HAVING
        SELECT department_id, AVG(salary) 
        FROM employees 
        GROUP BY department_id 
        HAVING AVG(salary) > 60000;""",
                          """
        -- HAVING带AND
        SELECT department_id, AVG(salary) 
        FROM employees 
        GROUP BY department_id 
        HAVING AVG(salary) > 60000 AND COUNT(*) > 2;""",
                          """
        -- HAVING带子查询
        SELECT department_id, AVG(salary) 
        FROM employees 
        GROUP BY department_id 
        HAVING AVG(salary) > (SELECT AVG(salary) FROM employees);""",
                          """
        -- HAVING带字符串函数
        SELECT department_id, COUNT(*) 
        FROM employees 
        GROUP BY department_id 
        HAVING COUNT(*) > 2 AND MAX(LENGTH(name)) > 10;
        """,
                          """
        -- 基础ORDER BY
        SELECT * FROM employees ORDER BY salary DESC;""",
                          """
        -- ORDER BY带函数
        SELECT * FROM employees ORDER BY YEAR(hire_date) DESC;""",
                          """
        -- ORDER BY带CASE WHEN
        SELECT id, name, salary,
               CASE WHEN salary > 70000 THEN 'High' 
                    WHEN salary > 50000 THEN 'Medium' 
                    ELSE 'Low' 
               END AS salary_grade
        FROM employees
        ORDER BY CASE WHEN salary > 70000 THEN 1 
                      WHEN salary > 50000 THEN 2 
                      ELSE 3 
                 END;""",
                          """
        -- ORDER BY带聚合函数
        SELECT department_id, AVG(salary) AS avg_salary
        FROM employees
        GROUP BY department_id
        ORDER BY AVG(salary) DESC;""",
                          """
        -- ORDER BY带表达式
        SELECT id, name, salary, salary * 12 AS annual_salary
        FROM employees
        ORDER BY salary * 12 DESC;""",
                          """
        -- ORDER BY带子查询
        SELECT e1.id, e1.name, e1.salary
        FROM employees e1
        ORDER BY (SELECT COUNT(*) FROM employees e2 WHERE e2.department_id = e1.department_id) DESC;
        """,
                          """
        -- 基础LIMIT
        SELECT * FROM employees""",
                          """
        -- LIMIT带偏移量
        SELECT * FROM employees""",
                          """
        -- LIMIT 0
        SELECT * FROM employees;""",
                          """
        -- LIMIT带子查询
        SELECT * FROM employees ;
        """
    ]

    def sql_lists = [
            """
        -- 基础SELECT
        SELECT id, name, salary FROM employees;""",

            """
        -- 包含表达式计算
        SELECT id, name, salary, salary * 12 AS annual_salary FROM employees;""",

            """
        -- 包含普通函数计算
        SELECT id, UPPER(name) AS upper_name, salary FROM employees;""",

            """
        -- 包含聚合函数
        SELECT department_id, SUM(salary) AS total_salary FROM employees GROUP BY department_id;""",

            """
        -- 包含CASE WHEN
        SELECT id, name, salary, 
               CASE WHEN salary > 70000 THEN 'High' 
                    WHEN salary > 50000 THEN 'Medium' 
                    ELSE 'Low' 
               END AS salary_grade 
        FROM employees;""",

            """
        -- 包含子查询
        SELECT id, name, salary, 
               (SELECT AVG(salary) FROM employees) AS avg_salary 
        FROM employees;
        """,

            """
        -- 基础FROM
        SELECT * FROM employees;""",
            """
        -- FROM子查询
        SELECT * FROM (SELECT id, name, salary FROM employees) AS emp_subquery;""",
            """
        -- FROM带条件的子查询
        SELECT * FROM (
            SELECT id, name, salary 
            FROM employees 
            WHERE salary > 60000
        ) AS high_earners;
        """,
            """
        -- 基础WHERE
        SELECT * FROM employees WHERE salary > 60000;""",
            """
        -- WHERE带AND/OR
        SELECT * FROM employees WHERE salary > 60000 AND department_id = 10;""",
            """
        -- WHERE带函数计算
        SELECT * FROM employees WHERE YEAR(hire_date) > 2019;""",
            """
        -- WHERE带IS NULL
        SELECT * FROM employees WHERE commission_pct IS NULL;""",
            """
        -- WHERE带算术运算
        SELECT * FROM employees WHERE salary + 10000 > 70000;""",
            """
        -- WHERE带IN (值列表)
        SELECT * FROM employees WHERE department_id IN (10, 20);""",
            """
        -- WHERE带IN (子查询)
        SELECT * FROM employees 
        WHERE department_id IN (
            SELECT department_id 
            FROM employees 
            GROUP BY department_id 
            HAVING COUNT(*) > 2
        );""",
            """
        -- WHERE带EXISTS
        SELECT * FROM employees e1
        WHERE EXISTS (
            SELECT 1 
            FROM employees e2 
            WHERE e2.salary > 80000 AND e2.department_id = e1.department_id
        );
        """,
            """
        -- 基础GROUP BY
        SELECT department_id, AVG(salary) FROM employees GROUP BY department_id;""",
            """
        -- GROUP BY带函数
        SELECT YEAR(hire_date) AS hire_year, AVG(salary) 
        FROM employees 
        GROUP BY YEAR(hire_date);""",
            """
        -- GROUP BY带表达式
        SELECT department_id, 
               CASE WHEN salary > 70000 THEN 'High' ELSE 'Normal' END AS salary_level,
               COUNT(*)
        FROM employees
        GROUP BY department_id, 
                 CASE WHEN salary > 70000 THEN 'High' ELSE 'Normal' END;""",
            """
        -- GROUP BY带GROUPING SETS
        SELECT department_id, YEAR(hire_date) AS hire_year, AVG(salary)
        FROM employees
        GROUP BY GROUPING SETS ((department_id), (YEAR(hire_date)), ());""",
            """
        -- GROUP BY带CUBE
        SELECT department_id, YEAR(hire_date) AS hire_year, AVG(salary)
        FROM employees
        GROUP BY CUBE (department_id, YEAR(hire_date));""",
            """
        -- GROUP BY带字符串函数
        SELECT SUBSTRING(name, 1, 1) AS first_letter, COUNT(*)
        FROM employees
        GROUP BY SUBSTRING(name, 1, 1);
        """,
            """
        -- 基础HAVING
        SELECT department_id, AVG(salary) 
        FROM employees 
        GROUP BY department_id 
        HAVING AVG(salary) > 60000;""",
            """
        -- HAVING带AND
        SELECT department_id, AVG(salary) 
        FROM employees 
        GROUP BY department_id 
        HAVING AVG(salary) > 60000 AND COUNT(*) > 2;""",
            """
        -- HAVING带子查询
        SELECT department_id, AVG(salary) 
        FROM employees 
        GROUP BY department_id 
        HAVING AVG(salary) > (SELECT AVG(salary) FROM employees);""",
            """
        -- HAVING带字符串函数
        SELECT department_id, COUNT(*) 
        FROM employees 
        GROUP BY department_id 
        HAVING COUNT(*) > 2 AND MAX(LENGTH(name)) > 10;
        """,
            """
        -- 基础ORDER BY
        SELECT * FROM employees ORDER BY salary DESC;""",
            """
        -- ORDER BY带函数
        SELECT * FROM employees ORDER BY YEAR(hire_date) DESC;""",
            """
        -- ORDER BY带CASE WHEN
        SELECT id, name, salary,
               CASE WHEN salary > 70000 THEN 'High' 
                    WHEN salary > 50000 THEN 'Medium' 
                    ELSE 'Low' 
               END AS salary_grade
        FROM employees
        ORDER BY CASE WHEN salary > 70000 THEN 1 
                      WHEN salary > 50000 THEN 2 
                      ELSE 3 
                 END;""",
            """
        -- ORDER BY带聚合函数
        SELECT department_id, AVG(salary) AS avg_salary
        FROM employees
        GROUP BY department_id
        ORDER BY AVG(salary) DESC;""",
            """
        -- ORDER BY带表达式
        SELECT id, name, salary, salary * 12 AS annual_salary
        FROM employees
        ORDER BY salary * 12 DESC;""",
            """
        -- ORDER BY带子查询
        SELECT e1.id, e1.name, e1.salary
        FROM employees e1
        ORDER BY (SELECT COUNT(*) FROM employees e2 WHERE e2.department_id = e1.department_id) DESC;
        """,
            """
        -- 基础LIMIT
        SELECT * FROM employees LIMIT 5;""",
            """
        -- LIMIT带偏移量
        SELECT * FROM employees LIMIT 3, 5;""",
            """
        -- LIMIT 0
        SELECT * FROM employees LIMIT 0;""",
            """
        -- LIMIT带子查询
        SELECT * FROM employees 
        LIMIT (SELECT COUNT(*) FROM employees WHERE department_id = 10);
        """
    ]

    for (int i = 0; i < mtmv_sql_lists.size(); i++) {
        async_mv_rewrite_success(db, mtmv_sql_lists[i], sql_lists[i], "rbo_mtmv_${i}", [TRY_IN_RBO])
        sql """ DROP MATERIALIZED VIEW IF EXISTS rbo_mtmv_${i}"""
    }


}

