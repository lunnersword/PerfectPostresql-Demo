//
//  PostgresConnector+Advanced.swift
//  PerfectPostresql-Demo
//
//  Created by 沈永超 on 2020/7/15.
//

import Foundation

extension PostgresConnector {
    func views() {
//        Suppose the combined listing of weather records and city location is of particular interest to your application, but you do not want to type the query each time you need it. You can create a view over the query, which gives a name to the query that you can refer to like an ordinary table:
        let stmt = """
                    CREATE VIEW myview AS
                        SELECT city, temp_lo, temp_hi, prcp, date, location
                            FROM weather, cities
                            WHERE city = name;
                    """
        exec(stmt, params: [])
        selectFromWeather()
        
//        Making liberal use of views is a key aspect of good SQL database design. Views allow you to encapsulate the details of the structure of your tables, which might change as your application evolves, behind consistent interfaces.

//        Views can be used in almost any place a real table can be used. Building views upon other views is not uncommon.
    }
    
    func foreignKeys() {
//        Consider the following problem: You want to make sure that no one can insert rows in the weather table that do not have a matching entry in the cities table. This is called maintaining the referential integrity of your data. In simplistic database systems this would be implemented (if at all) by first looking at the cities table to check if a matching record exists, and then inserting or rejecting the new weather records. This approach has a number of problems and is very inconvenient, so PostgreSQL can do this for you.
//        let dropWeater = "DROP TABLE weather CASCADE;"
//        exec(dropWeater, params: [])
//        let dropCity = "DROP TABLE cities;"
//        exec(dropCity, params: [])
        
        let createCity = """
                        CREATE TABLE cities (
                            city varchar(80) primary key,
                            location point
                        );
                        """
        exec(createCity, params: [])

        let createWeather = """
                                CREATE TABLE weather (
                                    city varchar(80) references cities(city),
                                    temp_lo int,
                                    temp_hi int,
                                    prcp    real,
                                    date    date
                                );
                            """
        exec(createWeather, params: [])

        let tryToInsertWeather = "INSERT INTO weather VALUES ('Berkeley', 45, 53, 0.0, '1994-11-28');"
        exec(tryToInsertWeather, params: [])
    }
    
    func transactions() {
//        Transactions are a fundamental concept of all database systems. The essential point of a transaction is that it bundles multiple steps into a single, all-or-nothing operation. The intermediate states between the steps are not visible to other concurrent transactions, and if some failure occurs that prevents the transaction from completing, then none of the steps affect the database at all.
//        For example, consider a bank database that contains balances for various customer accounts, as well as total deposit balances for branches. Suppose that we want to record a payment of $100.00 from Alice's account to Bob's account. Simplifying outrageously, the SQL commands for this might look like:
        let _ = """
        UPDATE accounts SET balance = balance - 100.00
            WHERE name = 'Alice';
        UPDATE branches SET balance = balance - 100.00
            WHERE name = (SELECT branch_name FROM accounts WHERE name = 'Alice');
        UPDATE accounts SET balance = balance + 100.00
            WHERE name = 'Bob';
        UPDATE branches SET balance = balance + 100.00
            WHERE name = (SELECT branch_name FROM accounts WHERE name = 'Bob');
        """

//        In PostgreSQL, a transaction is set up by surrounding the SQL commands of the transaction with BEGIN and COMMIT commands. So our banking transaction would actually look like:
        let _ = """
            BEGIN;
            UPDATE accounts SET balance = balance - 100.00
                WHERE name = 'Alice';
            -- etc etc
            COMMIT;
            """
        
//        PostgreSQL actually treats every SQL statement as being executed within a transaction. If you do not issue a BEGIN command, then each individual statement has an implicit BEGIN and (if successful) COMMIT wrapped around it. A group of statements surrounded by BEGIN and COMMIT is sometimes called a transaction block.

//        It's possible to control the statements in a transaction in a more granular fashion through the use of savepoints. Savepoints allow you to selectively discard parts of the transaction, while committing the rest. After defining a savepoint with SAVEPOINT, you can if needed roll back to the savepoint with ROLLBACK TO. All the transaction's database changes between defining the savepoint and rolling back to it are discarded, but changes earlier than the savepoint are kept.

//        After rolling back to a savepoint, it continues to be defined, so you can roll back to it several times. Conversely, if you are sure you won't need to roll back to a particular savepoint again, it can be released, so the system can free some resources. Keep in mind that either releasing or rolling back to a savepoint will automatically release all savepoints that were defined after it.

//        All this is happening within the transaction block, so none of it is visible to other database sessions. When and if you commit the transaction block, the committed actions become visible as a unit to other sessions, while the rolled-back actions never become visible at all.
        
        let _ = """
            BEGIN;
            UPDATE accounts SET balance = balance - 100.00
                WHERE name = 'Alice';
            SAVEPOINT my_savepoint;
            UPDATE accounts SET balance = balance + 100.00
                WHERE name = 'Bob';
            -- oops ... forget that and use Wally's account
            ROLLBACK TO my_savepoint;
            UPDATE accounts SET balance = balance + 100.00
                WHERE name = 'Wally';
            COMMIT;
            """
    }
    
    func windows() {
//        A window function performs a calculation across a set of table rows that are somehow related to the current row. This is comparable to the type of calculation that can be done with an aggregate function. However, window functions do not cause rows to become grouped into a single output row like non-window aggregate calls would. Instead, the rows retain their separate identities. Behind the scenes, the window function is able to access more than just the current row of the query result.
//        Here is an example that shows how to compare each employee's salary with the average salary in his or her department:
        let _ = "SELECT depname, empno, salary, avg(salary) OVER (PARTITION BY depname) FROM empsalary;"
        
        let _ = """
              depname  | empno | salary |          avg
            -----------+-------+--------+-----------------------
             develop   |    11 |   5200 | 5020.0000000000000000
             develop   |     7 |   4200 | 5020.0000000000000000
             develop   |     9 |   4500 | 5020.0000000000000000
             develop   |     8 |   6000 | 5020.0000000000000000
             develop   |    10 |   5200 | 5020.0000000000000000
             personnel |     5 |   3500 | 3700.0000000000000000
             personnel |     2 |   3900 | 3700.0000000000000000
             sales     |     3 |   4800 | 4866.6666666666666667
             sales     |     1 |   5000 | 4866.6666666666666667
             sales     |     4 |   4800 | 4866.6666666666666667
            (10 rows)
            """
        
//        A window function call always contains an OVER clause directly following the window function's name and argument(s). This is what syntactically distinguishes it from a normal function or non-window aggregate. The OVER clause determines exactly how the rows of the query are split up for processing by the window function. The PARTITION BY clause within OVER divides the rows into groups, or partitions, that share the same values of the PARTITION BY expression(s). For each row, the window function is computed across the rows that fall into the same partition as the current row.
//        You can also control the order in which rows are processed by window functions using ORDER BY within OVER. (The window ORDER BY does not even have to match the order in which the rows are output.) Here is an example:
        let _ = """
                SELECT depname, empno, salary,
                       rank() OVER (PARTITION BY depname ORDER BY salary DESC)
                FROM empsalary;
                """
        let _ = """
                depname  | empno | salary | rank
                -----------+-------+--------+------
                 develop   |     8 |   6000 |    1
                 develop   |    10 |   5200 |    2
                 develop   |    11 |   5200 |    2
                 develop   |     9 |   4500 |    4
                 develop   |     7 |   4200 |    5
                 personnel |     2 |   3900 |    1
                 personnel |     5 |   3500 |    2
                 sales     |     1 |   5000 |    1
                 sales     |     4 |   4800 |    2
                 sales     |     3 |   4800 |    2
                (10 rows)
                """
//        As shown here, the rank function produces a numerical rank for each distinct ORDER BY value in the current row's partition, using the order defined by the ORDER BY clause. rank needs no explicit parameter, because its behavior is entirely determined by the OVER clause.

//        The rows considered by a window function are those of the “virtual table” produced by the query's FROM clause as filtered by its WHERE, GROUP BY, and HAVING clauses if any. For example, a row removed because it does not meet the WHERE condition is not seen by any window function. A query can contain multiple window functions that slice up the data in different ways using different OVER clauses, but they all act on the same collection of rows defined by this virtual table.

//        We already saw that ORDER BY can be omitted if the ordering of rows is not important. It is also possible to omit PARTITION BY, in which case there is a single partition containing all rows.

//        There is another important concept associated with window functions: for each row, there is a set of rows within its partition called its window frame. Some window functions act only on the rows of the window frame, rather than of the whole partition. By default, if ORDER BY is supplied then the frame consists of all rows from the start of the partition up through the current row, plus any following rows that are equal to the current row according to the ORDER BY clause. When ORDER BY is omitted the default frame consists of all rows in the partition. [4] Here is an example using sum:
        let _ = "SELECT salary, sum(salary) OVER () FROM empsalary;"
        let _ = """
                salary |  sum
                --------+-------
                   5200 | 47100
                   5000 | 47100
                   3500 | 47100
                   4800 | 47100
                   3900 | 47100
                   4200 | 47100
                   4500 | 47100
                   4800 | 47100
                   6000 | 47100
                   5200 | 47100
                (10 rows)
                """
//        Above, since there is no ORDER BY in the OVER clause, the window frame is the same as the partition, which for lack of PARTITION BY is the whole table; in other words each sum is taken over the whole table and so we get the same result for each output row. But if we add an ORDER BY clause, we get very different results:
        let _ = "SELECT salary, sum(salary) OVER (ORDER BY salary) FROM empsalary;"
        let _ = """
                salary |  sum
                --------+-------
                   3500 |  3500
                   3900 |  7400
                   4200 | 11600
                   4500 | 16100
                   4800 | 25700
                   4800 | 25700
                   5000 | 30700
                   5200 | 41100
                   5200 | 41100
                   6000 | 47100
                (10 rows)
                """
//        Here the sum is taken from the first (lowest) salary up through the current one, including any duplicates of the current one (notice the results for the duplicated salaries).

//        Window functions are permitted only in the SELECT list and the ORDER BY clause of the query. They are forbidden elsewhere, such as in GROUP BY, HAVING and WHERE clauses. This is because they logically execute after the processing of those clauses. Also, window functions execute after non-window aggregate functions. This means it is valid to include an aggregate function call in the arguments of a window function, but not vice versa.

//        If there is a need to filter or group rows after the window calculations are performed, you can use a sub-select. For example:
        let _ = """
                SELECT depname, empno, salary, enroll_date
                FROM
                  (SELECT depname, empno, salary, enroll_date,
                          rank() OVER (PARTITION BY depname ORDER BY salary DESC, empno) AS pos
                     FROM empsalary
                  ) AS ss
                WHERE pos < 3;
                """
//        The above query only shows the rows from the inner query having rank less than 3.

//        When a query involves multiple window functions, it is possible to write out each one with a separate OVER clause, but this is duplicative and error-prone if the same windowing behavior is wanted for several functions. Instead, each windowing behavior can be named in a WINDOW clause and then referenced in OVER. For example:
        let _ = """
                SELECT sum(salary) OVER w, avg(salary) OVER w
                FROM empsalary
                WINDOW w AS (PARTITION BY depname ORDER BY salary DESC);
                """
    }
    
    func inheritance() {
//        Inheritance is a concept from object-oriented databases. It opens up interesting new possibilities of database design.
//        Let's create two tables: A table cities and a table capitals. Naturally, capitals are also cities, so you want some way to show the capitals implicitly when you list all cities. If you're really clever you might invent some scheme like this:
        let _ = """
                CREATE TABLE capitals (
                  name       text,
                  population real,
                  elevation  int,    -- (in ft)
                  state      char(2)
                );

                CREATE TABLE non_capitals (
                  name       text,
                  population real,
                  elevation  int     -- (in ft)
                );

                CREATE VIEW cities AS
                  SELECT name, population, elevation FROM capitals
                    UNION
                  SELECT name, population, elevation FROM non_capitals;
                """
//        This works OK as far as querying goes, but it gets ugly when you need to update several rows, for one thing.

//        A better solution is this:
        let _ = """
                CREATE TABLE cities (
                  name       text,
                  population real,
                  elevation  int     -- (in ft)
                );

                CREATE TABLE capitals (
                  state      char(2)
                ) INHERITS (cities);
                """
//        In this case, a row of capitals inherits all columns (name, population, and elevation) from its parent, cities. The type of the column name is text, a native PostgreSQL type for variable length character strings. State capitals have an extra column, state, that shows their state. In PostgreSQL, a table can inherit from zero or more other tables.
//        For example, the following query finds the names of all cities, including state capitals, that are located at an elevation over 500 feet:
        let _ = """
                SELECT name, elevation
                FROM cities
                WHERE elevation > 500;
                """
//        which returns:
        let _ = """
                   name    | elevation
                -----------+-----------
                 Las Vegas |      2174
                 Mariposa  |      1953
                 Madison   |       845
                (3 rows)
                """
//        On the other hand, the following query finds all the cities that are not state capitals and are situated at an elevation over 500 feet:

        let _ = """
                SELECT name, elevation
                FROM ONLY cities
                WHERE elevation > 500;
                """
        let _ = """
                  name    | elevation
                -----------+-----------
                 Las Vegas |      2174
                 Mariposa  |      1953
                (2 rows)
                """
//        Here the ONLY before cities indicates that the query should be run over only the cities table, and not tables below cities in the inheritance hierarchy. Many of the commands that we have already discussed — SELECT, UPDATE, and DELETE — support this ONLY notation.
    }
}
