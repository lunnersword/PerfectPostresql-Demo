//
//  PostgresHelper.swift
//  PerfectTemplate
//
//  Created by 沈永超 on 2020/7/15.
//

import Foundation
import PerfectPostgreSQL

public struct PostgresConnector {
    public static var host: String        = "localhost"
    public static var username: String    = ""
    public static var password: String    = ""
    public static var database: String    = "mydb"
    public static var port: Int            = 5432
    
    public static var connectionString: String {
        return "postgresql://\(PostgresConnector.username.stringByEncodingURL):\(PostgresConnector.password.stringByEncodingURL)@\(PostgresConnector.host.stringByEncodingURL):\(PostgresConnector.port)/\(PostgresConnector.database.stringByEncodingURL)"
    }

    public init(){}
    
    func createWeather() {
        let stmt = "CREATE TABLE \"weather\" (\"city\" varchar(80) NOT NULL, \"temp_lo\" int, \"temp_hi\" int, \"prcp\" real, \"date\" date);"
        exec(stmt, params: [])
    }
    
    func insertIntoWeather(city: String, temp_lo: Int, temp_hi: Int, prcp: Double, date: String) {
        let stmt = "INSERT INTO \"weather\" VALUES ($1, $2, $3, $4, $5)"
        exec(stmt, params: [city, temp_lo, temp_hi, prcp, date])
    }
    
    func selectFromWeather() {
        let stmt = "SELECT * FROM weather"
        let server = connect()
        let result = server.exec(statement: stmt)
        let num = result.numTuples()
        print("City     |   temp_lo     |   temp_hi     |       prcp    |   date       ")
        for x in 0..<num {
            let city = result.getFieldString(tupleIndex: x, fieldIndex: 0) ?? ""
            let temp_lo = result.getFieldInt(tupleIndex: x, fieldIndex: 1) ?? 0
            let temp_hi = result.getFieldInt(tupleIndex: x, fieldIndex: 2) ?? 0
            let prcp = result.getFieldDouble(tupleIndex: x, fieldIndex: 3) ?? 0.0
            let date = result.getFieldString(tupleIndex: x, fieldIndex: 4) ?? ""
            print("\(city)      |   \(temp_lo)      |   \(temp_hi)      |   \(prcp)     |   \(date) ")
        }
        
    }
    
    func select1() {
        let stmt = "SELECT city, (temp_hi+temp_lo)/2 AS temp_avg, date FROM weather;"
        let server = connect()
        let result = server.exec(statement: stmt)
        let num = result.numTuples()
        print("City     |   temp_avg    |   date       ")
        for x in 0..<num {
            let city = result.getFieldString(tupleIndex: x, fieldIndex: 0) ?? ""
            let temp_avg = result.getFieldInt(tupleIndex: x, fieldIndex: 1) ?? 0
            let date = result.getFieldString(tupleIndex: x, fieldIndex: 2) ?? ""
            print("\(city)      |   \(temp_avg)     |   \(date) ")
        }
    }
    
    func select2() {
        let stmt = "SELECT * FROM weather WHERE city = \'San Francisco\' AND prcp > 0.0;"
        let server = connect()
        let result = server.exec(statement: stmt)
        let num = result.numTuples()
        print("City     |   temp_lo     |   temp_hi     |       prcp    |   date       ")
        for x in 0..<num {
            let city = result.getFieldString(tupleIndex: x, fieldIndex: 0) ?? ""
            let temp_lo = result.getFieldInt(tupleIndex: x, fieldIndex: 1) ?? 0
            let temp_hi = result.getFieldInt(tupleIndex: x, fieldIndex: 2) ?? 0
            let prcp = result.getFieldDouble(tupleIndex: x, fieldIndex: 3) ?? 0.0
            let date = result.getFieldString(tupleIndex: x, fieldIndex: 4) ?? ""
            print("\(city)      |   \(temp_lo)      |   \(temp_hi)      |   \(prcp)     |   \(date) ")
        }
    }
    
    func select3() {
        let stmt = "SELECT DISTINCT city FROM weather ORDER BY city;"
        let server = connect()
        let result = server.exec(statement: stmt)
        let num = result.numTuples()
        print("City       ")
        for x in 0..<num {
            let city = result.getFieldString(tupleIndex: x, fieldIndex: 0) ?? ""
            print("\(city)")
        }
    }
    
    func join1() {
        let stmt = "SELECT * FROM weather, citys WHERE city = name;"
        let server = connect()
        let result = server.exec(statement: stmt)
        let num = result.numTuples()
        print("City     |   temp_lo     |   temp_hi     |       prcp    |   date    | location   ")
        for x in 0..<num {
            let city = result.getFieldString(tupleIndex: x, fieldIndex: 0) ?? ""
            let temp_lo = result.getFieldInt(tupleIndex: x, fieldIndex: 1) ?? 0
            let temp_hi = result.getFieldInt(tupleIndex: x, fieldIndex: 2) ?? 0
            let prcp = result.getFieldDouble(tupleIndex: x, fieldIndex: 3) ?? 0.0
            let date = result.getFieldString(tupleIndex: x, fieldIndex: 4) ?? ""
            let name = result.getFieldString(tupleIndex: x, fieldIndex: 5) ?? ""
            let location = result.getFieldString(tupleIndex: x, fieldIndex: 6) ?? ""
            print("\(city)      |   \(temp_lo)      |   \(temp_hi)      |   \(prcp)     |   \(date)   |   \(name)   |    \(location)")
        }
    }
    //This query is called a left outer (left join) join because the table mentioned on the left of the join operator will have each of its rows in the output at least once, whereas the table on the right will only have those rows output that match some row of the left table. When outputting a left-table row for which there is no right-table match, empty (null) values are substituted for the right-table columns.
//    SELECT column1, column2...
//    FROM table_A
//    LEFT JOIN table_B ON join_condition
//    WHERE row_condition

    func leftOuterJoin() {
        let stmt = "SELECT * FROM weather LEFT OUTER JOIN citys ON (weather.city = citys.name);"
        let server = connect()
        let result = server.exec(statement: stmt)
        let num = result.numTuples()
        print("City     |   temp_lo     |   temp_hi     |       prcp    |   date    | location   ")
        for x in 0..<num {
            let city = result.getFieldString(tupleIndex: x, fieldIndex: 0) ?? ""
            let temp_lo = result.getFieldInt(tupleIndex: x, fieldIndex: 1) ?? 0
            let temp_hi = result.getFieldInt(tupleIndex: x, fieldIndex: 2) ?? 0
            let prcp = result.getFieldDouble(tupleIndex: x, fieldIndex: 3) ?? 0.0
            let date = result.getFieldString(tupleIndex: x, fieldIndex: 4) ?? ""
            let name = result.getFieldString(tupleIndex: x, fieldIndex: 5) ?? ""
            let location = result.getFieldString(tupleIndex: x, fieldIndex: 6) ?? ""
            print("\(city)      |   \(temp_lo)      |   \(temp_hi)      |   \(prcp)     |   \(date)   |   \(name)   |    \(location)")
        }
    }
    
    // SQL right outer join returns all rows in the right table and all the matching rows found in the left table.
//  SELECT column1, column2...
//  FROM table_A
//  RIGHT JOIN table_B ON join_condition
//  WHERE row_condition

    func rightOuterJoin() {
        let stmt = "SELECT * FROM weather RIGHT OUTER JOIN citys ON (weather.city = citys.name);"
        let server = connect()
        let result = server.exec(statement: stmt)
        let num = result.numTuples()
        print("City     |   temp_lo     |   temp_hi     |       prcp    |   date    | location   ")
        for x in 0..<num {
            let city = result.getFieldString(tupleIndex: x, fieldIndex: 0) ?? ""
            let temp_lo = result.getFieldInt(tupleIndex: x, fieldIndex: 1) ?? 0
            let temp_hi = result.getFieldInt(tupleIndex: x, fieldIndex: 2) ?? 0
            let prcp = result.getFieldDouble(tupleIndex: x, fieldIndex: 3) ?? 0.0
            let date = result.getFieldString(tupleIndex: x, fieldIndex: 4) ?? ""
            let name = result.getFieldString(tupleIndex: x, fieldIndex: 5) ?? ""
            let location = result.getFieldString(tupleIndex: x, fieldIndex: 6) ?? ""
            print("\(city)      |   \(temp_lo)      |   \(temp_hi)      |   \(prcp)     |   \(date)   |   \(name)   |    \(location)")
        }
    }
    
//    SQL full outer join returns:
//
//    all rows in the left table table_A.
//    all rows in the right table table_B.
//    and all matching rows in both tables.
    
//    SELECT column1, column2...
//    FROM table_A
//    FULL OUTER JOIN table_B ON join_condition
//    WHERE row_condition
    
//    Some database management systems do not support SQL full outer join syntax e.g., MySQL. Because SQL full outer join returns a result set that is a combined result of both SQL left join and SQL right join. Therefore you can easily emulate the SQL full outer join using SQL left join and SQL right join with UNION operator as follows:
//
//    SELECT column1, column2...
//    FROM table_A
//    LEFT JOIN table_B ON join_condition
//    UNION
//    SELECT column1, column2...
//    FROM table_A
//    RIGHT JOIN table_B ON join_condition
    func fullOuterJoin() {
        let stmt = "SELECT * FROM weather FULL OUTER JOIN citys ON (weather.city = citys.name);"
        let server = connect()
        let result = server.exec(statement: stmt)
        let num = result.numTuples()
        print("City     |   temp_lo     |   temp_hi     |       prcp    |   date    | location   ")
        for x in 0..<num {
            let city = result.getFieldString(tupleIndex: x, fieldIndex: 0) ?? ""
            let temp_lo = result.getFieldInt(tupleIndex: x, fieldIndex: 1) ?? 0
            let temp_hi = result.getFieldInt(tupleIndex: x, fieldIndex: 2) ?? 0
            let prcp = result.getFieldDouble(tupleIndex: x, fieldIndex: 3) ?? 0.0
            let date = result.getFieldString(tupleIndex: x, fieldIndex: 4) ?? ""
            let name = result.getFieldString(tupleIndex: x, fieldIndex: 5) ?? ""
            let location = result.getFieldString(tupleIndex: x, fieldIndex: 6) ?? ""
            print("\(city)      |   \(temp_lo)      |   \(temp_hi)      |   \(prcp)     |   \(date)   |   \(name)   |    \(location)")
        }

    }
    
    func selfJoin() {
        let stmt =  """
                    SELECT W1.city, W1.temp_lo AS low, W1.temp_hi AS high,
                        W2.city, W2.temp_lo AS low, W2.temp_hi AS high
                        FROM weather W1, weather W2
                        WHERE W1.temp_lo < W2.temp_lo
                        AND W1.temp_hi > W2.temp_hi;
                    """
        let server = connect()
        let result = server.exec(statement: stmt)
        let num = result.numTuples()
        print("City     |   low     |   high     |       city    |   low    | high   ")
        for x in 0..<num {
            let city = result.getFieldString(tupleIndex: x, fieldIndex: 0) ?? ""
            let temp_lo = result.getFieldInt(tupleIndex: x, fieldIndex: 1) ?? 0
            let temp_hi = result.getFieldInt(tupleIndex: x, fieldIndex: 2) ?? 0
            let city1 = result.getFieldString(tupleIndex: x, fieldIndex: 3) ?? ""
            let temp_lo1 = result.getFieldInt(tupleIndex: x, fieldIndex: 4) ?? 0
            let temp_hi1 = result.getFieldInt(tupleIndex: x, fieldIndex: 5) ?? 0

            print("\(city)      |   \(temp_lo)      |   \(temp_hi)      |   \(city1)     |   \(temp_lo1)   |   \(temp_hi1) ")
        }
    }
    
    // Aggregate functions
    func aggregates() {
        do {
            let stmt = "SELECT max(temp_lo) FROM weather;"
            let server = connect()
            let result = server.exec(statement: stmt)
            let num = result.numTuples()
            print("Max  ")
            for x in 0..<num {
                let max = result.getFieldInt(tupleIndex: x, fieldIndex: 0) ?? 0
                print("\(max)")
            }
        }
        do {
            //SELECT city FROM weather WHERE temp_lo = max(temp_lo);     WRONG
            //        but this will not work since the aggregate max cannot be used in the WHERE clause. (This restriction exists because the WHERE clause determines which rows will be included in the aggregate calculation; so obviously it has to be evaluated before aggregate functions are computed.) However, as is often the case the query can be restated to accomplish the desired result, here by using a subquery:
            
            let stmt = "SELECT city FROM weather WHERE temp_lo = (SELECT max(temp_lo) from weather);"
            let server = connect()
            let result = server.exec(statement: stmt)
            let num = result.numTuples()
            print("City  ")
            for x in 0..<num {
                let city = result.getFieldString(tupleIndex: x, fieldIndex: 0) ?? ""
                print("\(city)")
            }
        }
        
        do {
            let stmt = """
                        SELECT city, max(temp_lo)
                            FROM weather
                            GROUP BY city;
                        """
            let server = connect()
            let result = server.exec(statement: stmt)
            let num = result.numTuples()
            print("City     |       Max")
            for x in 0..<num {
                let city = result.getFieldString(tupleIndex: x, fieldIndex: 0) ?? ""
                let max = result.getFieldInt(tupleIndex: x, fieldIndex: 1) ?? 0
                print("\(city)      |       \(max)")
                
            }
        }
        
        // which gives us one output row per city. Each aggregate result is computed over the table rows matching that city. We can filter these grouped rows using HAVING:

        do {
            let stmt = """
                        SELECT city, max(temp_lo)
                        FROM weather
                        GROUP BY city
                        HAVING max(temp_lo) < 40;
                        """
            let server = connect()
            let result = server.exec(statement: stmt)
            let num = result.numTuples()
            print("City     |       Max")
            for x in 0..<num {
                let city = result.getFieldString(tupleIndex: x, fieldIndex: 0) ?? ""
                let max = result.getFieldInt(tupleIndex: x, fieldIndex: 1) ?? 0
                print("\(city)      |       \(max)")
                
            }
        }

//        which gives us the same results for only the cities that have all temp_lo values below 40. Finally, if we only care about cities whose names begin with “S”, we might do:
        
        do {
            let stmt = """
                        SELECT city, max(temp_lo)
                        FROM weather
                        WHERE city LIKE 'S%'
                        GROUP BY city
                        HAVING max(temp_lo) < 40;
                        """
            let server = connect()
            let result = server.exec(statement: stmt)
            let num = result.numTuples()
            print("City     |       Max")
            for x in 0..<num {
                let city = result.getFieldString(tupleIndex: x, fieldIndex: 0) ?? ""
                let max = result.getFieldInt(tupleIndex: x, fieldIndex: 1) ?? 0
                print("\(city)      |       \(max)")
                
            }
        }
        //            It is important to understand the interaction between aggregates and SQL's WHERE and HAVING clauses. The fundamental difference between WHERE and HAVING is this: WHERE selects input rows before groups and aggregates are computed (thus, it controls which rows go into the aggregate computation), whereas HAVING selects group rows after groups and aggregates are computed. Thus, the WHERE clause must not contain aggregate functions; it makes no sense to try to use an aggregate to determine which rows will be inputs to the aggregates. On the other hand, the HAVING clause always contains aggregate functions. (Strictly speaking, you are allowed to write a HAVING clause that doesn't use aggregates, but it's seldom useful. The same condition could be used more efficiently at the WHERE stage.)
    }
    
    func updateWeather() {
        let stmt = "UPDATE weather SET temp_hi = temp_hi - 2, temp_lo = temp_lo-2 WHERE date > '1994-11-28';"
        let server = connect()
        let _ = server.exec(statement: stmt)
    }
    
    func delete() {
        do {
            let stmt = "DELETE FROM weather WHERE city = 'Hayward';"
            let server = connect()
            let _ = server.exec(statement: stmt)
            selectFromWeather()
        }
        
        do {
//            Without a qualification, DELETE will remove all rows from the given table, leaving it empty. The system will not request confirmation before doing this!
            let stmt = "DELETE FROM weather"
            let server = connect()
            let _ = server.exec(statement: stmt)
            selectFromWeather()
        }
    }
    func createCity() {
        let stmt = "CREATE TABLE \"citys\" (\"name\" varchar(80), \"location\" point);"
        exec(stmt, params: [])
    }
    
    func insertIntoCity(name: String, location: String) {
        let stmt = "INSERT INTO \"citys\" VALUES($1, $2)"
        exec(stmt, params: [name, location])
    }

    func connect() -> PGConnection {
        let server = PGConnection()
        let status = server.connectdb(PostgresConnector.connectionString)
        if status != .ok {
            print("\(status)")
        }
        return server
    }


    func exec(_ statement: String, params: [Any]) {
        let server = connect()
        let _ = server.exec(statement: statement, params: params)
        server.close()
    }

    func isError(_ errorMsg: String) -> Bool {
        if errorMsg.contains(string: "ERROR") {
            print(errorMsg)
            return true
        }
        return false
    }

}
