# database-js-sqlparser
Common functionality for [database-js](https://github.com/mlaanderson/database-js) drivers that operate on non-database backends

## About
Database-js-sqlparser is a [database-js](https://github.com/mlaanderson/database-js) driver that parses SQL statements and passes requests and commands
to an underlying class which does the storage mechanism interaction. On its own it accomplishes nothing.

The sql parser supports the following SQL:

#### Tables
````SQL
CREATE TABLE <table_name>(<column_name> <column_type>,...)
````
Where the column type can be one of:
* CHARACTER(n) - String of n length. Always padded or truncated to n length.
* VARCHAR(n) - String of up to n length. Always truncated to n length.
* BOOLEAN - Boolean (true or false)
* INTEGER, SMALLINT, BIGINT - Integer numeric values
* DECIMAL, NUMERIC, FLOAT, REAL, DOUBLE - Floating point numeric values, limited by Javascript's floating point implementation.
* DATE, TIME, TIMESTAMP - Date values
* TEXT - String values of arbitrary length

````SQL
DROP TABLE <table_name>
````

#### Queries
````SQL
SELECT [
    *,
    <column_name>[AS <column_label>],
    SUM|COUNT(<column_name)[AS <aggregate_label>]
] FROM <table_name>
[[INNER,LEFT,RIGHT] JOIN <table_name> ON <join_condition>]
[GROUP BY <column_name>]
[WHERE <where_condition>]
[ORDER BY <column_name>]
[LIMIT [row_offset,]<number_of_rows>]
````
##### Joins:
Inner, left and right joins are supported. Full or outer joins are not supported.

##### Aggregate Functions:
Sum and count are currently supported. Sum will not fail on non-numeric columns, but the return is undefined.

#### Inserts
````SQL
INSERT INTO <table_name>(<column1>,<column2>,...) VALUES(<value1>,<value2>,...)
````
It's best to use paramaterized SQL:
````SQL
INSERT INTO <table_name>(<column1>,<column2>,...) VALUES(?,?,...)
````

#### Updates
````SQL
UPDATE <table_name> SET <column1> = <value1>, <column2> = <value2>,...
[WHERE <where_condition>]
````
Using parameterized SQL:
````SQL
UPDATE <table_name> SET <column1> = ?, <column2> = ?,...
[WHERE <where_condition>]
````

#### Deletes
````SQL
DELETE FROM <table_name> [WHERE <where_condition>]
````

## Implementation in an extending class
A class extending the database-js-sqlparser class needs to override seven methods. 
Each method needs to return a Promise to allow for asynchronous implementations.

#### ready() : Promise&lt;boolean&gt;
Indicates that the underlying storage mechanism is loaded and ready to receive
reads and writes.

To implement an always ready driver, use the following signature:
````javascript
ready() {
    return Promise.resolve(true);
}
````

#### close() : Promse&lt;boolean&gt;
Allows the underlying storage mechanism to close if necessary.

#### load(table: string) : Promise&lt;Array&lt;{[key:string]:any}&gt;&gt;
Loads the rows from for a given table from the underlying storage and returns them
via the Promise.

The resolved value of the Promise needs to be an array of table rows, where each
row is a JSON like object with the column names as keys for the row values:

````JSON
[
    {
        "id": 1,
        "name": "Me",
        "age": 32
    },
    {
        "id": 2,
        "name": "You",
        "age": 27
    }
]
````

#### store(table: string, index: string|number, row: any) : Promise&lt;string|number&gt;
Updates or inserts a row into the underlying storage system. If index is a string of number,
then the action is an update, if index is null or undefined this is an insert. Resolves the
promise with the updated or inserted index.

#### remove(table: string, index: string|number) : Promise&lt;string|number&gt;
Removes a row from the underlying storage system. Resolves the promise with the
index that was removed.

#### create(table: string, definition: Array&lt;column_definition&gt;) : Promise&lt;boolean&gt;
Creates a new table according to the passed definition, resolves with true if successful.

The column definition is as follows:
````javascript
{
    "name": string,    // The column name
    "index": number,   // The column index, can be ignored
    "type": "string"|"integer"|"float"|"date",
    "length"?: number, // For VARCHAR(n) or CHARACTER(n) the string length limit
    "pad"?: " ",       // For CHARACTER(n) the string to pad short strings with
}
````

#### drop(table: string) : Promise&lt;boolean&gt;
Drops the table from the underlying storage system. The user will expect the
table data to be removed as well.