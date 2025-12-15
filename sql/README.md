# Introduction
This repository is a hands-on SQL learning and mini-project environment designed to make practicing real database skills fast, repeatable, and professional. You can spin up a containerized PostgreSQL instance with Docker, load a sample dataset, and work through  exercises that cover CRUD, DDL, DML, DQL, joins, aggregates, window functions, and query style. The environment is scriptable with Bash (for setup, data loading, and automation), queryable via psql, and optionally explored visually with DBeaver or pgAdmin. Version control is managed with Git and documentation with the README so your solutions, notes, and schema changes are tracked like a real project.
# SQL Queries

###### Table Setup (DDL)
```sql

CREATE SCHEMA IF NOT EXISTS cd;

--Members Table
CREATE TABLE IF NOT EXISTS cd.members (
  memid           INTEGER       PRIMARY KEY,
  surname         VARCHAR(200)  NOT NULL,
  firstname       VARCHAR(200)  NOT NULL,
  address         VARCHAR(300)  NOT NULL,
  zipcode         INTEGER       NOT NULL,
  telephone       VARCHAR(20)   NOT NULL,
  recommendedby   INTEGER       NULL,
  joindate        TIMESTAMP     NOT NULL,
  CONSTRAINT members_recommendedby_fk
    FOREIGN KEY (recommendedby) REFERENCES cd.members(memid)
);

--Facilities Table
CREATE TABLE IF NOT EXISTS cd.facilities (
  facid               INTEGER        PRIMARY KEY,
  name                VARCHAR(100)   NOT NULL,
  membercost          NUMERIC(10,2)  NOT NULL,
  guestcost           NUMERIC(10,2)  NOT NULL,
  initialoutlay       NUMERIC(12,2)  NOT NULL,
  monthlymaintenance  NUMERIC(10,2)  NOT NULL
);

--Bookings Table
CREATE TABLE IF NOT EXISTS cd.bookings (
  bookid     INTEGER     PRIMARY KEY,
  facid      INTEGER     NOT NULL REFERENCES cd.facilities(facid),
  memid      INTEGER     NOT NULL REFERENCES cd.members(memid),
  starttime  TIMESTAMP   NOT NULL,
  slots      INTEGER     NOT NULL
);



```
###### Question 1: Insert some data into a table

```sql
insert into cd.facilities
    (facid, name, membercost, guestcost, initialoutlay, monthlymaintenance)
    values (9, 'Spa', 20, 30, 100000, 800);
```

###### Question 2: Insert calculated data into a table

```sql
insert into cd.facilities
    (facid, name, membercost, guestcost, initialoutlay, monthlymaintenance)
select (select max(facid) from cd.facilities)+1,
       'Spa', 20, 30, 100000, 800;

```


###### Question 3: Update some existing data

```sql
update cd.facilities
set initialoutlay = 10000
where facid = 1;

```

###### Question 4: Update a row based on the contents of another row

```sql
update cd.facilities facs
set membercost = (select membercost * 1.1 from cd.facilities where facid = 0),
    guestcost  = (select guestcost  * 1.1 from cd.facilities where facid = 0)
where facs.facid = 1;


```

###### Question 5: Delete all bookings

```sql
delete from cd.bookings;
```

###### Question 6: Delete a member from the cd.members table

```sql
delete from cd.members
    where memid = 37;
```

###### Question 7: Control which rows are retrieved - part 2

```sql
SELECT facid, name, membercost, monthlymaintenance
FROM cd.facilities
WHERE membercost > 0
  AND (membercost < monthlymaintenance / 50.0);


```

###### Question 8: Basic string searches

```sql
SELECT facid, name, membercost, guestcost,initialoutlay,monthlymaintenance
FROM cd.facilities
WHERE name LIKE '%Tennis%';


```

###### Question 9: Matching against multiple possible values

```sql
SELECT *
FROM cd.facilities
WHERE facid IN (1, 5);


```

###### Question 10: Working with dates

```sql
SELECT memid, surname, firstname, joindate
FROM cd.members
WHERE joindate >= '2012-09-01';


```

###### Question 11: Combining results from multiple queries

```sql
SELECT surname
FROM cd.members
UNION
SELECT name
FROM cd.facilities;

```

###### Question 12: Retrieve the start times of members' bookings

```sql
SELECT b.starttime
FROM cd.bookings AS b
         JOIN cd.members  AS m
              ON m.memid = b.memid
WHERE m.firstname = 'David'
  AND m.surname  = 'Farrell';


```

###### Question 13: Work out the start times of bookings for tennis courts

```sql
SELECT b.starttime AS start, f.name
FROM cd.bookings   AS b
         JOIN cd.facilities AS f
              ON b.facid = f.facid
WHERE f.name LIKE 'Tennis Court%'
  AND DATE(b.starttime) = DATE '2012-09-21'
ORDER BY b.starttime;

```

###### Question 14: Produce a list of all members, along with their recommender

```sql
SELECT
    m.firstname,
    m.surname,
    r.firstname AS rec_firstname,
    r.surname   AS rec_surname
FROM cd.members AS m
         LEFT JOIN cd.members AS r
                   ON m.recommendedby = r.memid
ORDER BY m.surname, m.firstname;

```

###### Question 15: Produce a list of all members who have recommended another member

```sql
SELECT DISTINCT r.firstname, r.surname
FROM cd.members AS m
         JOIN cd.members AS r
              ON r.memid = m.recommendedby
ORDER BY r.surname, r.firstname;


```

###### Question 16: Produce a list of all members, along with their recommender, using no joins.

```sql
SELECT DISTINCT
    m.firstname || ' ' || m.surname AS member,
    (
        SELECT r.firstname || ' ' || r.surname
        FROM cd.members AS r
        WHERE r.memid = m.recommendedby
    ) AS recommended
FROM cd.members AS m
ORDER BY member;

```

###### Question 17: Count the number of recommendations each member makes.

```sql
SELECT recommendedby, COUNT(*) AS count
FROM cd.members
WHERE recommendedby IS NOT NULL
GROUP BY recommendedby
ORDER BY recommendedby;


```

###### Question 18: List the total slots booked per facility

```sql
SELECT
    facid,
    SUM(slots) AS slots
FROM cd.bookings
GROUP BY facid
ORDER BY facid;


```

###### Question 19: List the total slots booked per facility in a given month

```sql
SELECT facid, SUM(slots) AS total_slots
FROM cd.bookings
WHERE starttime >= DATE '2012-09-01'
  AND starttime <  DATE '2012-10-01'
GROUP BY facid
ORDER BY total_slots;


```

###### Question 20: List the total slots booked per facility per month

```sql
SELECT facid,
       EXTRACT(MONTH FROM starttime) AS month,
       SUM(slots) AS total_slots
FROM cd.bookings
WHERE EXTRACT(YEAR FROM starttime) = 2012
GROUP BY facid, month
ORDER BY facid, month;


```

###### Question 21: Find the count of members who have made at least one booking

```sql
SELECT COUNT(DISTINCT memid) AS count
FROM cd.bookings;


```

###### Question 22: List each member's first booking after September 1st 2012

```sql
SELECT
    m.surname,
    m.firstname,
    m.memid,
    MIN(b.starttime) AS starttime
FROM cd.members   AS m
         JOIN cd.bookings  AS b USING (memid)
WHERE b.starttime >= DATE '2012-09-01'
GROUP BY m.memid, m.firstname, m.surname
ORDER BY m.memid;

```

###### Question 23: Produce a list of member names, with each row containing the total member count

```sql
SELECT
    COUNT(*) OVER () AS count,
  m.firstname,
  m.surname
FROM cd.members AS m
ORDER BY m.joindate, m.memid;
```

###### Question 24: Produce a numbered list of members

```sql
SELECT
    ROW_NUMBER() OVER (ORDER BY m.joindate, m.memid) AS row_number,
    m.firstname,
    m.surname
FROM cd.members AS m
ORDER BY m.joindate, m.memid;
```

###### Question 25: Output the facility id that has the highest number of slots booked, again

```sql
SELECT facid, total_slots
FROM (
         SELECT facid,
                SUM(slots)                     AS total_slots,
                RANK() OVER (ORDER BY SUM(slots) DESC) AS r
         FROM cd.bookings
         GROUP BY facid
     ) t
WHERE r = 1
ORDER BY facid;


```

###### Question 26: Format the names of members

```sql
SELECT
    m.surname || ', ' || m.firstname AS full_name
FROM cd.members AS m
ORDER BY m.surname, m.firstname;


```

###### Question 27: Find telephone numbers with parentheses

```sql
SELECT memid, telephone
FROM cd.members
WHERE telephone ~ '[()]'
ORDER BY memid;
```

###### Question 28: Count the number of members whose surname starts with each letter of the alphabet

```sql
SELECT
    SUBSTR(m.surname, 1, 1) AS initial,
    COUNT(*)                AS member_count
FROM cd.members AS m
GROUP BY SUBSTR(m.surname, 1, 1)
ORDER BY initial;

```
