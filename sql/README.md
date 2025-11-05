# Introduction

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

###### Question 3: Update some existing data

```sql
update cd.facilities
set initialoutlay = 10000
where facid = 1;

```

###### Question 3: Update some existing data

```sql
update cd.facilities
set initialoutlay = 10000
where facid = 1;

```

###### Question 3: Update some existing data

```sql
update cd.facilities
set initialoutlay = 10000
where facid = 1;

```

###### Question 3: Update some existing data

```sql
update cd.facilities
set initialoutlay = 10000
where facid = 1;

```

###### Question 3: Update some existing data

```sql
update cd.facilities
set initialoutlay = 10000
where facid = 1;

```

###### Question 3: Update some existing data

```sql
update cd.facilities
set initialoutlay = 10000
where facid = 1;

```

###### Question 3: Update some existing data

```sql
update cd.facilities
set initialoutlay = 10000
where facid = 1;

```

###### Question 3: Update some existing data

```sql
update cd.facilities
set initialoutlay = 10000
where facid = 1;

```

###### Question 3: Update some existing data

```sql
update cd.facilities
set initialoutlay = 10000
where facid = 1;

```

###### Question 3: Update some existing data

```sql
update cd.facilities
set initialoutlay = 10000
where facid = 1;

```

###### Question 3: Update some existing data

```sql
update cd.facilities
set initialoutlay = 10000
where facid = 1;

```

###### Question 3: Update some existing data

```sql
update cd.facilities
set initialoutlay = 10000
where facid = 1;

```

###### Question 3: Update some existing data

```sql
update cd.facilities
set initialoutlay = 10000
where facid = 1;

```

###### Question 3: Update some existing data

```sql
update cd.facilities
set initialoutlay = 10000
where facid = 1;

```

###### Question 3: Update some existing data

```sql
update cd.facilities
set initialoutlay = 10000
where facid = 1;

```

###### Question 3: Update some existing data

```sql
update cd.facilities
set initialoutlay = 10000
where facid = 1;

```