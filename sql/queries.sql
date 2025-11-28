--1
insert into cd.facilities
(facid, name, membercost, guestcost, initialoutlay, monthlymaintenance)
values (9, 'Spa', 20, 30, 100000, 800);

--2
insert into cd.facilities
(facid, name, membercost, guestcost, initialoutlay, monthlymaintenance)
select (select max(facid) from cd.facilities)+1,
       'Spa', 20, 30, 100000, 800;

--3
update cd.facilities
set initialoutlay = 10000
where facid = 1;

--4
update cd.facilities facs
set membercost = (select membercost * 1.1 from cd.facilities where facid = 0),
    guestcost  = (select guestcost  * 1.1 from cd.facilities where facid = 0)
where facs.facid = 1;

--5
delete from cd.bookings;

--6
delete from cd.members
where memid = 37;

--7
SELECT facid, name, membercost, monthlymaintenance
FROM cd.facilities
WHERE membercost > 0
  AND (membercost < monthlymaintenance / 50.0);




--8
SELECT facid, name, membercost, guestcost,initialoutlay,monthlymaintenance
FROM cd.facilities
WHERE name LIKE '%Tennis%';




--9
SELECT *
FROM cd.facilities
WHERE facid IN (1, 5);




--10
SELECT memid, surname, firstname, joindate
FROM cd.members
WHERE joindate >= '2012-09-01';




--11
SELECT surname
FROM cd.members
UNION
SELECT name
FROM cd.facilities;


--12
SELECT b.starttime
FROM cd.bookings AS b
         JOIN cd.members  AS m
              ON m.memid = b.memid
WHERE m.firstname = 'David'
  AND m.surname  = 'Farrell';


--13
SELECT b.starttime AS start, f.name
FROM cd.bookings   AS b
         JOIN cd.facilities AS f
              ON b.facid = f.facid
WHERE f.name LIKE 'Tennis Court%'
  AND DATE(b.starttime) = DATE '2012-09-21'
ORDER BY b.starttime;


--14
SELECT
    m.firstname,
    m.surname,
    r.firstname AS rec_firstname,
    r.surname   AS rec_surname
FROM cd.members AS m
         LEFT JOIN cd.members AS r
                   ON m.recommendedby = r.memid
ORDER BY m.surname, m.firstname;

--15
SELECT DISTINCT r.firstname, r.surname
FROM cd.members AS m
         JOIN cd.members AS r
              ON r.memid = m.recommendedby
ORDER BY r.surname, r.firstname;


--16
SELECT DISTINCT
    m.firstname || ' ' || m.surname AS member,
    (
        SELECT r.firstname || ' ' || r.surname
        FROM cd.members AS r
        WHERE r.memid = m.recommendedby
    ) AS recommended
FROM cd.members AS m
ORDER BY member;


--17
SELECT recommendedby, COUNT(*) AS count
FROM cd.members
WHERE recommendedby IS NOT NULL
GROUP BY recommendedby
ORDER BY recommendedby;


--18
SELECT
    facid,
    SUM(slots) AS slots
FROM cd.bookings
GROUP BY facid
ORDER BY facid;


--19
SELECT facid, SUM(slots) AS total_slots
FROM cd.bookings
WHERE starttime >= DATE '2012-09-01'
  AND starttime <  DATE '2012-10-01'
GROUP BY facid
ORDER BY total_slots;


--20
SELECT facid,
       EXTRACT(MONTH FROM starttime) AS month,
       SUM(slots) AS total_slots
FROM cd.bookings
WHERE EXTRACT(YEAR FROM starttime) = 2012
GROUP BY facid, month
ORDER BY facid, month;


--21
SELECT COUNT(DISTINCT memid) AS count
FROM cd.bookings;


--22
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

--23
SELECT
    COUNT(*) OVER () AS count,
  m.firstname,
  m.surname
FROM cd.members AS m
ORDER BY m.joindate, m.memid;


--24
SELECT
    ROW_NUMBER() OVER (ORDER BY m.joindate, m.memid) AS row_number,
    m.firstname,
    m.surname
FROM cd.members AS m
ORDER BY m.joindate, m.memid;

--25
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


--26
SELECT
    m.surname || ', ' || m.firstname AS full_name
FROM cd.members AS m
ORDER BY m.surname, m.firstname;


--27
SELECT memid, telephone
FROM cd.members
WHERE telephone ~ '[()]'
ORDER BY memid;

--28
SELECT
    SUBSTR(m.surname, 1, 1) AS initial,
    COUNT(*)                AS member_count
FROM cd.members AS m
GROUP BY SUBSTR(m.surname, 1, 1)
ORDER BY initial;

