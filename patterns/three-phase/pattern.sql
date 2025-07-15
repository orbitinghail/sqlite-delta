-- noqa: disable=all
.echo on
.mode table
.bail on
-- noqa: enable=all

CREATE TABLE AppTable(
  -- application specific columns
	id INTEGER,
	data BLOB,

	-------------------------------
	-- three-phase pattern columns:

	-- `phase` represents the current phase of the row
  -- 0: new row, not seen by any changeset
  -- 1: row version used by an in-progress changeset
  -- 2: stable row version
	phase INTEGER NOT NULL DEFAULT 0,

  -- `deleted` indicates whether the row is logically deleted.
  -- the row will be removed after reaching phase 2
	deleted BOOL NOT NULL DEFAULT 0,

	-- the primary key must include phase, as there are now up to 3 copies of each
	-- row depending on snapshot state
	PRIMARY KEY (id, phase)
);

------------------------------------------
-- How to correctly insert rows
------------------------------------------

-- simple inserts will work if you know the row does not exist. `phase` will be
-- set to 0 per its default value.
insert into AppTable (id, data) values
  (1, 'data 1; revision 1'),
  (2, 'data 2; revision 1'),
  (3, 'data 3; revision 1'),
  (4, 'data 4; revision 1'),
  (5, 'data 5; revision 1');

------------------------------------------
-- How to correctly insert or update rows
------------------------------------------

-- to insert or update a row we use an upsert operation. `phase` will be set to
-- 0 per its default value. The conflict clause must include all application
-- data columns other than the PK.
insert into AppTable (id, data) values
  -- modify some existing rows
  (1, 'data 1; revision 2'),
  (3, 'data 3; revision 2'),
  (5, 'data 5; revision 2'),
  -- create some new rows
  (6, 'data 6; revision 1'),
  (7, 'data 7; revision 1')
on conflict do update set data = excluded.data;

------------------------------------------
-- How to correctly delete rows
------------------------------------------

-- to delete a row, we upsert a row with the same id and set `deleted` to 1.
-- we can leave the rest of the columns NULL, assuming that the application
-- does not require them to be set.
insert into AppTable (id, deleted) values (2, 1)
on conflict do update set deleted = excluded.deleted;

------------------------------------------
-- How to correctly read from the AppTable
------------------------------------------

-- Due to the `phase` column, the AppTable can have up to three versions of each
-- row. And due to the `deleted` column rows may be logically deleted. Thus to
-- correctly read the latest version of a row, you can use the following query:
select * from (
  select * from AppTable where id = 1 order by phase asc limit 1
) where deleted = 0;

------------------------------------------
-- Creating a Changeset
------------------------------------------

-- First we need to transition all phase 0 rows to phase 1.
-- This ensures that concurrent writes will not corrupt the changeset, while
-- also ensuring that we can build the changeset in parallel and resume from a
-- crash.
update AppTable set phase = 1 where phase = 0;

-- Then compute the changeset by joining the phase 1 rows with the phase 2 rows.
select
	IFNULL(before.id, after.id) as id,
	before.data as data_before, after.data as data_after,
	after.deleted as deleted
from
	(select * from AppTable where phase = 2) as before
	right join
	(select * from AppTable where phase = 1) as after
	using (id);

-- Finally, to complete the changeset, we need to perform cleanup in a transaction:
begin;

-- remove dead phase=2 rows
delete from AppTable as outer
where
  phase = 2 AND
  (
    -- first case: dead phase=2 rows
    deleted = 1

    -- second case: phase=2 rows which are in phase=1
    OR exists (
      select * from AppTable as inner where outer.id = inner.id and phase = 1
    )
  );

-- migrate phase 1 rows to phase 2
update AppTable set phase = 2 where phase = 1;

commit;
