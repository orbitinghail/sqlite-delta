-- noqa: disable=all
.echo on
.mode table
.bail on
.param init
-- noqa: enable=all

-- The Application table must have an explicit rowid. To do this you have two choices:
-- 1. Use an INTEGER PRIMARY KEY column, which will be the rowid.
-- 2. Use a separate column for the rowid, which must be unique and indexed.
-- This is demonstrated in the `AppTable` and `AppTableExplicitRowid` tables below.

CREATE TABLE AppTable(
	id INTEGER PRIMARY KEY,
	t TEXT
);

CREATE TABLE AppTableExplicitRowid(
	id blob UNIQUE, -- this is the logical id for the row
	t TEXT,

	rid INTEGER PRIMARY KEY
);

-- The `changes` table tracks changes to the application tables.
create table changes (
  -- tid is a unique identifier for the application table.
	tid INTEGER NOT NULL,

  -- rid is the row identifier of the changed row in the application table.
	rid INTEGER NOT NULL,

	-- Global Sequence Number (GSN) is a unique number representing when the
	-- change occurred relative to all other changes.
	gsn INTEGER NOT NULL,

	PRIMARY KEY (tid, rid)
) WITHOUT ROWID;

-------------------------------------
-- Triggers: Automatic change tracking for application tables
-------------------------------------

-- Define three triggers for each application table we want to track changes for.
-- Each trigger is identical other than the table id it is associated with and the
-- trigger operation.

-- For additional performance and storage efficiency, we use Application
-- defined Table IDs rather than strings. In this example, we use 1 for
-- `AppTable` and 2 for `AppTableExplicitRowid`.
--
-- If you prefer to use strings, you can replace the `tid` column type with
-- `text` and modify the triggers accordingly.

CREATE TRIGGER IF NOT EXISTS trg_AppTable_insert AFTER INSERT ON AppTable
BEGIN
  INSERT INTO changes(tid, rid, gsn)
  VALUES (1, new.rowid, (SELECT IFNULL(MAX(gsn), 0) + 1 FROM changes))
  ON CONFLICT DO UPDATE SET gsn = excluded.gsn;
END;

CREATE TRIGGER IF NOT EXISTS trg_AppTable_update AFTER UPDATE ON AppTable
BEGIN
  INSERT INTO changes(tid, rid, gsn)
  VALUES (1, new.rowid, (SELECT IFNULL(MAX(gsn), 0) + 1 FROM changes))
  ON CONFLICT DO UPDATE SET gsn = excluded.gsn;
END;

CREATE TRIGGER IF NOT EXISTS trg_AppTable_delete AFTER DELETE ON AppTable
BEGIN
  INSERT INTO changes(tid, rid, gsn)
  VALUES (1, new.rowid, (SELECT IFNULL(MAX(gsn), 0) + 1 FROM changes))
  ON CONFLICT DO UPDATE SET gsn = excluded.gsn;
END;

CREATE TRIGGER IF NOT EXISTS trg_AppTableExplicitRowid_insert AFTER INSERT ON AppTableExplicitRowid
BEGIN
  INSERT INTO changes(tid, rid, gsn)
  VALUES (2, new.rowid, (SELECT IFNULL(MAX(gsn), 0) + 1 FROM changes))
  ON CONFLICT DO UPDATE SET gsn = excluded.gsn;
END;

CREATE TRIGGER IF NOT EXISTS trg_AppTableExplicitRowid_update AFTER UPDATE ON AppTableExplicitRowid
BEGIN
  INSERT INTO changes(tid, rid, gsn)
  VALUES (2, new.rowid, (SELECT IFNULL(MAX(gsn), 0) + 1 FROM changes))
  ON CONFLICT DO UPDATE SET gsn = excluded.gsn;
END;

CREATE TRIGGER IF NOT EXISTS trg_AppTableExplicitRowid_delete AFTER DELETE ON AppTableExplicitRowid
BEGIN
  INSERT INTO changes(tid, rid, gsn)
  VALUES (2, new.rowid, (SELECT IFNULL(MAX(gsn), 0) + 1 FROM changes))
  ON CONFLICT DO UPDATE SET gsn = excluded.gsn;
END;

------------------------------------------
-- Reading and Writing to the Application Tables
------------------------------------------

-- The application tables can be read and written to as normal. Triggers handle all the heavy lifting.
insert into AppTable (id, t) values
  (1, 'data 1; revision 1'),
  (2, 'data 2; revision 1'),
  (3, 'data 3; revision 1'),
  (4, 'data 4; revision 1'),
  (5, 'data 5; revision 1');

update AppTable set t = 'data 1; revision 2' where id = 1;

select * from AppTable;

-- When inserting into a table with an explicit rowid, you can provide NULL to have it automatically assigned.
insert into AppTableExplicitRowid (id, t) values
  (1, 'data 1; revision 1'),
  (2, 'data 2; revision 1'),
  (3, 'data 3; revision 1');

update AppTableExplicitRowid set t = 'data 1; revision 2' where id = 1;

select * from AppTableExplicitRowid;

------------------------------------------
-- Creating a Changeset
------------------------------------------

-- First we need to save the current GSN in order to clean up the changes table
-- after building the changeset.
-- noqa: disable=all
.param set @current_gsn "SELECT max(gsn) FROM changes"
-- noqa: enable=all

-- Open a read transaction (WAL mode is recommended for concurrent writes)
BEGIN;

-- Then compute the changeset by joining each application table with the changes table
SELECT
  IFNULL(base.rowid, changes.rid) AS rowid,
  IF(base.rowid IS NULL, 'delete', 'upsert') AS operation,
  base.*
FROM
  (SELECT * FROM changes WHERE tid = 1) AS changes
  FULL OUTER JOIN AppTable AS base ON base.rowid = changes.rid;

SELECT
  IFNULL(base.rowid, changes.rid) AS rowid,
  IF(base.rowid IS NULL, 'delete', 'upsert') AS operation,
  base.*
FROM
  (SELECT * FROM changes WHERE tid = 2) AS changes
  FULL OUTER JOIN AppTableExplicitRowid AS base ON base.rowid = changes.rid;

-- Close the read transaction
COMMIT;

-- Finally, after ensuring the changeset is durable, we can clean up the changes table
-- by deleting all changes with a GSN less than or equal to the saved current GSN.
DELETE FROM changes WHERE gsn <= @current_gsn;
