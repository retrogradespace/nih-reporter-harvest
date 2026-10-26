-- nih_child_tables.sql -- five child tables, one per array field that
-- nih_parsed only exposes as JSON text, each exploded to one row per array
-- element:
--
--   nih_parsed column               -> table                         one row per
--   principal_investigators_json    -> nih_principal_investigators   appl_id + PI
--   program_officers_json           -> nih_program_officers          appl_id + program officer
--   agency_ic_fundings_json         -> nih_agency_ic_fundings        appl_id + funding institute
--   spending_categories_json        -> nih_spending_categories       appl_id + spending category
--   covid_response_json             -> nih_covid_response            appl_id + COVID response code
--
-- Ported from an earlier Oracle version of these (views there, built with
-- JSON_TABLE). Same rows, same columns, same rules, with three differences:
--
-- 1. TABLES, NOT VIEWS. Unpacking the arrays of ~3M records at query time
--    makes a full scan of one view take the better part of a minute, so
--    they are built once instead. This file drops and rebuilds all five in
--    one transaction; src/7_build_child_tables.py runs it after every weekly
--    load (6_run_weekly_refresh.py), so they are never older than the last
--    load. A reader sees either the old five or the new five, never a mix.
--    nih_parsed and nih_indicators stay views.
-- 2. NO INSTITUTION FISCAL YEAR. The Oracle version carried an
--    institution-specific fiscal-year label; here fiscal_year is NIH's.
-- 3. ORGANIZATION COLUMNS ADDED. This data covers every institution, so each
--    table also carries organization_org_name, organization_org_ipf_code
--    and organization_org_state.
--
-- SOURCE: the arrays are read from nih_raw_projects_current.raw_json with
-- json_each(). The project columns come from nih_parsed (copied once into a
-- temp table, joined on appl_id), so their definitions live in one place.
-- Run nih_parsed_view.sql first.
--
-- STANDALONE: each table carries the project's identifying columns, so a
-- Tableau data source (or a pandas frame) can be built on one table alone
-- with no join back to nih_parsed just to label a row. contact_pi_name is
-- the project's contact PI as NIH formats it (LAST, FIRST M), repeated on
-- every row -- in nih_principal_investigators it sits beside the pi_*
-- columns, which describe that row's own PI.
--
-- NO PROJECT-LEVEL DOLLARS, ON PURPOSE: award_amount, direct_cost_amt and
-- indirect_cost_amt are deliberately absent from all five tables. They would
-- repeat on every element row (a 3-PI award would show its award amount
-- three times) and be double-counted by any SUM. Do not add them. The only
-- dollar columns here are nih_agency_ic_fundings' own per-institute
-- ic_total_cost / ic_direct_cost / ic_indirect_cost, which are one value per
-- row and safe to sum. An award with no institute breakdown has no rows
-- there, so those sums will not equal total award dollars: use nih_parsed
-- for totals, this table for the split by institute.
--
-- NULL AND EMPTY ARRAYS: a record whose array field is null, empty or not an
-- array has no rows in that table, and an element whose own fields are all
-- null is skipped. So a table's appl_id count is lower than nih_parsed's
-- (many records have no program officer, and spending categories only
-- exist from FY2008 on).
--
-- SPENDING CATEGORY NAMES ARE PAIRED BY POSITION: the raw record holds the
-- ids in spending_categories (an array of integers) and the names in
-- spending_categories_desc, ONE string of names separated by "; " in the
-- same order as the ids. nih_spending_categories takes the nth name for the
-- nth id (the string is turned into a JSON array by splitting on ";" and
-- indexed by position). That is only right while both lists stay the same
-- length and in the same order, and while no name contains a semicolon
-- (names do contain commas, hyphens and parentheses). A description that
-- can't be split cleanly leaves its names NULL. nih_spending_categories_check.sql
-- returns a row for anything that breaks this; 7_build_child_tables.py runs
-- it after every build and logs a WARNING.
--
-- *_seq is the element's 1-based position in the source array, i.e. the
-- order NIH returned it in. With appl_id it identifies a row. It carries no
-- meaning of its own: the contact PI is is_contact_pi = 'true', not
-- necessarily pi_seq = '1'.
--
-- IDENTIFIERS AND LABELS ARE TEXT: appl_id, fiscal_year,
-- organization_org_ipf_code, every *_seq, pi_profile_id, funding_fy and
-- spending_category_id are stored as TEXT, so Tableau and pandas treat them
-- as labels, not numbers to sum. As text, *_seq sorts '1', '10', '2' --
-- cast it when numeric order matters. Booleans (is_active, is_contact_pi)
-- are 'true' / 'false'. Only the ic_*_cost dollar columns are numeric.
--
-- USING MORE THAN ONE TABLE TOGETHER multiplies rows (3 PIs x 2 funding
-- institutes = 6 rows, each ic_total_cost twice). Use one table per data
-- source, or relationships on appl_id -- not physical joins.
--
-- NOT COVERED: organization.org_duns / organization.org_ueis are also arrays
-- in the raw record, but nih_parsed takes primary_duns / primary_uei
-- instead, so they have no table here. covid_response_code is the code as
-- NIH sends it (Reg-CV, CV, C3, C4, C6 seen so far); the record carries no
-- description for it.
--
-- Run standalone with:  sqlite3 data/nih_harvester.db < ddl/nih_child_tables.sql

begin;

drop table if exists temp.child_project;
create temp table child_project as
   select cast(appl_id as text) as appl_id,
          project_num,
          core_project_num,
          cast(fiscal_year as text) as fiscal_year,
          project_title,
          contact_pi_name,
          activity_code,
          award_type,
          agency_ic_admin_abbreviation,
          organization_org_name,
          cast(organization_org_ipf_code as text) as organization_org_ipf_code,
          organization_org_state,
          organization_dept_type,
          case is_active when 1 then 'true' when 0 then 'false' end as is_active,
          project_start_date,
          project_end_date
     from nih_parsed;
create unique index temp.child_project_appl on child_project(appl_id);

drop table if exists nih_principal_investigators;
create table nih_principal_investigators as
   select p.appl_id,
          cast(je.key + 1 as text) as pi_seq,
          -- NIH PI profile id; null on a few old records
          cast(json_extract(je.value, '$.profile_id') as text) as pi_profile_id,
          json_extract(je.value, '$.first_name') as pi_first_name,
          json_extract(je.value, '$.middle_name') as pi_middle_name,
          json_extract(je.value, '$.last_name') as pi_last_name,
          json_extract(je.value, '$.full_name') as pi_full_name,
          json_extract(je.value, '$.title') as pi_title,
          case json_extract(je.value, '$.is_contact_pi') when 1 then 'true' when 0 then 'false' end as is_contact_pi,
          p.project_num, p.core_project_num, p.fiscal_year, p.project_title, p.contact_pi_name,
          p.activity_code, p.award_type, p.agency_ic_admin_abbreviation,
          p.organization_org_name, p.organization_org_ipf_code, p.organization_org_state,
          p.organization_dept_type, p.is_active, p.project_start_date, p.project_end_date
     from nih_raw_projects_current n
     join json_each(n.raw_json, '$.principal_investigators') je
     join temp.child_project p on p.appl_id = cast(n.appl_id as text)
    where json_type(n.raw_json, '$.principal_investigators') = 'array'
      and je.type = 'object'
      -- skips an element whose own fields are all null (see header)
      and coalesce(json_extract(je.value, '$.profile_id'), json_extract(je.value, '$.first_name'),
                   json_extract(je.value, '$.middle_name'), json_extract(je.value, '$.last_name'),
                   json_extract(je.value, '$.full_name'), json_extract(je.value, '$.title'),
                   json_extract(je.value, '$.is_contact_pi')) is not null;

drop table if exists nih_program_officers;
create table nih_program_officers as
   select p.appl_id,
          cast(je.key + 1 as text) as po_seq,
          json_extract(je.value, '$.first_name') as po_first_name,
          json_extract(je.value, '$.middle_name') as po_middle_name,
          json_extract(je.value, '$.last_name') as po_last_name,
          json_extract(je.value, '$.full_name') as po_full_name,
          p.project_num, p.core_project_num, p.fiscal_year, p.project_title, p.contact_pi_name,
          p.activity_code, p.award_type, p.agency_ic_admin_abbreviation,
          p.organization_org_name, p.organization_org_ipf_code, p.organization_org_state,
          p.organization_dept_type, p.is_active, p.project_start_date, p.project_end_date
     from nih_raw_projects_current n
     join json_each(n.raw_json, '$.program_officers') je
     join temp.child_project p on p.appl_id = cast(n.appl_id as text)
    where json_type(n.raw_json, '$.program_officers') = 'array'
      and je.type = 'object'
      and coalesce(json_extract(je.value, '$.first_name'), json_extract(je.value, '$.middle_name'),
                   json_extract(je.value, '$.last_name'), json_extract(je.value, '$.full_name')) is not null;

-- ic_* are the FUNDING institute's own fields -- not the same thing as the
-- project column agency_ic_admin_abbreviation (the administering institute).
-- The three ic_*_cost columns are per institute and safe to sum.
drop table if exists nih_agency_ic_fundings;
create table nih_agency_ic_fundings as
   select p.appl_id,
          cast(je.key + 1 as text) as funding_seq,
          cast(json_extract(je.value, '$.fy') as text) as funding_fy,
          json_extract(je.value, '$.code') as ic_code,
          json_extract(je.value, '$.abbreviation') as ic_abbreviation,
          json_extract(je.value, '$.name') as ic_name,
          json_extract(je.value, '$.total_cost') as ic_total_cost,
          json_extract(je.value, '$.direct_cost_ic') as ic_direct_cost,
          json_extract(je.value, '$.indirect_cost_ic') as ic_indirect_cost,
          p.project_num, p.core_project_num, p.fiscal_year, p.project_title, p.contact_pi_name,
          p.activity_code, p.award_type, p.agency_ic_admin_abbreviation,
          p.organization_org_name, p.organization_org_ipf_code, p.organization_org_state,
          p.organization_dept_type, p.is_active, p.project_start_date, p.project_end_date
     from nih_raw_projects_current n
     join json_each(n.raw_json, '$.agency_ic_fundings') je
     join temp.child_project p on p.appl_id = cast(n.appl_id as text)
    where json_type(n.raw_json, '$.agency_ic_fundings') = 'array'
      and je.type = 'object'
      and coalesce(json_extract(je.value, '$.fy'), json_extract(je.value, '$.code'),
                   json_extract(je.value, '$.abbreviation'), json_extract(je.value, '$.name'),
                   json_extract(je.value, '$.total_cost'), json_extract(je.value, '$.direct_cost_ic'),
                   json_extract(je.value, '$.indirect_cost_ic')) is not null;

-- spending_categories is an array of bare integers. The names come from
-- spending_categories_desc: escaped for JSON, split on ";" into a JSON
-- array, and the element at the id's own position taken and trimmed. A
-- description that doesn't make a valid JSON array (e.g. a control
-- character in it) leaves the names NULL instead of failing the build.
drop table if exists nih_spending_categories;
create table nih_spending_categories as
   with s as (
      select n.appl_id,
             n.raw_json,
             '["' || replace(replace(replace(json_extract(n.raw_json, '$.spending_categories_desc'),
                                             '\', '\\'), '"', '\"'), ';', '","') || '"]' as names
        from nih_raw_projects_current n
       where json_type(n.raw_json, '$.spending_categories') = 'array'
   )
   select p.appl_id,
          cast(je.key + 1 as text) as category_seq,
          cast(je.value as text) as spending_category_id,
          case when json_valid(s.names)
               then trim(json_extract(s.names, '$[' || je.key || ']'))
          end as spending_category_name,
          p.project_num, p.core_project_num, p.fiscal_year, p.project_title, p.contact_pi_name,
          p.activity_code, p.award_type, p.agency_ic_admin_abbreviation,
          p.organization_org_name, p.organization_org_ipf_code, p.organization_org_state,
          p.organization_dept_type, p.is_active, p.project_start_date, p.project_end_date
     from s
     join json_each(s.raw_json, '$.spending_categories') je
     join temp.child_project p on p.appl_id = cast(s.appl_id as text)
    where je.type = 'integer';

-- covid_response is an array of bare strings.
drop table if exists nih_covid_response;
create table nih_covid_response as
   select p.appl_id,
          cast(je.key + 1 as text) as covid_seq,
          je.value as covid_response_code,
          p.project_num, p.core_project_num, p.fiscal_year, p.project_title, p.contact_pi_name,
          p.activity_code, p.award_type, p.agency_ic_admin_abbreviation,
          p.organization_org_name, p.organization_org_ipf_code, p.organization_org_state,
          p.organization_dept_type, p.is_active, p.project_start_date, p.project_end_date
     from nih_raw_projects_current n
     join json_each(n.raw_json, '$.covid_response') je
     join temp.child_project p on p.appl_id = cast(n.appl_id as text)
    where json_type(n.raw_json, '$.covid_response') = 'array'
      and je.type = 'text';

create index idx_nih_principal_investigators_appl on nih_principal_investigators(appl_id);
create index idx_nih_program_officers_appl on nih_program_officers(appl_id);
create index idx_nih_agency_ic_fundings_appl on nih_agency_ic_fundings(appl_id);
create index idx_nih_spending_categories_appl on nih_spending_categories(appl_id);
create index idx_nih_covid_response_appl on nih_covid_response(appl_id);

drop table temp.child_project;

commit;
