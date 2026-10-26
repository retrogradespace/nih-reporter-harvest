-- Spending category name check -- the warning step for
-- nih_spending_categories (nih_child_tables.sql). One read-only SELECT.
--
-- nih_spending_categories takes the nth name in spending_categories_desc
-- for the nth id in spending_categories. This returns one row per problem
-- and NO ROWS when everything is fine:
--   COUNT MISMATCH   a project whose number of ids differs from its number
--                    of names (includes: ids but no description string, and
--                    a description string but no ids)
--   NO NAME          a category row that ended up with no name (e.g. a
--                    description that couldn't be split)
--   ID HAS >1 NAME   an id paired with different names on different
--                    projects -- the sign of a shifted list, e.g. a name
--                    that contains a semicolon
-- Any row means spending_category_name should not be trusted for that
-- project / id until it has been looked at. The counts are read from
-- nih_raw_projects_current directly, so they do not depend on the table
-- being right.
--
-- 7_build_child_tables.py runs this after every build and logs each row as
-- a WARNING; it never fails the run. appl_id and spending_category_id are
-- text in every branch, matching the tables.
--
-- Keep this file to the one statement: 7_ executes its whole text.

select 'COUNT MISMATCH' as issue,
       c.appl_id,
       null as spending_category_id,
       'ids=' || c.id_count || ' names=' || c.name_count as detail
  from (
          select cast(n.appl_id as text) as appl_id,
                 case json_type(n.raw_json, '$.spending_categories')
                    when 'array' then json_array_length(n.raw_json, '$.spending_categories')
                    else 0
                 end as id_count,
                 coalesce(length(json_extract(n.raw_json, '$.spending_categories_desc'))
                          - length(replace(json_extract(n.raw_json, '$.spending_categories_desc'), ';', ''))
                          + 1, 0) as name_count
            from nih_raw_projects_current n
       ) c
 where c.id_count <> c.name_count
union all
select 'NO NAME',
       s.appl_id,
       s.spending_category_id,
       'category_seq=' || s.category_seq
  from nih_spending_categories s
 where s.spending_category_name is null
union all
select 'ID HAS >1 NAME',
       null,
       s.spending_category_id,
       count(distinct s.spending_category_name) || ' names, e.g. '
          || min(s.spending_category_name) || ' / ' || max(s.spending_category_name)
  from nih_spending_categories s
 group by s.spending_category_id
having count(distinct s.spending_category_name) > 1
