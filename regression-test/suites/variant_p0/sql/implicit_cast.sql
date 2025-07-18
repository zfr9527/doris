set exec_mem_limit=8G;
set exec_mem_limit = 8G;
set topn_opt_limit_threshold = 1024; 
SELECT count() from ghdata;
SELECT cast(v["repo"]["name"] as string) as repo_name, count() AS stars FROM ghdata WHERE v["type"] = 'WatchEvent' GROUP BY repo_name  ORDER BY stars DESC, repo_name LIMIT 5;
SELECT COUNT() FROM ghdata WHERE v["type"] match 'WatchEvent';
SELECT max(cast(v["id"] as bigint)) FROM ghdata;
SELECT sum(cast(v["payload"]["pull_request"]["milestone"]["creator"]["site_admin"] as int)) FROM ghdata;
SELECT sum(length(v["payload"]["pull_request"]["base"]["repo"]["html_url"])) FROM ghdata;
SELECT v["payload"]["member"]["id"] FROM ghdata where v["payload"]["member"]["id"] is not null  ORDER BY k LIMIT 10;
-- select k, v:payload.commits.author.name AS name, e FROM ghdata as t lateral view  explode(cast(v:payload.commits.author.name as array<string>)) tm1 as e  order by k limit 5;
select k, json_extract(v, '$.repo') from ghdata WHERE v["type"] = 'WatchEvent'  order by k limit 10;
-- SELECT v["payload"]["member"]["id"], count() FROM ghdata where v["payload"]["member"]["id"] is not null group by v["payload"]["member"]["id"] order by 1, 2 desc LIMIT 10;
select k, v["id"], v["type"], v["repo"]["name"] from ghdata WHERE v["type"] = 'WatchEvent'  order by k limit 10;
SELECT v["payload"]["pusher_type"] FROM ghdata where v["payload"]["pusher_type"] is not null ORDER BY k LIMIT 10;
-- implicit cast to decimal type
-- TODO: FE has problem with implicit cast to decimal type
-- SELECT v["id"] FROM ghdata where v["id"] not in (7273, 10.118626, -69352) order by cast(v["id"] as decimal) limit 10;