# Scrape Remove Living Data

Interested in remote work. This script creates a database of wage/cost info for hundreds of cities. Can then run queries against things like the ratio of earnings to expense.

```
select c.continent, c.country, c.name, s.p75, h.mean1, h.mean2, h.mean2/s.p75 as liveratio 
from salary s inner join cost_house h inner join city c on h.id = s.id and s.id = c.id 
where name = 'Palo Alto'
order by h.mean2/s.p75 asc
limit 20;
┌───────────────┬───────────────┬───────────┬──────────────────┬────────┬────────┬────────────────────┐
│   continent   │    country    │   name    │       p75        │ mean1  │ mean2  │     liveratio      │
├───────────────┼───────────────┼───────────┼──────────────────┼────────┼────────┼────────────────────┤
│ North America │ United States │ Palo Alto │ 139578.010660894 │ 3300.0 │ 2600.0 │ 0.0186275759891486 │
└───────────────┴───────────────┴───────────┴──────────────────┴────────┴────────┴────────────────────┘
```