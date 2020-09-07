## Execution:
```bash
python expedia_crawler.py --start-date MM-DD-YYYY --end-date MM-DD-YYY --threads <num> --estimate-level <num> --debug
```

## Parameters:
- ### start-date
  Expects a date (MM-DD-YYYY)
  Defines the query start date
- ### end-date
  Expects a date (MM-DD-YYYY)
  Defines the query end date
- ### threads
  Expects an integer (default is 1)
  Defines the number of threads the search will be split into
- ### estimate-level
  Expects an integer (default is 2)
  Defines the level of estimation to be printed
  - level 0: will print only the estimate for each day
  - level 1: will print the estimate for every single query
- ### debug
  Expects nothing
  Will print some debugging info