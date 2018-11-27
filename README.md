# marc_authority_harvester

Script for retrieving updated MARC authority records from external sources.

## Prerequisites

For Python 3 dependencies see: [requirements.txt](requirements.txt). You may need to install additional dependencies 
for `lxml`, see the [official Documentation](https://lxml.de/).

## Running the script

Run `python3 main.py -h` for instructions. 
* There are two supported output formats (MARC and MARCXML).
* There are two sources for authority data supported, namely the [iDAI.gazetteer](https://gazetteer.dainst.org) and the 
[Libray of Congress](http://id.loc.gov/index.html).
* There are three ways to specify the date marking the oldest updates you are interested in:
  1. Specify a date in ISO format (YYYY-MM-DD).
  2. Specify a day(s) offset from your current date.
  3. Continue from the last day the script was executed (after each run, this date is stored in a log file in the output
directory).
