a really really ugly parser for the the PDFs the Sierra Leone government publishes about Ebola in that country.

uses [Tabula](http://tabula.technology)

from a [hack day](https://www.eventbrite.com/e/nypl-labs-civic-data-hack-day-tickets-15378123381)

installation:
-------------
````
$ git clone git@github.com:jeremybmerrill/ebola_parsers.git
$ cd ebola_parsers
$ rbenv install jruby-1.7.16 # or another recent JRuby version; or use RVM if you prefer
$ rbenv local jruby-1.7.16
````

````
$ gem install bundler
$ bundle install
$ cd sierra_leone
$ ruby bin/parse_local_sierra_leone_ebola_files.rb ../inputs/** #once the scraper is installed, execute it
````
