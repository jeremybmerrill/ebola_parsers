#!/usr/bin/env jruby
# encoding: utf-8

# you can require this file if you'd like to use it in another script.

require 'upton'
require 'fileutils'
require 's3-publisher'
require 'aws-sdk-v1'
require 'active_record'
require 'activerecord-jdbcmysql-adapter'
require 'yaml'
require 'tabula'
require 'tmpdir'

# A list of headers, in computerese
EBOLA_CASES = [       "district_name",
                      "population",
                      "new_noncase",
                      "new_suspected",
                      "new_probable",
                      "new_confirmed",
                      "cum_noncase",
                      "cum_suspected",
                      "cum_probable",
                      "cum_confirmed",
                      "death_suspected",
                      "death_probable",
                      "death_confirmed",
                      "cfr"]
LAB_RESULTS = [] # isn't listed per district, so skipping.
# LAB_RESULTS = [ "total_lab_samples",
#                 "repeat_samples",
#                 "new_samples",
#                 "new_positive",
#                 "new_negative",
#                 "pending" ]
CORPSES = [ "positive_corpse",
            "negative_corpse" ]
VHF_MANAGEMENT_ETC = [] # isn't listed for each district, so skipping.
                     # ["etc_new_admission",
                     #  "etc_currently_admitted",
                     #  "etc_cum_admission",
                     #  "etc_new_deaths",
                     #  "etc_cum_deaths",
                     #  "etc_new_discharges",
                     #  "etc_cum_discharges"]
CONTACTS = ["cum_contacts",
            "cum_completed_contacts",
            "contacts_followed",
            "new_contacts",
            "contacts_healthy",
            "contacts_ill",
            "contacts_not_seen",
            "new_completed_contacts",
            "percent_seen",]

HEADERS   = {'lab_results' => LAB_RESULTS,'ebola_cases' => EBOLA_CASES, 'corpses' => CORPSES, 'vhf_management_etc' => VHF_MANAGEMENT_ETC, 'contacts' => CONTACTS }
TYPES   = ['lab_results','ebola_cases', 'corpses', 'vhf_management_etc', 'contacts']

DEFAULT_NAME = "sierra_leone_ebola"
DATE_DIMENSIONS = [117.9375,379.3125,154.0625,484.5] # 121.125,381.4375,147.6875,476]
#                  125.375,382.5,145.5625,473.875
#                  117.9375,379.3125,154.0625,484.5
HEADER_NAMES = ["Name of district", "District", "DISTRICT"]


class SierraLeoneEbolaParser
  def initialize(config)
    @config = config
    # setup the places we're going to put our data (MySQL and a CSV for data, S3 for pdfs).
    # @csv_output = @config.has_key?("csv") ? @config["csv"] : DEFAULT_NAME + "_stats.csv"
    # open(@csv_output , 'wb'){|f| f << "region, date, " + SIERRA_LEONE_EBOLA_HEADERS.join(", ") + "\n"} unless !@csv_output || File.exists?(@csv_output)
    # AWS.config(access_key_id: @config['aws']['access_key_id'], secret_access_key: @config['aws']['secret_access_key']) if @config['aws']
    # ActiveRecord::Base.establish_connection(:adapter => 'jdbcmysql', :host => @config['mysql']['host'], :username => @config['mysql']['username'], :password => @config['mysql']['password'], :port => @config['mysql']['port'], :database => @config['mysql']['database']) unless !@config || !@config['mysql']
    # ActiveRecord::Base.connection.execute("CREATE TABLE IF NOT EXISTS #{DEFAULT_NAME}_by_region(region varchar(30), date datetime, "+
    #   SIERRA_LEONE_EBOLA_HEADERS.join(" integer,")+" integer" +
    #   ")") if @config["mysql"]
    # ActiveRecord::Base.connection.execute("CREATE TABLE IF NOT EXISTS #{DEFAULT_NAME}_citywide(region varchar(30), date datetime, "+
    #   SIERRA_LEONE_EBOLA_HEADERS.join(" integer,")+" integer" +
    #   ")") if @config["mysql"]
  end

  def figure_out_table_type(first_row)
    if first_row[0] == "SAMPLES RECEIVED"
      "lab_results"
    elsif first_row[0] == "District population"
      'ebola_cases'
    elsif first_row[0].match(/\d\d-\d\d-\d\d/)
      'corpses'
    elsif first_row.select{|r| r.length > 1 }[0] == "Admissions"
      'vhf_management_etc'
    elsif first_row[0] == "Total Contacts listed during outbreak"
      'contacts'
    else 
      puts first_row.inspect
      nil
    end
  end

  def process(pdf_data, pdf_path, date)
    # parse the given PDF
    report = parse_pdf( pdf_data, (pdf_basename = pdf_path.split("/")[-1]), (volume_number = pdf_basename.split('.pdf')[0].scan(/\d+/).first), date )
    return if report.nil?
    
    # if this report is already in the database, don't put it in the DB (and assume it exists in S3, perhaps under another date)
    table_name = "#{DEFAULT_NAME}_by_region"
    return if @config['mysql'] && ActiveRecord::Base.connection.active? && !ActiveRecord::Base.connection.execute("SELECT * FROM #{table_name} WHERE region = '#{report.region}' AND month = '#{report.month}' AND year = '#{report.year}'").empty?
    
    # add our data to MySQL, if config.yml says to.
    ActiveRecord::Base.connection.execute("INSERT INTO #{table_name}(region, date, #{SIERRA_LEONE_EBOLA_HEADERS.join(',')}) VALUES (" + report.to_csv_row(true)+ ")") if @config['mysql']


    # N.B.: If there's no database, you'll get duplicate records in the CSV. 
    # open(@csv_output, 'ab'){|f| f << report.to_csv_rows + "\n"} if @csv_output

    open(report.csv_file_name, 'wb'){|f| f << report.to_csv_rows + "\n"}


    puts "#{volume_number}: #{report.date.strftime("%Y-%m-%d")}"

    # Save the file to disk and/or S3, if specified in config.yml
    if @config['aws'] && @config['aws']['s3']
      if !@s3[config['aws']['s3']['bucket']].objects[key].exists?
        S3Publisher.publish(@config['aws']['s3']['bucket'], {logger: 'faux /dev/null'}) do |p| 
          p.push( File.join(DEFAULT_NAME, report.date.to_s, pdf_basename), 
                  data: pdf_data, gzip: false) 
        end
      end
    end
    if @config['local_pdfs_path']
      full_path = File.join(@config['local_pdfs_path'], "#{report.year}_#{report.month}_sum", pdf_basename)
      FileUtils.mkdir_p( File.dirname full_path )
      FileUtils.copy(report.path, full_path) unless File.exists?(full_path) # don't overwrite
    end
  end

  # transform a PDF into the data we want to extract
  def parse_pdf(pdf, pdf_basename, volume_number, date)
    tmp_dir = File.join(Dir::tmpdir, "#{DEFAULT_NAME}_pdfs")
    Dir.mkdir(tmp_dir) unless Dir.exists?(tmp_dir)

    # write the file to disk; we need to write the file to disk for Tabula to use it.  
    open( pdf_path = File.join(tmp_dir, pdf_basename) , 'wb'){|f| f << pdf}
    # open the file in Tabula
    begin
      pages = (extractor = Tabula::Extraction::ObjectExtractor.new(pdf_path, :all)).extract
    rescue java.io.IOException => e
      puts "Failed to open PDF (#{pdf_basename}) #{e.message}"
      return nil
    end

    raw_date = pages.first.get_area(DATE_DIMENSIONS).get_table.rows.to_a[0][0].text.gsub(')', '').strip # 28 November, 2014
    date = DateTime.strptime(raw_date, "%d %B, %Y")

    # create a report to represent the data from this report (but it's empty right now)
    report = SierraLeoneEbolaReport.new(volume_number, date, pdf_path)

    # for the second table Tabula detects in the PDF (deaths and cases)
    # lab_results = page.spreadsheets[0]
    # ebola_cases = page.spreadsheets[1]
    # corpses = page.spreadsheets[2]
    # vhf_management_etc = page.spreadsheets[3]
    # contacts = page.spreadsheets[4]
    spreadsheets = pages.map(&:spreadsheets).flatten
    spreadsheets.each_with_index do |spreadsheet, index|
      puts "\n"
      # these variables are set inside the block, but need scope into this block
      spreadsheet_type = 'unset' 
      this_spreadsheet_headers = nil


      # and for each row in that spreadsheet
      spreadsheet.rows.each_with_index do |row, i|
        # get the name of the violation and the amount in this month (for this region)
        district_name, *row_text_data = *row.map{|cell| cell.text.gsub(",", '').gsub("\n", '').gsub("\r", '')}
        row_data = row_text_data.map(&:to_i)

        # figure out what kind of table this is
        if spreadsheet_type == 'unset'
          spreadsheet_type = figure_out_table_type(row_text_data)
          spreadsheet_type = 'unset' if row_text_data.all?{|d| d.length == 0 }
          report.types[spreadsheet_type] = []
          this_spreadsheet_headers = HEADERS[spreadsheet_type]
        end
        next unless ['ebola_cases', 'corpses', 'contacts'].include? spreadsheet_type # only process these, the other two have wacky headers
        next if district_name.downcase == "total" # duplicative (there's also "national")


        # but skip the header
        next if HEADER_NAMES.include?(district_name) || district_name.strip.empty?
        region = SierraLeoneEbolaRegion.new district_name, this_spreadsheet_headers

        # add this data to the report object
        region.statistics = Hash[*this_spreadsheet_headers.zip(row_data).flatten]
        # zero out any statistic that aren't included in this month's report
        this_spreadsheet_headers.each do |item|
          region.statistics[item] = 0 unless region.statistics.has_key?(item)
        end

        report.types[spreadsheet_type] << region
      end
    end
    extractor.close!


    report
  end
end

# a class to represent the data contained in each report.
class SierraLeoneEbolaReport
  attr_reader :types, :date, :path

  def initialize volume_number, date, path
    @types = {}
    @date = date
    @path = path
    @volume_number = volume_number
  end

  def to_a
    transposed = (HEADERS.values.reduce(&:+).zip(@types.to_a.sort_by{|type| TYPES.index(type) }.map{|type, regs| regs.sort_by(&:region_name).map{|r| type == 'ebola_cases' ? r.to_a[0...-1] : r.to_a[1..-1] }.transpose }.reduce(&:+))).map{|row| [@date.strftime("%Y-%m-%d")] + row}
    transposed[0][1] = "date"
    transposed[0][1] = "variable"
    transposed
  end

  def csv_file_name
    File.basename(@path, 'pdf') + "csv"
  end

  def to_csv_rows
    to_a.map{|row| row.join(",")}.join("\n")
  end

end

# a class to represent eac h
class SierraLeoneEbolaRegion
  attr_reader :region_name, :path, :headers
  attr_accessor :statistics

  def initialize region_name, headers
    @region_name = region_name
    @statistics = {}
    @headers = headers
  end

  def to_a
    [@region_name] + @headers.map{|h| @statistics[h]}
  end

end
