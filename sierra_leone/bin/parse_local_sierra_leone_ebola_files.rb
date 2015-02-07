#!/usr/bin/env ruby
# encoding: utf-8

# Usage: 
# e.g. ./bin/parse_local_sierra_leone_ebola_files.rb "input/*/*.pdf"


require_relative '../lib/sierra_leone_parser.rb'

if __FILE__ == $0

  # initialize the parser
  config = File.exists?(File.join(File.dirname(File.expand_path(__FILE__)), '..', 'config.yml')) ? YAML::load_file(File.join(File.dirname(File.expand_path(__FILE__)), '..', 'config.yml')) : {}
  compstat_parser = SierraLeoneEbolaParser.new(config)
  
  # for each set of files
  ARGV.each do |glob| 
    Dir[glob + (glob.include?("*") || glob.match(/\.pdf$/) ? '' : "/**/*.pdf")].each do |filepath|
      next unless File.exists?(filepath)

      # open the PDF
      pdf_contents = open(filepath, 'rb'){|f| f.read }

      # and extract the data from it.
      csv = compstat_parser.process(pdf_contents, filepath, nil)
    end
  end
end
