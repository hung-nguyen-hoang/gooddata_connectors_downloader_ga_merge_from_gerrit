# encoding: UTF-8

require 'simplecov'
require 'pmap'
require 'rspec'
require 'pathname'
require 'gooddata'

require 'spec_helper'
require 'environment/default'

# Automagically include all helpers/*_helper.rb

base = Pathname(__FILE__).dirname.expand_path
Dir.glob(base + 'helpers/*_helper.rb').each do |file|
  require file
end

RSpec.configure do |config|
  config.filter_run_excluding broken: true

  config.include GoodData::Connectors::GoogleAnalyticsDownloader::ConnectionHelper
  config.include GoodData::Connectors::GoogleAnalyticsDownloader::S3Helper
  config.include GoodData::Connectors::GoogleAnalyticsDownloader::Connections

  config.before(:all) do
    $log = Logger.new(STDOUT)
  end

  config.before(:suite) do
    FileUtils.mkdir('tmp') unless Dir.exist?('tmp')
    FileUtils.mkdir('output') unless Dir.exist?('output')
    GoodData::Connectors::GoogleAnalyticsDownloader::S3Helper.connect
    # remote_path = GoodData::Connectors::GoogleAnalyticsDownloader::S3Helper.generate_remote_path('configuration.json')
    # GoodData::Connectors::GoogleAnalyticsDownloader::S3Helper.upload_file('spec/data/configurations/default_configuration.json', remote_path)
  end

  config.after(:suite) do
    FileUtils.rm_rf('tmp')
    FileUtils.rm_rf('metadata')
    FileUtils.rm_rf('source')
    GoodData::Connectors::GoogleAnalyticsDownloader::S3Helper.clear_bucket
  end
end

SimpleCov.formatter = SimpleCov::Formatter::MultiFormatter[
  SimpleCov::Formatter::HTMLFormatter
]

SimpleCov.start do
  add_group 'Downloader', 'lib/gooddata_connectors_downloader_ga'
end
