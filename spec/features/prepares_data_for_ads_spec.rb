require 'gooddata_connectors_downloader_ga'
require 'spec_helper'

describe 'Downloading data and preparing metadata and files for ads integrator', type: :feature do
  before :all do
    GA = GoodData::Connectors::GoogleAnalyticsDownloader::DownloaderGoogleAnalytics
    Metadata = GoodData::Connectors::Metadata::Metadata
    ConnectionHelper = GoodData::Connectors::GoogleAnalyticsDownloader::ConnectionHelper
    S3Helper = GoodData::Connectors::GoogleAnalyticsDownloader::S3Helper
  end

  before :each do
    FileUtils.mkdir('tmp') unless Dir.exist?('tmp')
  end

  after :each do
    FileUtils.rm_rf('tmp')
    FileUtils.rm_rf('metadata')
    FileUtils.rm_rf('source')
    S3Helper.clear_bucket
  end

  it 'prepares data without full and segments+filters' do
    expected_metadata = File.open('spec/data/expected_data/expected_metadata_1.json').read

    remote_config_path = S3Helper.generate_remote_path('configuration.json')
    S3Helper.upload_file('spec/data/configurations/configuration_1.json', remote_config_path)
    remote_data_path = S3Helper.generate_remote_path('ga_queries.csv')
    S3Helper.upload_file('spec/data/info_files/ga_queries_1.csv', remote_data_path)

    metadata = Metadata.new(ConnectionHelper::PARAMS)
    downloader = GA.new(metadata, ConnectionHelper::PARAMS)
    metadata.set_source_context(ConnectionHelper::DEFAULT_DOWNLOADER, {}, downloader)

    execute(downloader)

    metadata_path = S3Helper.generate_remote_path('metadata/user/') + time_path
    S3Helper.download_files(metadata_path, 'tmp/')
    files_path = S3Helper.generate_remote_path('ga_downloader_1/user/') + time_path
    S3Helper.download_files(files_path, 'tmp/')

    data = File.open(Dir['tmp/*_data.gz'].first).read
    metadata = File.open(Dir['tmp/*_metadata.json'].first).read

    expect(data.empty?).to be false
    expect(metadata).to eq expected_metadata
  end

  def get_metadata(batch, entity = '')
    parsed_batch = JSON.parse(batch)
    s3object = S3Helper.get_object(parsed_batch['files'].select { |file| file['entity'].match entity.to_s }.first['file'])
    s3object.metadata.to_h.to_hash.to_s
  end

  def time_path
    Time.now.strftime('%Y') + '/' + Time.now.strftime('%m') + '/' + Time.now.strftime('%d')
  end

  def execute(downloader)
    downloader.connect
    downloader.download_data
  end
end
