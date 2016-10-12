
require 'aws-sdk-v1'

module GoodData
  module Connectors
    module GoogleAnalyticsDownloader
      module Connections
        class << self
          def init
            @metadata = GoodData::Connectors::Metadata::Metadata.new(GoodData::Connectors::GoogleAnalyticsDownloader::ConnectionHelper::PARAMS)
            @downloader = GoodData::Connectors::GoogleAnalyticsDownloader::DownloaderGoogleAnalytics.new(@metadata, GoodData::Connectors::GoogleAnalyticsDownloader::ConnectionHelper::PARAMS)
            @metadata.set_source_context(GoodData::Connectors::DownloaderCsv::ConnectionHelper::DEFAULT_DOWNLOADER, {}, @downloader)
          end

          attr_reader :metadata
          attr_reader :downloader
        end
      end
    end
  end
end
