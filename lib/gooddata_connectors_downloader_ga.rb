require 'gooddata_connectors_base'
require 'gooddata'
require 'gooddata_connectors_downloader_ga/version'
require 'gooddata_connectors_downloader_ga/ga_downloader'

module GoodData
  module Connectors
    module GoogleAnalyticsDownloader
      class GoogleAnalyticsDownloaderMiddleWare < GoodData::Bricks::Middleware
        def call(params)
          $log = params['GDC_LOGGER']
          $log.info 'Initializing GoogleAnalyticsDownloaderMiddleware'
          ga_downloader = DownloaderGoogleAnalytics.new(params['metadata_wrapper'], params)
          @app.call(params.merge('ga_downloader_wrapper' => ga_downloader))
        end
      end
    end
  end
end
