# encoding: UTF-8
#
# Copyright (c) 2010-2015 GoodData Corporation. All rights reserved.
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

module GoodData
  module Connectors
    module GoogleAnalyticsDownloader
      module ConnectionHelper
        set_const :DEFAULT_BDS_BUCKET, 'gdc-ms-connectors'
        set_const :DEFAULT_BDS_FOLDER, 'AIDAJFITXNOA7ZNFS3H4U_gdc-ms-connectors_ConnectorsTestSuite'
        set_const :DEFAULT_ACCOUNT, 'testing'
        set_const :DEFAULT_TOKEN, 'unit'
        set_const :DEFAULT_BDS_ACCESS_KEY, ENV['S3_ACCESS_KEY']
        set_const :DEFAULT_BDS_SECRET_KEY, ENV['S3_SECRET_KEY']

        set_const :DEFAULT_DOWNLOADER, 'ga_downloader_1'

        set_const :PARAMS, 'bds_bucket' => DEFAULT_BDS_BUCKET,
                           'bds_folder' => DEFAULT_BDS_FOLDER,
                           'account_id' => DEFAULT_ACCOUNT,
                           'token' => DEFAULT_TOKEN,
                           'bds_access_key' => DEFAULT_BDS_ACCESS_KEY,
                           'bds_secret_key' => DEFAULT_BDS_SECRET_KEY,
                           'ID' => DEFAULT_DOWNLOADER,
                           'GDC_LOGGER' => Logger.new(File.open(File::NULL, 'w'))
      end
    end
  end
end
